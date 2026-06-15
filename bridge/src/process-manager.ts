import type { Logger } from "pino";

import type { PiRpcForwarder, PiRpcForwarderMessage } from "./rpc-forwarder.js";

export interface ProcessManagerEvent {
    cwd: string;
    payload: PiRpcForwarderMessage;
    internal?: boolean;
}

export interface AcquireControlRequest {
    clientId: string;
    cwd: string;
    sessionPath?: string;
}

export interface AcquireControlResult {
    success: boolean;
    reason?: string;
}

export interface ProcessManagerStats {
    activeProcessCount: number;
    lockedCwdCount: number;
    lockedSessionCount: number;
}

export interface ControlSnapshot {
    cwdOwnerClientId?: string;
    sessionOwnerClientId?: string;
}

export interface PiProcessManager {
    setMessageHandler(handler: (event: ProcessManagerEvent) => void): void;
    getOrStart(cwd: string): PiRpcForwarder;
    sendRpc(cwd: string, payload: Record<string, unknown>): void;
    acquireControl(request: AcquireControlRequest): AcquireControlResult;
    hasControl(clientId: string, cwd: string, sessionPath?: string): boolean;
    getControlSnapshot(cwd: string, sessionPath?: string): ControlSnapshot;
    releaseControl(clientId: string, cwd: string, sessionPath?: string): void;
    releaseClient(clientId: string): void;
    getStats(): ProcessManagerStats;
    evictIdleProcesses(): Promise<void>;
    stop(): Promise<void>;
}

export interface ProcessManagerOptions {
    idleTtlMs: number;
    heartbeatIntervalMs?: number;
    logger: Logger;
    forwarderFactory: (cwd: string) => PiRpcForwarder;
    now?: () => number;
    enableEvictionTimer?: boolean;
}

interface ForwarderEntry {
    cwd: string;
    forwarder: PiRpcForwarder;
    lastUsedAt: number;
    shouldPoll: boolean;
    heartbeatId: string;
    lastStatus?: {
        isStreaming: boolean;
        isCompacting: boolean;
    };
}

interface SessionLock {
    clientId: string;
    cwd: string;
}

export function createPiProcessManager(options: ProcessManagerOptions): PiProcessManager {
    const now = options.now ?? (() => Date.now());
    const entries = new Map<string, ForwarderEntry>();
    const lockByCwd = new Map<string, string>();
    const lockBySession = new Map<string, SessionLock>();
    let messageHandler: (event: ProcessManagerEvent) => void = () => {};

    const evictionIntervalMs = Math.max(1_000, Math.floor(options.idleTtlMs / 2));
    const shouldStartTimer = options.enableEvictionTimer ?? true;
    const evictionTimer = shouldStartTimer
        ? setInterval(() => {
            void evictIdleProcessesInternal();
        }, evictionIntervalMs)
        : undefined;

    const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 1_000;
    const heartbeatTimer = shouldStartTimer
        ? setInterval(() => {
            for (const [cwd, entry] of entries.entries()) {
                if (!lockByCwd.has(cwd) && entry.shouldPoll) {
                    entry.forwarder.send({ id: entry.heartbeatId, type: "get_state" });
                }
            }
        }, heartbeatIntervalMs)
        : undefined;

    evictionTimer?.unref();
    heartbeatTimer?.unref();

    const getOrStart = (cwd: string): PiRpcForwarder => {
        const existingEntry = entries.get(cwd);
        if (existingEntry) {
            existingEntry.lastUsedAt = now();
            return existingEntry.forwarder;
        }

        const forwarder = options.forwarderFactory(cwd);
        const heartbeatId = `bridge-heartbeat-${Math.random().toString(36).slice(2, 11)}`;
        const entry: ForwarderEntry = {
            cwd,
            forwarder,
            lastUsedAt: now(),
            shouldPoll: false,
            heartbeatId,
        };

        forwarder.setMessageHandler((payload) => {
            const isHeartbeatResponse = payload.type === "response" && payload.command === "get_state";
            const isInternalHeartbeat = isHeartbeatResponse && payload.id === entry.heartbeatId;
            const data = isHeartbeatResponse ? (payload.data as Record<string, unknown>) : null;
            const isWorking = !!(data && ((data.isStreaming as boolean) === true || (data.isCompacting as boolean) === true));

            const wasPolling = entry.shouldPoll;

            if (isHeartbeatResponse) {
                if (data) {
                    const currentStatus = {
                        isStreaming: !!data.isStreaming,
                        isCompacting: !!data.isCompacting,
                    };

                    options.logger.debug({ cwd, status: currentStatus, tag: "bridge-heartbeat" }, "RPC heartbeat response");

                    if (entry.lastStatus) {
                        if (
                            entry.lastStatus.isStreaming !== currentStatus.isStreaming ||
                            entry.lastStatus.isCompacting !== currentStatus.isCompacting
                        ) {
                            options.logger.info(
                                {
                                    cwd,
                                    prev: entry.lastStatus,
                                    next: currentStatus,
                                    tag: "bridge-heartbeat",
                                },
                                "RPC process status changed",
                            );
                        }
                    }
                    entry.lastStatus = currentStatus;
                    entry.shouldPoll = isWorking;
                }
            } else {
                const eventType = payload.type as string;
                if (eventType === "agent_start" || eventType === "compaction_start") {
                    entry.shouldPoll = true;
                }
            }

            if (!wasPolling && entry.shouldPoll) {
                options.logger.info(
                    {
                        cwd,
                        reason: isHeartbeatResponse ? "heartbeat-working" : "agent-activity-start",
                        tag: "bridge-heartbeat",
                    },
                    "Starting heartbeat polling",
                );
            }

            if (!isHeartbeatResponse || isWorking) {
                entry.lastUsedAt = now();
            }
            messageHandler({ cwd, payload, internal: isInternalHeartbeat });
        });
        forwarder.setLifecycleHandler((event) => {
            options.logger.info({ cwd, event }, "RPC forwarder lifecycle event");
        });

        entries.set(cwd, entry);

        options.logger.info({ cwd }, "Started RPC forwarder for cwd");

        return forwarder;
    };

    const evictIdleProcessesInternal = async (): Promise<void> => {
        const evictionCutoff = now() - options.idleTtlMs;

        for (const [cwd, entry] of entries.entries()) {
            const shouldKeepRunning = entry.lastUsedAt >= evictionCutoff || lockByCwd.has(cwd);
            if (shouldKeepRunning) continue;

            await entry.forwarder.stop();
            entries.delete(cwd);

            options.logger.info({ cwd }, "Evicted idle RPC forwarder");
        }
    };

    return {
        setMessageHandler(handler: (event: ProcessManagerEvent) => void): void {
            messageHandler = handler;
        },
        getOrStart(cwd: string): PiRpcForwarder {
            return getOrStart(cwd);
        },
        sendRpc(cwd: string, payload: Record<string, unknown>): void {
            const forwarder = getOrStart(cwd);
            const entry = entries.get(cwd);
            if (entry) {
                entry.lastUsedAt = now();
            }
            forwarder.send(payload);
        },
        acquireControl(request: AcquireControlRequest): AcquireControlResult {
            const currentCwdOwner = lockByCwd.get(request.cwd);
            if (currentCwdOwner && currentCwdOwner !== request.clientId) {
                return {
                    success: false,
                    reason: `cwd is controlled by another client: ${request.cwd}`,
                };
            }

            if (request.sessionPath) {
                const currentSessionLock = lockBySession.get(request.sessionPath);
                if (currentSessionLock && currentSessionLock.clientId !== request.clientId) {
                    return {
                        success: false,
                        reason: `session is controlled by another client: ${request.sessionPath}`,
                    };
                }
            }

            lockByCwd.set(request.cwd, request.clientId);
            if (request.sessionPath) {
                lockBySession.set(request.sessionPath, {
                    clientId: request.clientId,
                    cwd: request.cwd,
                });
            }

            return { success: true };
        },
        hasControl(clientId: string, cwd: string, sessionPath?: string): boolean {
            if (lockByCwd.get(cwd) !== clientId) {
                return false;
            }

            if (sessionPath) {
                const sessionLock = lockBySession.get(sessionPath);
                if (!sessionLock || sessionLock.clientId !== clientId || sessionLock.cwd !== cwd) {
                    return false;
                }
            }

            return true;
        },
        getControlSnapshot(cwd: string, sessionPath?: string): ControlSnapshot {
            return {
                cwdOwnerClientId: lockByCwd.get(cwd),
                sessionOwnerClientId: sessionPath ? lockBySession.get(sessionPath)?.clientId : undefined,
            };
        },
        releaseControl(clientId: string, cwd: string, sessionPath?: string): void {
            if (lockByCwd.get(cwd) === clientId) {
                lockByCwd.delete(cwd);
            }

            if (sessionPath) {
                const sessionLock = lockBySession.get(sessionPath);
                if (sessionLock && sessionLock.clientId === clientId) {
                    lockBySession.delete(sessionPath);
                }
                return;
            }

            for (const [lockedSessionPath, sessionLock] of lockBySession.entries()) {
                if (sessionLock.clientId === clientId && sessionLock.cwd === cwd) {
                    lockBySession.delete(lockedSessionPath);
                }
            }
        },
        releaseClient(clientId: string): void {
            for (const [cwd, ownerClientId] of lockByCwd.entries()) {
                if (ownerClientId === clientId) {
                    lockByCwd.delete(cwd);
                }
            }

            for (const [sessionPath, sessionLock] of lockBySession.entries()) {
                if (sessionLock.clientId === clientId) {
                    lockBySession.delete(sessionPath);
                }
            }
        },
        getStats(): ProcessManagerStats {
            return {
                activeProcessCount: entries.size,
                lockedCwdCount: lockByCwd.size,
                lockedSessionCount: lockBySession.size,
            };
        },
        async evictIdleProcesses(): Promise<void> {
            await evictIdleProcessesInternal();
        },
        async stop(): Promise<void> {
            if (evictionTimer) {
                clearInterval(evictionTimer);
            }
            if (heartbeatTimer) {
                clearInterval(heartbeatTimer);
            }

            for (const entry of entries.values()) {
                await entry.forwarder.stop();
            }

            entries.clear();
            lockByCwd.clear();
            lockBySession.clear();
        },
    };
}

import { describe, expect, it } from "vitest";
import { sanitizeCwdForSessionDir } from "../src/server.js";

describe("sanitizeCwdForSessionDir", () => {
    it("wraps result in double dashes", () => {
        expect(sanitizeCwdForSessionDir("/tmp/project")).toBe("--tmp-project--");
    });

    it("replaces slashes with dashes", () => {
        expect(sanitizeCwdForSessionDir("/home/user/my-project")).toBe("--home-user-my-project--");
    });

    it("replaces unsafe characters with underscores", () => {
        expect(sanitizeCwdForSessionDir("/tmp/my project!@#")).toBe("--tmp-my_project___--");
    });

    it("removes leading and trailing slashes before processing", () => {
        expect(sanitizeCwdForSessionDir("/tmp/project/")).toBe("--tmp-project--");
        expect(sanitizeCwdForSessionDir("//tmp/project//")).toBe("--tmp-project--");
    });

    it("handles windows-style paths", () => {
        expect(sanitizeCwdForSessionDir("C:/Users/User/Project")).toBe("--C_-Users-User-Project--");
    });

    it("handles empty or root paths", () => {
        expect(sanitizeCwdForSessionDir("/")).toBe("----");
    });
});

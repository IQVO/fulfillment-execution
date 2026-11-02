import { describe, expect, it, vi } from "vitest";
import { fetchAllPages, newIdempotencyKey } from "./api";

function pageResponse(body: unknown): Response {
  return { ok: true, status: 200, json: async () => body } as Response;
}

describe("fetchAllPages", () => {
  it("follows nextCursor until it is absent and keeps the filters", async () => {
    const calls: string[] = [];
    const fetchImpl = vi.fn(async (url: string) => {
      calls.push(url);
      if (!url.includes("cursor=")) return pageResponse({ items: [{ id: "a" }, { id: "b" }], nextCursor: "c1" });
      if (url.includes("cursor=c1")) return pageResponse({ items: [{ id: "c" }], nextCursor: "c2" });
      return pageResponse({ items: [{ id: "d" }] });
    });
    const rows = await fetchAllPages<{ id: string }>("/tasks?orderRef=order-1", fetchImpl as unknown as typeof fetch);
    expect(rows.map((r) => r.id)).toEqual(["a", "b", "c", "d"]);
    expect(calls).toEqual([
      "/tasks?orderRef=order-1",
      "/tasks?orderRef=order-1&cursor=c1",
      "/tasks?orderRef=order-1&cursor=c2",
    ]);
  });

  it("fails loudly on a non-2xx answer", async () => {
    const fetchImpl = vi.fn(async () => ({ ok: false, status: 400 }) as Response);
    await expect(fetchAllPages("/tasks?orderRef=x", fetchImpl as unknown as typeof fetch)).rejects.toThrow("HTTP 400");
  });
});

describe("newIdempotencyKey", () => {
  it("mints a distinct 1-128 printable ASCII key per call", () => {
    const a = newIdempotencyKey();
    expect(a).toMatch(/^[\x20-\x7e]{1,128}$/);
    expect(newIdempotencyKey()).not.toBe(a);
  });
});

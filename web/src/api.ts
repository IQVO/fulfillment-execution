/** The fleet collection envelope (REST guideline section 3): a page of items plus an opaque cursor. */
export interface Page<T> {
  items: T[];
  nextCursor?: string;
}

/** Upper bound on pages followed by fetchAllPages, so a misbehaving server cannot loop the client forever. */
export const MAX_PAGES = 200;

/**
 * Reads a whole collection: GETs `url`, then follows `nextCursor` (as the `cursor` query parameter,
 * filters repeated) until it is absent. A client that read only the first page would silently truncate
 * the list once the collection outgrows the default page size (STANDARD 5.2).
 */
export async function fetchAllPages<T>(
  url: string,
  fetchImpl: typeof fetch = fetch,
  signal?: AbortSignal,
): Promise<T[]> {
  const all: T[] = [];
  let cursor: string | undefined;
  for (let pages = 0; pages < MAX_PAGES; pages++) {
    const next = new URL(url, "http://placeholder.invalid");
    if (cursor) next.searchParams.set("cursor", cursor);
    const target = url.startsWith("http") ? next.toString() : `${next.pathname}${next.search}`;
    const res = await fetchImpl(target, { signal });
    if (!res.ok) {
      throw new Error(`GET ${url} failed: HTTP ${res.status}`);
    }
    const body = (await res.json()) as Page<T>;
    all.push(...body.items);
    if (!body.nextCursor) return all;
    cursor = body.nextCursor;
  }
  throw new Error(`GET ${url}: more than ${MAX_PAGES} pages`);
}

/** A fresh Idempotency-Key for ONE logical submit (reuse it on a retry of that submit). */
export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}

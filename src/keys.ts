/**
 * Key codec (DESIGN.md, "Core mechanism").
 *
 *   {prefix}/streams/{streamId}/e/{baseVersion:012d}.json
 *   {prefix}/streams/{streamId}/c/{chunkBase:012d}.json
 *   {prefix}/streams/{streamId}/head.json
 *
 * Zero-padding makes lexicographic order equal numeric order. Version math
 * must come from reading commit bodies, never from key arithmetic.
 */

const PAD = 12;

export function padVersion(v: number): string {
  if (!Number.isInteger(v) || v < 0) throw new RangeError(`invalid version: ${v}`);
  return String(v).padStart(PAD, "0");
}

export function validateStreamId(streamId: string): void {
  if (streamId.length === 0) throw new RangeError("streamId must be non-empty");
  if (streamId.includes("/")) {
    throw new RangeError(`streamId must not contain "/": ${streamId}`);
  }
  if (streamId.startsWith("$")) {
    throw new RangeError(`stream IDs beginning with "$" are reserved: ${streamId}`);
  }
}

export function streamPrefix(prefix: string, streamId: string): string {
  return `${prefix}/streams/${streamId}`;
}

export function commitPrefix(prefix: string, streamId: string): string {
  return `${streamPrefix(prefix, streamId)}/e/`;
}

export function chunkPrefix(prefix: string, streamId: string): string {
  return `${streamPrefix(prefix, streamId)}/c/`;
}

export function commitKey(prefix: string, streamId: string, baseVersion: number): string {
  return `${commitPrefix(prefix, streamId)}${padVersion(baseVersion)}.json`;
}

export function chunkKey(prefix: string, streamId: string, chunkBase: number): string {
  return `${chunkPrefix(prefix, streamId)}${padVersion(chunkBase)}.json`;
}

export function headKey(prefix: string, streamId: string): string {
  return `${streamPrefix(prefix, streamId)}/head.json`;
}

/** Base version encoded in a commit or chunk key. */
export function baseFromKey(key: string): number {
  const file = key.slice(key.lastIndexOf("/") + 1);
  const m = /^(\d{12})\.json$/.exec(file);
  if (!m) throw new RangeError(`not a versioned object key: ${key}`);
  return Number(m[1]);
}

/** The chunk bucket a commit belongs to, by its *base* version. */
export function bucketBase(baseVersion: number, chunkSize: number): number {
  return Math.floor(baseVersion / chunkSize) * chunkSize;
}

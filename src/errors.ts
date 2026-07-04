/** Error taxonomy (DESIGN.md, "Public API sketch"). All typed, all exported. */

/** Another writer won the version we targeted; re-read, re-decide, re-append. */
export class ConcurrencyError extends Error {
  constructor(
    public readonly streamId: string,
    message: string,
  ) {
    super(message);
    this.name = "ConcurrencyError";
  }
}

/**
 * Transport-level failure (timeout, 5xx, lost response). Retryable; the
 * append path retries the conditional PUT and lets the commitId self-check
 * disambiguate a retry that collides with its own successful write.
 */
export class TransientStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TransientStoreError";
  }
}

/**
 * The store observed a state the protocol proves impossible (e.g. a version
 * gap that no chunk covers after recovery retries). Never expected; indicates
 * external interference or a bug — surfaced loudly, never papered over.
 */
export class CorruptionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CorruptionError";
  }
}

export class SerializationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SerializationError";
  }
}

/**
 * The subject has a soft-deleted (pending/committing) shred tombstone:
 * appends must stop writing its personal data, key delivery returns empty
 * (KEYS_DESIGN.md, Shred protocol). Raised before any PUT.
 */
export class SubjectErasedError extends Error {
  constructor(public readonly subjectId: string) {
    super(`subject ${subjectId} is erased or being erased; refusing to encrypt`);
    this.name = "SubjectErasedError";
  }
}

/**
 * Fail-closed decryption failure: the key is shredded, undeliverable, or
 * the ciphertext fails authentication. A shredded stream presents as this,
 * never as stale plaintext.
 */
export class ShreddedDataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ShreddedDataError";
  }
}

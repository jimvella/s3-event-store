/**
 * Payload serializer — the seam encryption ships through (DESIGN.md,
 * Encryption & erasure contract, seam 1). The store treats `data` as
 * opaque; a serializer maps plaintext ↔ stored representation per event
 * and may stamp the reserved `keyId` envelope field. Compaction copies
 * envelopes verbatim, so it never re-serializes and needs no key access.
 */

import type { EventEnvelope } from "./types.js";

export interface SerializedPayload {
  data: unknown;
  /** Set by encrypting serializers: the generation that encrypted `data`. */
  keyId?: string;
}

export interface PayloadSerializer {
  serialize(streamId: string, event: { type: string; data: unknown }): Promise<SerializedPayload>;
  /** Returns the plaintext `data` for an envelope. Fails closed on shredded keys. */
  deserialize(streamId: string, envelope: EventEnvelope): Promise<unknown>;
}

/** The default: plain JSON payloads stored as-is. */
export function jsonSerializer(): PayloadSerializer {
  return {
    async serialize(_streamId, event) {
      return { data: event.data };
    },
    async deserialize(_streamId, envelope) {
      return envelope.data;
    },
  };
}

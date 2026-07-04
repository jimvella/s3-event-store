/** Public API surface. Drivers are separate entry points (src/drivers/*). */

export type {
  GetResult,
  ListedKey,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "./driver.js";
export {
  ConcurrencyError,
  CorruptionError,
  SerializationError,
  ShreddedDataError,
  SubjectErasedError,
  TransientStoreError,
} from "./errors.js";
export { jsonSerializer, type PayloadSerializer, type SerializedPayload } from "./serializer.js";
export { encryptingSerializer, type EncryptingSerializerConfig } from "./crypto/serializer.js";
export {
  createS3KeyStore,
  generationKey,
  keyIdOf,
  tombstoneKey,
  type KeyStore,
  type KeyringEntry,
  type S3KeyStoreConfig,
  type Tombstone,
  type TombstoneState,
} from "./crypto/keystore.js";
export { aesMasterKey, type MasterKey } from "./crypto/master-key.js";
export {
  AUDIT_STREAM,
  cancelShred,
  ensureTombstone,
  requestShred,
  sweepShreds,
  type CancelOutcome,
  type ShredContext,
  type SweepReport,
} from "./crypto/shred.js";
export {
  createEventStore,
  type AppendOptions,
  type CompactionResult,
  type EventStore,
  type EventStoreConfig,
  type HeadResolution,
  type ReadOptions,
} from "./store.js";
export type {
  AppendResult,
  ChunkObject,
  CommitObject,
  EventEnvelope,
  EventInput,
  EventMeta,
  ExpectedVersion,
  HeadHint,
} from "./types.js";

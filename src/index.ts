/** Public API surface. Drivers are separate entry points (src/drivers/*). */

export type {
  GetResult,
  ListedKey,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "./driver.js";
export { ConcurrencyError, CorruptionError, SerializationError, TransientStoreError } from "./errors.js";
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

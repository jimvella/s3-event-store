/**
 * SimDriver: the storage-driver interface over SimStore, with every call
 * routed through the scheduler as an atomic op. One instance per actor, so
 * the trace attributes ops to the actor that issued them.
 *
 * LIST pages arrive with a deliberately tiny page size so pagination races
 * occur constantly (SIMULATOR_PLAN.md, SimStore).
 */

import type {
  GetResult,
  ListPage,
  PutIfAbsentResult,
  PutIfMatchResult,
  StorageDriver,
} from "../src/driver.js";
import type { Scheduler } from "./scheduler.js";
import type { SimStore } from "./store.js";

export function createSimDriver(
  scheduler: Scheduler,
  store: SimStore,
  actor: string,
  opts?: { listPageSize?: number },
): StorageDriver {
  const pageSize = opts?.listPageSize ?? 3;
  return {
    get(key, o): Promise<GetResult> {
      const pin = o?.ifMatch !== undefined ? " if-match" : "";
      return scheduler.op(actor, `get${pin} ${key}`, () => store.get(key, o));
    },
    put(key, body): Promise<{ etag: string }> {
      return scheduler.op(actor, `put ${key}`, () => store.put(key, body));
    },
    putIfAbsent(key, body): Promise<PutIfAbsentResult> {
      return scheduler.op(actor, `putIfAbsent ${key}`, () => store.putIfAbsent(key, body));
    },
    putIfMatch(key, body, etag): Promise<PutIfMatchResult> {
      return scheduler.op(actor, `putIfMatch ${key}`, () => store.putIfMatch(key, body, etag));
    },
    list(prefix, o): Promise<ListPage> {
      return scheduler.op(actor, `list ${prefix} after=${o?.startAfter ?? "-"}`, () =>
        store.list(prefix, { maxKeys: o?.maxKeys ?? pageSize, ...(o?.startAfter !== undefined ? { startAfter: o.startAfter } : {}) }),
      );
    },
    delete(key): Promise<void> {
      return scheduler.op(actor, `delete ${key}`, () => store.delete(key));
    },
    deleteMany(keys): Promise<void> {
      return scheduler.op(actor, `deleteMany ${keys.length} keys`, () => store.deleteMany(keys));
    },
  };
}

/**
 * Bun Litestream - SQLite replication for Bun.js
 *
 * A pure TS implementation of Litestream.
 *
 * @example
 * ```typescript
 * import { Db, S3 } from "bun-litestream"
 *
 * // Create S3 replica
 * const replica = S3.make("s3://my-bucket/backups")
 *
 * // Open database with replication
 * const db = new Db.DB({
 *   path: "./my.db",
 *   replica,
 * })
 *
 * await db.open()
 *
 * // Your database is now being replicated!
 * // Use Bun's native SQLite API directly
 *
 * // Clean shutdown
 * await db.close()
 * ```
 */

export * as Db from "./Db.ts"
export * as Ltx from "./Ltx.ts"
export * as S3 from "./S3.ts"
export * as Snapshot from "./Snapshot.ts"
export * as Wal from "./Wal.ts"

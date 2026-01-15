/**
 * Database replication engine
 *
 * Monitors a SQLite database and replicates changes to a remote replica (S3).
 * Uses the LTX file format for efficient transaction log storage.
 */

import * as BunSqlite from "bun:sqlite"
import * as NFs from "node:fs"
import * as NPath from "node:path"
import * as Ltx from "./Ltx.ts"
import type * as S3 from "./S3.ts"
import {
  calcSize,
  readHeader,
  WAL_FRAME_HEADER_SIZE,
  WAL_HEADER_SIZE,
  WalReader,
} from "./Wal.ts"

const DEFAULT_MONITOR_INTERVAL = 1_000
const DEFAULT_CHECKPOINT_INTERVAL = 6_0000
const DEFAULT_MIN_CHECKPOINT_PAGE_N = 1_000
const DEFAULT_TRUNCATE_PAGE_N = 121359 // ~500MB with 4KB page size
const DEFAULT_BUSY_TIMEOUT = 1000

const META_DIR_SUFFIX = "-litestream"

export interface DBConfig {
  path: string
  replica?: S3.ReplicaClient
  monitorInterval?: number
  checkpointInterval?: number
  minCheckpointPageN?: number
  truncatePageN?: number
  busyTimeout?: number
}

interface SyncInfo {
  offset: number
  salt1: number
  salt2: number
  snapshotting: boolean
  reason?: string
}

export class DB {
  #path: string
  #metaPath: string
  #db: BunSqlite.Database | null = null
  #pageSize = 0
  #replica: S3.ReplicaClient | null = null

  // Configuration
  #monitorInterval: number
  #checkpointInterval: number
  #minCheckpointPageN: number
  #truncatePageN: number
  #busyTimeout: number

  // State
  #running = false
  #monitorTimer: Timer | null = null
  #syncedSinceCheckpoint = false
  #lastSyncAt: Date | null = null

  constructor(config: DBConfig) {
    this.#path = config.path
    this.#metaPath = NPath.join(
      NPath.dirname(config.path),
      "." + NPath.basename(config.path) + META_DIR_SUFFIX,
    )
    this.#replica = config.replica || null
    this.#monitorInterval = config.monitorInterval ?? DEFAULT_MONITOR_INTERVAL
    this.#checkpointInterval = config.checkpointInterval
      ?? DEFAULT_CHECKPOINT_INTERVAL
    this.#minCheckpointPageN = config.minCheckpointPageN
      ?? DEFAULT_MIN_CHECKPOINT_PAGE_N
    this.#truncatePageN = config.truncatePageN ?? DEFAULT_TRUNCATE_PAGE_N
    this.#busyTimeout = config.busyTimeout ?? DEFAULT_BUSY_TIMEOUT
  }

  getPath(): string {
    return this.#path
  }

  walPath(): string {
    return this.#path + "-wal"
  }

  ltxDir(): string {
    return NPath.join(this.#metaPath, "ltx")
  }

  ltxLevelDir(level: number): string {
    return NPath.join(this.ltxDir(), level.toString())
  }

  ltxPath(level: number, minTxid: Ltx.TXID, maxTxid: Ltx.TXID): string {
    return NPath.join(
      this.ltxLevelDir(level),
      Ltx.formatFilename(minTxid, maxTxid),
    )
  }

  async open(): Promise<void> {
    if (!NFs.existsSync(this.#path)) {
      throw new Error(`database file does not exist: ${this.#path}`)
    }

    await this.#init()

    if (this.#replica) {
      await this.#replica.init()
    }

    this.#running = true
    this.#startMonitor()
  }

  #init = async (): Promise<void> => {
    if (this.#db) return

    this.#db = new BunSqlite.Database(this.#path)
    this.#db.run(`PRAGMA busy_timeout = ${this.#busyTimeout}`)

    // Enable WAL mode
    const [{ journal_mode }] = this
      .#db
      .query<{ journal_mode: string }, []>(`PRAGMA journal_mode = WAL`)
      .all()
    if (journal_mode !== "wal") {
      throw new Error(`failed to enable WAL mode: ${journal_mode}`)
    }

    // Disable auto-checkpoint (we handle checkpointing)
    this.#db.run(`PRAGMA wal_autocheckpoint = 0`)

    // Get page size
    const [{ page_size }] = this
      .#db
      .query<{ page_size: number }, []>(`PRAGMA page_size`)
      .all()
    this.#pageSize = page_size

    // Create internal tables
    this.#db.run(`
			CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
			CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);
		`)

    NFs.mkdirSync(this.#metaPath, { recursive: true })
    NFs.mkdirSync(this.ltxLevelDir(0), { recursive: true })

    await this.#ensureWalExists()
  }

  /**
   * Ensure WAL file exists with at least one frame
   */
  #ensureWalExists = async (): Promise<void> => {
    const walPath = this.walPath()

    try {
      const stat = NFs.statSync(walPath)
      if (stat.size >= WAL_HEADER_SIZE) {
        return
      }
    } catch {
    }

    this.#db!.run(`
			INSERT INTO _litestream_seq (id, seq) VALUES (1, 1)
			ON CONFLICT (id) DO UPDATE SET seq = seq + 1
		`)
  }

  async close(): Promise<void> {
    this.#running = false

    // Stop monitor
    if (this.#monitorTimer) {
      clearInterval(this.#monitorTimer)
      this.#monitorTimer = null
    }

    // Final sync
    if (this.#db) {
      await this.sync()

      // Sync to replica
      if (this.#replica) {
        await this.syncReplica()
      }
    }

    // Close database
    if (this.#db) {
      this.#db.close()
      this.#db = null
    }
  }

  #startMonitor = (): void => {
    if (this.#monitorTimer) return

    let lastWalSize = 0
    let lastWalHeader: Buffer | null = null

    this.#monitorTimer = setInterval(async () => {
      if (!this.#running) return

      try {
        // Quick change detection
        const walPath = this.walPath()

        let walSize = 0
        try {
          const stat = NFs.statSync(walPath)
          walSize = stat.size
        } catch {
          return // No WAL yet
        }

        // Read WAL header for salt comparison
        let walHeader: Buffer | null = null
        try {
          const fd = NFs.openSync(walPath, "r")
          walHeader = Buffer.alloc(WAL_HEADER_SIZE)
          NFs.readSync(fd, walHeader, 0, WAL_HEADER_SIZE, 0)
          NFs.closeSync(fd)
        } catch {
          return
        }

        // Skip if unchanged
        if (
          walSize === lastWalSize && walHeader && lastWalHeader
            ?.equals(walHeader)
        ) {
          return
        }

        lastWalSize = walSize
        lastWalHeader = walHeader

        // Sync
        await this.sync()
      } catch (err) {
        console.error("sync error:", err)
      }
    }, this.#monitorInterval)
  }

  /**
   * Sync WAL changes to LTX files
   */
  async sync(): Promise<void> {
    if (!this.#db) {
      throw new Error("database not initialized")
    }

    // Ensure WAL exists
    await this.#ensureWalExists()

    // Get WAL size before sync
    const origWalSize = this.#getWalSize()

    // Verify current position and sync
    const info = await this.#verify()
    const synced = await this.#doSync(info)

    if (synced) {
      this.#syncedSinceCheckpoint = true
    }

    // Get WAL size after sync
    const newWalSize = this.#getWalSize()

    // Check if checkpoint needed
    await this.#checkpointIfNeeded(origWalSize, newWalSize)

    this.#lastSyncAt = new Date()
  }

  #getWalSize = (): number => {
    try {
      const stat = NFs.statSync(this.walPath())
      return stat.size
    } catch {
      return 0
    }
  }

  /**
   * Verify WAL state and determine sync parameters
   */
  #verify = async (): Promise<SyncInfo> => {
    const info: SyncInfo = {
      offset: WAL_HEADER_SIZE,
      salt1: 0,
      salt2: 0,
      snapshotting: true,
    }

    // Get current position from last LTX file
    const pos = await this.getPos()
    if (pos.txid === 0n) {
      return info // First sync
    }

    // Read last LTX file to get WAL position
    const ltxPath = this.ltxPath(0, pos.txid, pos.txid)
    try {
      const ltxData = NFs.readFileSync(ltxPath)
      const decoder = new Ltx.Decoder(ltxData)
      const header = decoder.decodeHeader()

      info.offset = Number(header.walOffset + header.walSize)
      info.salt1 = header.walSalt1
      info.salt2 = header.walSalt2
    } catch {
      // LTX file missing or corrupted, need snapshot
      info.reason = "ltx file missing or corrupted"
      return info
    }

    const walSize = this.#getWalSize()
    if (info.offset > walSize) {
      info.reason = "wal truncated by another process"
      info.offset = WAL_HEADER_SIZE
      return info
    }

    // Compare WAL header salt
    try {
      const walHeader = readHeader(this.walPath())
      if (walHeader.salt1 !== info.salt1 || walHeader.salt2 !== info.salt2) {
        info.reason = "wal salt mismatch"
        info.offset = WAL_HEADER_SIZE
        info.salt1 = walHeader.salt1
        info.salt2 = walHeader.salt2
        return info
      }
    } catch {
      return info
    }

    // No snapshot needed
    info.snapshotting = false
    return info
  }

  #doSync = async (info: SyncInfo): Promise<boolean> => {
    // Get next transaction ID
    const pos = await this.getPos()
    const txid = pos.txid + 1n

    let reader: WalReader
    try {
      if (info.offset === WAL_HEADER_SIZE) {
        reader = WalReader.open(this.walPath())
      } else {
        reader = WalReader.openWithOffset(
          this.walPath(),
          info.offset,
          info.salt1,
          info.salt2,
        )
      }
    } catch (err) {
      console.error("failed to open WAL:", err)
      return false
    }

    try {
      // Build page map
      const { map: pageMap, maxOffset, commit } = reader.pageMap()

      const walSize = maxOffset > 0 ? maxOffset - info.offset : 0

      // Skip if no changes and not snapshotting
      if (!info.snapshotting && walSize === 0) {
        return false
      }

      // Get database size
      const dbStat = NFs.statSync(this.#path)
      const dbCommit = commit > 0
        ? commit
        : Math.floor(dbStat.size / this.#pageSize)

      // Create LTX encoder
      const encoder = new Ltx.Encoder()

      const [salt1, salt2] = reader.salt()

      encoder.encodeHeader({
        version: 1,
        flags: Ltx.HEADER_FLAG_NO_CHECKSUM,
        pageSize: this.#pageSize,
        commit: dbCommit,
        minTxid: txid,
        maxTxid: txid,
        timestamp: BigInt(Date.now()),
        preApplyChecksum: 0n,
        walOffset: BigInt(info.offset),
        walSize: BigInt(walSize),
        walSalt1: salt1,
        walSalt2: salt2,
        nodeID: 0n,
      })

      // Write pages
      if (info.snapshotting) {
        // Full snapshot - read from DB + WAL
        await this.#writeLtxFromDb(encoder, pageMap, dbCommit)
      } else {
        // Incremental - read only from WAL
        await this.#writeLtxFromWal(encoder, pageMap)
      }

      // Close encoder
      const ltxData = encoder.close()

      // Write LTX file
      const ltxPath = this.ltxPath(0, txid, txid)
      NFs.mkdirSync(NPath.dirname(ltxPath), { recursive: true })
      NFs.writeFileSync(ltxPath + ".tmp", ltxData)
      NFs.renameSync(ltxPath + ".tmp", ltxPath)

      return true
    } finally {
      reader.close()
    }
  }

  #writeLtxFromDb = async (
    encoder: Ltx.Encoder,
    pageMap: Map<number, number>,
    commit: number,
  ): Promise<void> => {
    const lockPage = Ltx.lockPgno(this.#pageSize)
    const data = Buffer.alloc(this.#pageSize)

    // Open database and WAL files
    const dbFd = NFs.openSync(this.#path, "r")
    const walFd = NFs.openSync(this.walPath(), "r")

    try {
      for (let pgno = 1; pgno <= commit; pgno++) {
        if (pgno === lockPage) continue

        // Check if page is in WAL
        const walOffset = pageMap.get(pgno)
        if (walOffset !== undefined) {
          // Read from WAL
          NFs.readSync(
            walFd,
            data,
            0,
            this.#pageSize,
            walOffset + WAL_FRAME_HEADER_SIZE,
          )
        } else {
          // Read from database file
          const dbOffset = (pgno - 1) * this.#pageSize
          NFs.readSync(dbFd, data, 0, this.#pageSize, dbOffset)
        }

        encoder.encodePage({ pgno }, new Uint8Array(data))
      }
    } finally {
      NFs.closeSync(dbFd)
      NFs.closeSync(walFd)
    }
  }

  #writeLtxFromWal = async (
    encoder: Ltx.Encoder,
    pageMap: Map<number, number>,
  ): Promise<void> => {
    const walFd = NFs.openSync(this.walPath(), "r")
    const data = Buffer.alloc(this.#pageSize)

    try {
      // Sort page numbers for LTX
      const pgnos = Array.from(pageMap.keys()).sort((a, b) => a - b)

      for (const pgno of pgnos) {
        const offset = pageMap.get(pgno)!
        NFs.readSync(
          walFd,
          data,
          0,
          this.#pageSize,
          offset + WAL_FRAME_HEADER_SIZE,
        )
        encoder.encodePage({ pgno }, new Uint8Array(data))
      }
    } finally {
      NFs.closeSync(walFd)
    }
  }

  async getPos(): Promise<Ltx.Pos> {
    const { minTxid, maxTxid } = await this.#maxLtx()
    if (maxTxid === 0n) {
      return { txid: 0n, postApplyChecksum: 0n }
    }

    // Read LTX file for position
    const ltxPath = this.ltxPath(0, minTxid, maxTxid)
    try {
      const ltxData = NFs.readFileSync(ltxPath)
      const decoder = new Ltx.Decoder(ltxData)
      decoder.verify()
      return decoder.postApplyPos()
    } catch {
      return { txid: 0n, postApplyChecksum: 0n }
    }
  }

  #maxLtx = async (): Promise<{ minTxid: Ltx.TXID; maxTxid: Ltx.TXID }> => {
    const levelDir = this.ltxLevelDir(0)

    try {
      const entries = NFs.readdirSync(levelDir)
      let maxTxid = 0n
      let minTxid = 0n

      for (const entry of entries) {
        try {
          const { minTxid: min, maxTxid: max } = Ltx.parseFilename(entry)
          if (max > maxTxid) {
            minTxid = min
            maxTxid = max
          }
        } catch {
          // Skip invalid filenames
        }
      }

      return { minTxid, maxTxid }
    } catch {
      return { minTxid: 0n, maxTxid: 0n }
    }
  }

  #checkpointIfNeeded = async (
    origWalSize: number,
    newWalSize: number,
  ): Promise<void> => {
    if (this.#pageSize === 0) return

    // Emergency truncate checkpoint
    const truncateThreshold = calcSize(this.#pageSize, this.#truncatePageN)
    if (this.#truncatePageN > 0 && origWalSize >= truncateThreshold) {
      await this.checkpoint("TRUNCATE")
      return
    }

    // Regular checkpoint at min threshold
    const minThreshold = calcSize(this.#pageSize, this.#minCheckpointPageN)
    if (newWalSize >= minThreshold) {
      await this.checkpoint("PASSIVE")
      return
    }

    // Time-based checkpoint
    if (this.#checkpointInterval > 0 && this.#syncedSinceCheckpoint) {
      const dbStat = NFs.statSync(this.#path)
      const elapsed = Date.now() - dbStat.mtimeMs
      if (
        elapsed > this.#checkpointInterval
        && newWalSize > calcSize(this.#pageSize, 1)
      ) {
        await this.checkpoint("PASSIVE")
      }
    }
  }

  async checkpoint(
    mode: "PASSIVE" | "FULL" | "RESTART" | "TRUNCATE" = "PASSIVE",
  ): Promise<void> {
    if (!this.#db) return

    const { map: pageMap } = await this.#buildPageMap()
    if (pageMap.size > 0) {
      await this.sync()
    }

    try {
      this.#db.run(`PRAGMA wal_checkpoint(${mode})`)
    } catch (err) {
      if (mode === "PASSIVE") {
        // PASSIVE can fail with busy, that's ok
        return
      }
      throw err
    }

    // Write to internal table to ensure new WAL frame
    this.#db.run(`
			INSERT INTO _litestream_seq (id, seq) VALUES (1, 1)
			ON CONFLICT (id) DO UPDATE SET seq = seq + 1
		`)

    this.#syncedSinceCheckpoint = false
  }

  #buildPageMap = async (): Promise<
    { map: Map<number, number>; maxOffset: number }
  > => {
    try {
      const reader = WalReader.open(this.walPath())
      try {
        const { map, maxOffset } = reader.pageMap()
        return { map, maxOffset }
      } finally {
        reader.close()
      }
    } catch {
      return { map: new Map(), maxOffset: 0 }
    }
  }

  async syncReplica(): Promise<void> {
    if (!this.#replica) return

    // Get replica position
    const replicaPos = await this.#getReplicaPos()

    // Get local position
    const localPos = await this.getPos()

    // Upload missing LTX files
    for (let txid = replicaPos.txid + 1n; txid <= localPos.txid; txid++) {
      const ltxPath = this.ltxPath(0, txid, txid)
      try {
        const data = NFs.readFileSync(ltxPath)
        await this.#replica.writeLtxFile(0, txid, txid, data)
      } catch (err) {
        console.error(`failed to upload LTX file ${txid}:`, err)
        throw err
      }
    }
  }

  #getReplicaPos = async (): Promise<Ltx.Pos> => {
    if (!this.#replica) {
      return { txid: 0n, postApplyChecksum: 0n }
    }

    // Find max Ltx.TXID in replica
    const iterator = this.#replica.ltxFiles(0)
    let maxTxid = 0n

    let file = await iterator.next()
    while (file) {
      if (file.maxTxid > maxTxid) {
        maxTxid = file.maxTxid
      }
      file = await iterator.next()
    }
    iterator.close()

    return { txid: maxTxid, postApplyChecksum: 0n }
  }

  async snapshot(): Promise<Ltx.FileInfo | null> {
    if (!this.#replica) return null

    // Force a full sync
    await this.sync()

    const pos = await this.getPos()
    if (pos.txid === 0n) return null

    // Create snapshot (L1 compacted file)
    // For now, just upload the current state
    const ltxPath = this.ltxPath(0, pos.txid, pos.txid)
    const data = NFs.readFileSync(ltxPath)

    // Upload as snapshot (level -1 is snapshot level in litestream)
    return await this.#replica.writeLtxFile(-1, 1n, pos.txid, data)
  }
}

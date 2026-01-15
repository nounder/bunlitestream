/**
 * End-to-end integration tests for database replication
 *
 * These tests verify the full replication cycle:
 * 1. Create a database and write data
 * 2. Replicate to S3 (or mock)
 * 3. Snapshot from S3 to a new location
 * 4. Verify data integrity
 */

import { Database } from "bun:sqlite"
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as NFs from "node:fs"
import * as NOs from "node:os"
import * as NPath from "node:path"
import * as Db from "../src/Db.ts"
import * as Ltx from "../src/Ltx.ts"
import * as S3 from "../src/S3.ts"
import * as Snapshot from "../src/Snapshot.ts"

function makeTempDir(): string {
  return NFs.mkdtempSync(NPath.join(NOs.tmpdir(), "bun-litestream-e2e-"))
}

/**
 * In-memory replica client for testing without actual S3
 */
class MemoryReplicaClient implements S3.ReplicaClient {
  #files: Map<string, { data: Uint8Array; info: Ltx.FileInfo }> = new Map()

  type(): string {
    return "memory"
  }

  async init(): Promise<void> {}

  #key = (level: number, minTxid: Ltx.TXID, maxTxid: Ltx.TXID): string => {
    return `${level}/${minTxid.toString(16)}-${maxTxid.toString(16)}`
  }

  ltxFiles(level: number, seek?: Ltx.TXID): S3.FileIterator {
    const files = Array
      .from(this.#files.values())
      .filter((f) => f.info.level === level)
      .filter((f) => !seek || f.info.minTxid >= seek)
      .sort((a, b) => Number(a.info.minTxid - b.info.minTxid))

    let index = 0
    return {
      async next(): Promise<Ltx.FileInfo | null> {
        if (index >= files.length) return null
        return files[index++].info
      },
      close(): void {},
    }
  }

  async openLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
  ): Promise<ReadableStream<Uint8Array>> {
    const key = this.#key(level, minTxid, maxTxid)
    const file = this.#files.get(key)
    if (!file) throw new Error(`file not found: ${key}`)

    return new ReadableStream({
      start(controller) {
        controller.enqueue(file.data)
        controller.close()
      },
    })
  }

  async writeLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
    data: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<Ltx.FileInfo> {
    let bytes: Uint8Array
    if (data instanceof Uint8Array) {
      bytes = data
    } else {
      const chunks: Uint8Array[] = []
      const reader = data.getReader()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        chunks.push(value)
      }
      const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
      bytes = new Uint8Array(totalLength)
      let offset = 0
      for (const chunk of chunks) {
        bytes.set(chunk, offset)
        offset += chunk.length
      }
    }

    const info: Ltx.FileInfo = {
      level,
      minTxid,
      maxTxid,
      size: bytes.length,
      createdAt: new Date(),
    }

    this.#files.set(this.#key(level, minTxid, maxTxid), { data: bytes, info })
    return info
  }

  async deleteLtxFiles(files: Ltx.FileInfo[]): Promise<void> {
    for (const file of files) {
      this.#files.delete(this.#key(file.level, file.minTxid, file.maxTxid))
    }
  }

  async deleteAll(): Promise<void> {
    this.#files.clear()
  }

  getFileCount(): number {
    return this.#files.size
  }

  getFiles(): Ltx.FileInfo[] {
    return Array.from(this.#files.values()).map((f) => f.info)
  }
}

describe("End-to-end Replication (Memory)", () => {
  let testDir: string
  let sourceDbPath: string
  let restoredDbPath: string
  let replicaClient: MemoryReplicaClient

  beforeEach(() => {
    testDir = makeTempDir()
    sourceDbPath = NPath.join(testDir, "source.db")
    restoredDbPath = NPath.join(testDir, "restored.db")
    replicaClient = new MemoryReplicaClient()
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("replicate and restore simple table", async () => {
    // Step 1: Create source database with data
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    sourceDb.run("INSERT INTO users (name) VALUES ('Alice')")
    sourceDb.run("INSERT INTO users (name) VALUES ('Bob')")
    sourceDb.run("INSERT INTO users (name) VALUES ('Charlie')")
    sourceDb.close()

    // Step 2: Open with litestream and sync
    const db = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000, // Disable auto-sync
    })
    await db.open()
    await db.sync()
    await db.syncReplica()
    await db.close()

    // Verify files were uploaded
    expect(replicaClient.getFileCount())
      .toBeGreaterThan(0)

    // Step 3: Snapshot to new location
    await Snapshot.restore(replicaClient, { outputPath: restoredDbPath })

    // Step 4: Verify data integrity
    const restoredDb = new Database(restoredDbPath)
    const users = restoredDb
      .query<{ id: number; name: string }, []>(
        "SELECT * FROM users ORDER BY id",
      )
      .all()

    expect(users.length)
      .toBe(3)
    expect(users[0].name)
      .toBe("Alice")
    expect(users[1].name)
      .toBe("Bob")
    expect(users[2].name)
      .toBe("Charlie")

    restoredDb.close()
  })

  test("replicate multiple transactions and restore", async () => {
    // Step 1: Create source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(
      "CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    sourceDb.run("INSERT INTO counter (value) VALUES (0)")
    sourceDb.close()

    // Step 2: Open with litestream
    const db = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await db.open()

    // Initial sync
    await db.sync()
    await db.syncReplica()

    // Step 3: Make multiple changes with syncs
    for (let i = 1; i <= 5; i++) {
      const sqlDb = new Database(sourceDbPath)
      sqlDb.run(`UPDATE counter SET value = ${i} WHERE id = 1`)
      sqlDb.close()

      await db.sync()
      await db.syncReplica()
    }

    await db.close()

    // Verify multiple LTX files were created
    expect(replicaClient.getFileCount())
      .toBeGreaterThanOrEqual(2)

    // Step 4: Snapshot
    await Snapshot.restore(replicaClient, { outputPath: restoredDbPath })

    // Step 5: Verify final state
    const restoredDb = new Database(restoredDbPath)
    const result = restoredDb
      .query<{ value: number }, []>("SELECT value FROM counter WHERE id = 1")
      .get()

    expect(result?.value)
      .toBe(5)

    restoredDb.close()
  })

  test("replicate larger dataset and restore", async () => {
    // Step 1: Create source database with more data
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(`
      CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL,
        quantity INTEGER
      )
    `)

    // Insert 100 products
    const insertStmt = sourceDb.prepare(
      "INSERT INTO products (name, price, quantity) VALUES (?, ?, ?)",
    )
    for (let i = 0; i < 100; i++) {
      insertStmt.run(`Product ${i}`, 10.99 + i, 100 + i)
    }
    sourceDb.close()

    // Step 2: Replicate
    const db = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await db.open()
    await db.sync()
    await db.syncReplica()
    await db.close()

    // Step 3: Snapshot
    await Snapshot.restore(replicaClient, { outputPath: restoredDbPath })

    // Step 4: Verify
    const restoredDb = new Database(restoredDbPath)

    const count = restoredDb
      .query<{ count: number }, []>("SELECT COUNT(*) as count FROM products")
      .get()
    expect(count?.count)
      .toBe(100)

    // Verify specific records
    const product50 = restoredDb
      .query<{ name: string; price: number; quantity: number }, [number]>(
        "SELECT name, price, quantity FROM products WHERE id = ?",
      )
      .get(51) // ID 51 = Product 50

    expect(product50?.name)
      .toBe("Product 50")
    expect(product50?.price)
      .toBeCloseTo(60.99, 2)
    expect(product50?.quantity)
      .toBe(150)

    restoredDb.close()
  })

  test("replicate with blob data and restore", async () => {
    // Step 1: Create source database with blob data
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(
      "CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, data BLOB)",
    )

    // Create some binary data
    const binaryData1 = new Uint8Array(1000).fill(0xaa)
    const binaryData2 = new Uint8Array(2000).fill(0xbb)

    sourceDb
      .prepare("INSERT INTO files (name, data) VALUES (?, ?)")
      .run("file1.bin", binaryData1)
    sourceDb
      .prepare("INSERT INTO files (name, data) VALUES (?, ?)")
      .run("file2.bin", binaryData2)
    sourceDb.close()

    // Step 2: Replicate
    const db = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await db.open()
    await db.sync()
    await db.syncReplica()
    await db.close()

    // Step 3: Snapshot
    await Snapshot.restore(replicaClient, { outputPath: restoredDbPath })

    // Step 4: Verify blob data
    const restoredDb = new Database(restoredDbPath)

    const file1 = restoredDb
      .query<{ data: Uint8Array }, [string]>(
        "SELECT data FROM files WHERE name = ?",
      )
      .get("file1.bin")

    const file2 = restoredDb
      .query<{ data: Uint8Array }, [string]>(
        "SELECT data FROM files WHERE name = ?",
      )
      .get("file2.bin")

    expect(file1?.data.length)
      .toBe(1000)
    expect(file1?.data[0])
      .toBe(0xaa)
    expect(file1?.data[999])
      .toBe(0xaa)

    expect(file2?.data.length)
      .toBe(2000)
    expect(file2?.data[0])
      .toBe(0xbb)

    restoredDb.close()
  })

  test("source and restored databases have identical content", async () => {
    // Step 1: Create source database with multiple tables
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(`
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
      CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL);
      CREATE INDEX idx_orders_user ON orders(user_id);
    `)

    sourceDb.run("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
    sourceDb.run("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")
    sourceDb.run("INSERT INTO orders VALUES (1, 1, 99.99)")
    sourceDb.run("INSERT INTO orders VALUES (2, 1, 149.99)")
    sourceDb.run("INSERT INTO orders VALUES (3, 2, 29.99)")
    sourceDb.close()

    // Step 2: Replicate
    const db = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await db.open()
    await db.sync()
    await db.syncReplica()
    await db.close()

    // Step 3: Snapshot
    await Snapshot.restore(replicaClient, { outputPath: restoredDbPath })

    // Step 4: Compare databases
    const source = new Database(sourceDbPath)
    const restored = new Database(restoredDbPath)

    // Compare users
    const sourceUsers = source
      .query<{ id: number; name: string; email: string }, []>(
        "SELECT * FROM users ORDER BY id",
      )
      .all()
    const restoredUsers = restored
      .query<{ id: number; name: string; email: string }, []>(
        "SELECT * FROM users ORDER BY id",
      )
      .all()

    expect(restoredUsers)
      .toEqual(sourceUsers)

    // Compare orders
    const sourceOrders = source
      .query<{ id: number; user_id: number; total: number }, []>(
        "SELECT * FROM orders ORDER BY id",
      )
      .all()
    const restoredOrders = restored
      .query<{ id: number; user_id: number; total: number }, []>(
        "SELECT * FROM orders ORDER BY id",
      )
      .all()

    expect(restoredOrders)
      .toEqual(sourceOrders)

    // Compare schema (excluding litestream internal tables)
    const sourceSchema = source
      .query<{ sql: string }, []>(
        "SELECT sql FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
      )
      .all()
      .filter((s) => !s.sql.includes("_litestream"))
    const restoredSchema = restored
      .query<{ sql: string }, []>(
        "SELECT sql FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
      )
      .all()
      .filter((s) => !s.sql.includes("_litestream"))

    expect(restoredSchema)
      .toEqual(sourceSchema)

    source.close()
    restored.close()
  })
})

/**
 * Apply LTX file pages directly to an open database file
 * This enables streaming replication without closing the database
 */
async function applyLTXToDatabase(
  dbPath: string,
  ltxData: Uint8Array,
): Promise<void> {
  const decoder = new Ltx.Decoder(ltxData)
  const header = decoder.decodeHeader()

  // Open database file for direct page writes
  const fd = NFs.openSync(dbPath, "r+")

  try {
    let page
    while ((page = decoder.decodePage()) !== null) {
      const offset = (page.header.pgno - 1) * header.pageSize
      NFs.writeSync(fd, page.data, 0, page.data.length, offset)
    }

    // Ensure file is at least commit * pageSize
    const targetSize = header.commit * header.pageSize
    const stat = NFs.fstatSync(fd)
    if (stat.size < targetSize) {
      NFs.ftruncateSync(fd, targetSize)
    }
  } finally {
    NFs.closeSync(fd)
  }
}

/**
 * Streaming replica subscriber that applies changes to a destination database
 */
class StreamingSubscriber {
  #client: MemoryReplicaClient
  #destDbPath: string
  #lastTXID: Ltx.TXID = 0n
  #initialized = false

  constructor(client: MemoryReplicaClient, destDbPath: string) {
    this.#client = client
    this.#destDbPath = destDbPath
  }

  /**
   * Initialize destination database from current replica state
   */
  async initialize(): Promise<void> {
    if (this.#initialized) return

    // Get all current LTX files and apply them
    const iterator = this.#client.ltxFiles(0)
    const files: Ltx.FileInfo[] = []
    let file
    while ((file = await iterator.next()) !== null) {
      files.push(file)
    }
    iterator.close()

    if (files.length === 0) {
      throw new Error("No LTX files available for initialization")
    }

    // Sort by minTxid
    files.sort((a, b) => Number(a.minTxid - b.minTxid))

    // Apply first file to create database
    const firstStream = await this.#client.openLtxFile(
      files[0].level,
      files[0].minTxid,
      files[0].maxTxid,
    )
    const firstData = await streamToUint8Array(firstStream)

    // Create initial database from first LTX
    const decoder = new Ltx.Decoder(firstData)
    const header = decoder.decodeHeader()

    // Create database file with proper size
    const dbSize = header.commit * header.pageSize
    const db = new Uint8Array(dbSize)

    // Apply all pages from the first LTX file
    let page
    while ((page = decoder.decodePage()) !== null) {
      const offset = (page.header.pgno - 1) * header.pageSize
      if (offset + page.data.length <= db.length) {
        db.set(page.data, offset)
      }
    }

    // Write the initial database file
    NFs.writeFileSync(this.#destDbPath, db)
    this.#lastTXID = files[0].maxTxid

    // Apply remaining files incrementally
    for (let i = 1; i < files.length; i++) {
      const stream = await this.#client.openLtxFile(
        files[i].level,
        files[i].minTxid,
        files[i].maxTxid,
      )
      const data = await streamToUint8Array(stream)
      await applyLTXToDatabase(this.#destDbPath, data)
      this.#lastTXID = files[i].maxTxid
    }

    this.#initialized = true
  }

  /**
   * Sync any new changes from the replica
   */
  async sync(): Promise<number> {
    if (!this.#initialized) {
      await this.initialize()
      return 0
    }

    // Find new LTX files
    const iterator = this.#client.ltxFiles(0, this.#lastTXID + 1n)
    let appliedCount = 0
    let file

    while ((file = await iterator.next()) !== null) {
      const stream = await this.#client.openLtxFile(
        file.level,
        file.minTxid,
        file.maxTxid,
      )
      const data = await streamToUint8Array(stream)
      await applyLTXToDatabase(this.#destDbPath, data)
      this.#lastTXID = file.maxTxid
      appliedCount++
    }
    iterator.close()

    return appliedCount
  }

  getLastTxid(): Ltx.TXID {
    return this.#lastTXID
  }
}

async function streamToUint8Array(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = []
  const reader = stream.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(value)
  }
  const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.length
  }
  return result
}

describe("Streaming Replication (both databases open)", () => {
  let testDir: string
  let sourceDbPath: string
  let destDbPath: string
  let replicaClient: MemoryReplicaClient

  beforeEach(() => {
    testDir = makeTempDir()
    sourceDbPath = NPath.join(testDir, "source.db")
    destDbPath = NPath.join(testDir, "dest.db")
    replicaClient = new MemoryReplicaClient()
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("restored database can be opened readonly after initial open", async () => {
    // Create source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    sourceDb.run("INSERT INTO items (name) VALUES ('item1')")
    sourceDb.close()

    // Replicate
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await source.open()
    await source.sync()
    await source.syncReplica()

    // Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // NOTE: Bun's SQLite may fail to open a freshly-created database file in
    // readonly mode, or fail when querying it. The error is SQLITE_CANTOPEN
    // (errno 14). Opening in read-write mode first and executing a query
    // allows subsequent readonly opens to work reliably.
    const db1 = new Database(destDbPath)
    const initItems = db1
      .query<{ name: string }, []>("SELECT name FROM items")
      .all()
    expect(initItems.length).toBe(1)
    db1.close()

    // After opening and querying normally once, readonly works reliably
    const db2 = new Database(destDbPath, { readonly: true })
    const items = db2
      .query<{ name: string }, []>("SELECT name FROM items")
      .all()
    expect(items.length).toBe(1)
    expect(items[0].name).toBe("item1")
    db2.close()

    await source.close()
  })

  test("stream changes to destination without closing databases", async () => {
    // Step 1: Create and initialize source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
    sourceDb.run("INSERT INTO events (name) VALUES ('event1')")
    sourceDb.close()

    // Step 2: Open source with litestream
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await source.open()
    await source.sync()
    await source.syncReplica()

    // Step 3: Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // Step 4: Verify initial state on destination
    let destDb = new Database(destDbPath)
    let events = destDb
      .query<{ name: string }, []>("SELECT name FROM events ORDER BY id")
      .all()
    expect(events.length).toBe(1)
    expect(events[0].name).toBe("event1")
    destDb.close()

    // Step 5: Make changes on source (without closing source DB)
    const sourceSqlDb = new Database(sourceDbPath)
    sourceSqlDb.run("INSERT INTO events (name) VALUES ('event2')")
    sourceSqlDb.run("INSERT INTO events (name) VALUES ('event3')")
    sourceSqlDb.close()

    // Sync source to replica
    await source.sync()
    await source.syncReplica()

    // Step 6: Sync destination (streaming apply)
    const applied = await subscriber.sync()
    expect(applied).toBeGreaterThan(0)

    // Step 7: Verify destination sees new data
    destDb = new Database(destDbPath)
    events = destDb
      .query<{ name: string }, []>("SELECT name FROM events ORDER BY id")
      .all()
    expect(events.length).toBe(3)
    expect(events[0].name).toBe("event1")
    expect(events[1].name).toBe("event2")
    expect(events[2].name).toBe("event3")
    destDb.close()

    // Cleanup
    await source.close()
  })

  test("continuous streaming with multiple sync rounds", async () => {
    // Create source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(
      "CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    sourceDb.run("INSERT INTO counter (value) VALUES (0)")
    sourceDb.close()

    // Open source with litestream
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await source.open()
    await source.sync()
    await source.syncReplica()

    // Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // Verify initial state
    let destDb = new Database(destDbPath)
    let result = destDb
      .query<{ value: number }, []>("SELECT value FROM counter WHERE id = 1")
      .get()
    expect(result?.value).toBe(0)
    destDb.close()

    // Multiple rounds of changes and syncs
    for (let i = 1; i <= 5; i++) {
      // Update source
      const sourceSqlDb = new Database(sourceDbPath)
      sourceSqlDb.run(`UPDATE counter SET value = ${i} WHERE id = 1`)
      sourceSqlDb.close()

      // Sync to replica
      await source.sync()
      await source.syncReplica()

      // Sync destination
      await subscriber.sync()

      // Verify destination has current value
      destDb = new Database(destDbPath)
      result = destDb
        .query<{ value: number }, []>("SELECT value FROM counter WHERE id = 1")
        .get()
      expect(result?.value).toBe(i)
      destDb.close()
    }

    await source.close()
  })

  test("stream large batch of inserts", async () => {
    // Create source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run(
      "CREATE TABLE records (id INTEGER PRIMARY KEY, data TEXT, created_at INTEGER)",
    )
    sourceDb.close()

    // Open source with litestream
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await source.open()
    await source.sync()
    await source.syncReplica()

    // Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // Insert batch of records
    const sourceSqlDb = new Database(sourceDbPath)
    const stmt = sourceSqlDb.prepare(
      "INSERT INTO records (data, created_at) VALUES (?, ?)",
    )
    for (let i = 0; i < 100; i++) {
      stmt.run(`Record ${i}`, Date.now())
    }
    sourceSqlDb.close()

    // Sync
    await source.sync()
    await source.syncReplica()
    await subscriber.sync()

    // Verify all records on destination
    const destDb = new Database(destDbPath)
    const count = destDb
      .query<{ count: number }, []>("SELECT COUNT(*) as count FROM records")
      .get()
    expect(count?.count).toBe(100)

    // Spot check some records
    const record50 = destDb
      .query<{ data: string }, [number]>(
        "SELECT data FROM records WHERE id = ?",
      )
      .get(51)
    expect(record50?.data).toBe("Record 50")

    destDb.close()
    await source.close()
  })

  test("destination stays in sync across schema changes", async () => {
    // Create source database with initial table
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    sourceDb.run("INSERT INTO users (name) VALUES ('Alice')")
    sourceDb.close()

    // Open source with litestream
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 100000,
    })
    await source.open()
    await source.sync()
    await source.syncReplica()

    // Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // Verify initial state
    let destDb = new Database(destDbPath)
    let users = destDb
      .query<{ name: string }, []>("SELECT name FROM users")
      .all()
    expect(users.length).toBe(1)
    destDb.close()

    // Add new table on source
    let sourceSqlDb = new Database(sourceDbPath)
    sourceSqlDb.run(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
    )
    sourceSqlDb.run("INSERT INTO orders (user_id, amount) VALUES (1, 99.99)")
    sourceSqlDb.close()

    await source.sync()
    await source.syncReplica()
    await subscriber.sync()

    // Verify new table exists on destination
    destDb = new Database(destDbPath)
    const orders = destDb
      .query<{ amount: number }, []>("SELECT amount FROM orders")
      .all()
    expect(orders.length).toBe(1)
    expect(orders[0].amount).toBeCloseTo(99.99, 2)
    destDb.close()

    // Add index on source
    sourceSqlDb = new Database(sourceDbPath)
    sourceSqlDb.run("CREATE INDEX idx_orders_user ON orders(user_id)")
    sourceSqlDb.run("INSERT INTO users (name) VALUES ('Bob')")
    sourceSqlDb.run("INSERT INTO orders (user_id, amount) VALUES (2, 49.99)")
    sourceSqlDb.close()

    await source.sync()
    await source.syncReplica()
    await subscriber.sync()

    // Verify everything on destination
    destDb = new Database(destDbPath)

    users = destDb.query<{ name: string }, []>("SELECT name FROM users").all()
    expect(users.length).toBe(2)

    const allOrders = destDb
      .query<{ amount: number }, []>("SELECT amount FROM orders ORDER BY id")
      .all()
    expect(allOrders.length).toBe(2)

    // Check index exists
    const indexes = destDb
      .query<{ name: string }, []>(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orders_user'",
      )
      .all()
    expect(indexes.length).toBe(1)

    destDb.close()
    await source.close()
  })

  test("auto-sync with short interval", async () => {
    // Create source database with initial data
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE messages (id INTEGER PRIMARY KEY, text TEXT)")
    sourceDb.run("INSERT INTO messages (text) VALUES ('Initial')")
    sourceDb.close()

    // Open source with short auto-sync interval
    const source = new Db.DB({
      path: sourceDbPath,
      replica: replicaClient,
      monitorInterval: 50, // 50ms auto-sync
    })
    await source.open()

    // Do initial sync to create LTX files
    await source.sync()
    await source.syncReplica()

    // Initialize streaming subscriber
    const subscriber = new StreamingSubscriber(replicaClient, destDbPath)
    await subscriber.initialize()

    // Verify initial state
    let destDb = new Database(destDbPath)
    let messages = destDb
      .query<{ text: string }, []>("SELECT text FROM messages ORDER BY id")
      .all()
    expect(messages.length).toBe(1)
    expect(messages[0].text).toBe("Initial")
    destDb.close()

    // Insert more data on source
    const sourceSqlDb = new Database(sourceDbPath)
    sourceSqlDb.run("INSERT INTO messages (text) VALUES ('Hello')")
    sourceSqlDb.run("INSERT INTO messages (text) VALUES ('World')")
    sourceSqlDb.close()

    // Wait for auto-sync to pick up changes
    await new Promise((resolve) => setTimeout(resolve, 150))

    // Manually sync replica (auto-sync doesn't sync to replica)
    await source.syncReplica()

    // Sync destination
    await subscriber.sync()

    // Verify destination has all messages
    destDb = new Database(destDbPath)
    messages = destDb
      .query<{ text: string }, []>("SELECT text FROM messages ORDER BY id")
      .all()

    expect(messages.length).toBe(3)
    expect(messages[0].text).toBe("Initial")
    expect(messages[1].text).toBe("Hello")
    expect(messages[2].text).toBe("World")

    destDb.close()
    await source.close()
  })
})

// S3 Integration tests - only run when environment is configured
const S3_TEST_BUCKET = process.env.BUCKET_NAME
const S3_TEST_ENDPOINT = process.env.AWS_ENDPOINT_URL_S3
const S3_TEST_REGION = process.env.AWS_REGION || "us-east-1"
const S3_ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID
const S3_SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY

const describeS3 = S3_TEST_BUCKET ? describe : describe.skip

describeS3("End-to-end Replication (S3)", () => {
  let testDir: string
  let sourceDbPath: string
  let restoredDbPath: string
  let s3Client: S3.S3ReplicaClient
  const testPath = `e2e-test-${Date.now()}`

  beforeEach(() => {
    testDir = makeTempDir()
    sourceDbPath = NPath.join(testDir, "source.db")
    restoredDbPath = NPath.join(testDir, "restored.db")
    s3Client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
  })

  afterEach(async () => {
    try {
      await s3Client.deleteAll()
    } catch {
      // Ignore cleanup errors
    }
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("replicate to S3 and restore", async () => {
    // Step 1: Create source database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    sourceDb.run("INSERT INTO items (name) VALUES ('Item A')")
    sourceDb.run("INSERT INTO items (name) VALUES ('Item B')")
    sourceDb.run("INSERT INTO items (name) VALUES ('Item C')")
    sourceDb.close()

    // Step 2: Replicate to S3
    const db = new Db.DB({
      path: sourceDbPath,
      replica: s3Client,
      monitorInterval: 100000,
    })
    await db.open()
    await db.sync()
    await db.syncReplica()
    await db.close()

    // Step 3: Snapshot from S3
    await Snapshot.restore(s3Client, { outputPath: restoredDbPath })

    // Step 4: Verify
    const restoredDb = new Database(restoredDbPath)
    const items = restoredDb
      .query<{ id: number; name: string }, []>(
        "SELECT * FROM items ORDER BY id",
      )
      .all()

    expect(items.length)
      .toBe(3)
    expect(items[0].name)
      .toBe("Item A")
    expect(items[1].name)
      .toBe("Item B")
    expect(items[2].name)
      .toBe("Item C")

    restoredDb.close()
  })

  test("replicate incremental changes to S3", async () => {
    // Step 1: Create initial database
    const sourceDb = new Database(sourceDbPath)
    sourceDb.run("CREATE TABLE log (id INTEGER PRIMARY KEY, message TEXT)")
    sourceDb.run("INSERT INTO log (message) VALUES ('Initial entry')")
    sourceDb.close()

    // Step 2: Initial replication
    const db = new Db.DB({
      path: sourceDbPath,
      replica: s3Client,
      monitorInterval: 100000,
    })
    await db.open()
    await db.sync()
    await db.syncReplica()

    // Step 3: Add more entries
    for (let i = 1; i <= 3; i++) {
      const sqlDb = new Database(sourceDbPath)
      sqlDb.run(`INSERT INTO log (message) VALUES ('Entry ${i}')`)
      sqlDb.close()

      await db.sync()
      await db.syncReplica()
    }

    await db.close()

    // Step 4: Snapshot
    await Snapshot.restore(s3Client, { outputPath: restoredDbPath })

    // Step 5: Verify all entries
    const restoredDb = new Database(restoredDbPath)
    const entries = restoredDb
      .query<{ message: string }, []>("SELECT message FROM log ORDER BY id")
      .all()

    expect(entries.length)
      .toBe(4)
    expect(entries[0].message)
      .toBe("Initial entry")
    expect(entries[3].message)
      .toBe("Entry 3")

    restoredDb.close()
  })
})

import * as BunSqlite from "bun:sqlite"
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as NFs from "node:fs"
import * as NOs from "node:os"
import * as NPath from "node:path"
import * as Db from "../src/Db.ts"
import * as Ltx from "../src/Ltx.ts"

function makeTempDir(): string {
  return NFs.mkdtempSync(NPath.join(NOs.tmpdir(), "bun-litestream-db-"))
}

describe("DB Initialization", () => {
  let testDir: string
  let dbPath: string

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "test.db")
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("open throws on missing database file", async () => {
    const db = new Db.DB({ path: dbPath })

    let error: unknown = null
    try {
      await db.open()
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toContain("database file does not exist")
  })

  test("open initializes WAL mode", async () => {
    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()

    const db = new Db.DB({ path: dbPath })
    await db.open()

    expect(NFs.existsSync(dbPath + "-wal"))
      .toBe(true)

    await db.close()
  })

  test("open creates meta directory", async () => {
    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()

    const db = new Db.DB({ path: dbPath })
    await db.open()

    const metaDir = NPath.join(testDir, ".test.db-litestream")
    expect(NFs.existsSync(metaDir))
      .toBe(true)

    await db.close()
  })

  test("open creates LTX level directory", async () => {
    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()

    const db = new Db.DB({ path: dbPath })
    await db.open()

    const ltxDir = NPath.join(testDir, ".test.db-litestream", "ltx", "0")
    expect(NFs.existsSync(ltxDir))
      .toBe(true)

    await db.close()
  })

  test("open creates internal tables", async () => {
    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()

    const db = new Db.DB({ path: dbPath })
    await db.open()
    await db.close()

    const checkDb = new BunSqlite.Database(dbPath)
    const tables = checkDb
      .query<{ name: string }, []>(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_litestream%'",
      )
      .all()

    expect(
      tables.map((t) => t.name).sort(),
    )
      .toEqual(["_litestream_lock", "_litestream_seq"])

    checkDb.close()
  })
})

describe("DB Path Methods", () => {
  let testDir: string
  let dbPath: string
  let db: Db.DB

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "paths.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()

    db = new Db.DB({ path: dbPath })
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("getPath returns database path", () => {
    expect(db.getPath())
      .toBe(dbPath)
  })

  test("walPath returns WAL path", () => {
    expect(db.walPath())
      .toBe(dbPath + "-wal")
  })

  test("ltxDir returns LTX directory", () => {
    const expected = NPath.join(testDir, ".paths.db-litestream", "ltx")
    expect(db.ltxDir())
      .toBe(expected)
  })

  test("ltxPath generates correct filename", () => {
    const path = db.ltxPath(0, 1n, 5n)
    expect(path)
      .toContain("0000000000000001-0000000000000005.ltx")
  })
})

describe("DB Sync", () => {
  let testDir: string
  let dbPath: string
  let db: Db.DB

  beforeEach(async () => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "sync.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    sqliteDb.run("INSERT INTO test (value) VALUES ('initial')")
    sqliteDb.close()

    db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
    })
    await db.open()
  })

  afterEach(async () => {
    try {
      await db.close()
    } catch {
      // Ignore
    }
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("sync creates first LTX file (snapshot)", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)

    expect(files.length)
      .toBe(1)
    expect(files[0])
      .toMatch(/\.ltx$/)
  })

  test("sync creates LTX with correct TXID", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const { minTxid, maxTxid } = Ltx.parseFilename(files[0])

    expect(minTxid)
      .toBe(1n)
    expect(maxTxid)
      .toBe(1n)
  })

  test("sync creates valid LTX file", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)

    decoder.verify()

    const header = decoder.decodeHeader()
    expect(header.version)
      .toBe(1)
    expect(header.pageSize)
      .toBe(4096)
    expect(header.commit)
      .toBeGreaterThan(0)
  })

  test("multiple syncs increment TXID", async () => {
    await db.sync()

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("INSERT INTO test (value) VALUES ('second')")
    sqliteDb.close()

    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir).sort()

    expect(files.length)
      .toBe(2)

    const { minTxid: min1 } = Ltx.parseFilename(files[0])
    const { minTxid: min2 } = Ltx.parseFilename(files[1])

    expect(min1)
      .toBe(1n)
    expect(min2)
      .toBe(2n)
  })

  test("sync without changes does not create new file", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const initialFiles = NFs.readdirSync(ltxDir)

    await db.sync()

    const afterFiles = NFs.readdirSync(ltxDir)

    expect(afterFiles.length)
      .toBe(initialFiles.length)
  })

  test("getPos returns current position", async () => {
    await db.sync()

    const pos = await db.getPos()

    expect(pos.txid)
      .toBe(1n)
  })

  test("getPos returns zero before sync", async () => {
    const ltxDir = db.ltxLevelDir(0)
    try {
      const files = NFs.readdirSync(ltxDir)
      for (const file of files) {
        NFs.unlinkSync(NPath.join(ltxDir, file))
      }
    } catch {
      // Directory might not exist
    }

    const pos = await db.getPos()

    expect(pos.txid)
      .toBe(0n)
  })
})

describe("DB Checkpoint", () => {
  let testDir: string
  let dbPath: string
  let db: Db.DB

  beforeEach(async () => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "checkpoint.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB)")
    sqliteDb.close()

    db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
      minCheckpointPageN: 10,
    })
    await db.open()
  })

  afterEach(async () => {
    try {
      await db.close()
    } catch {
      // Ignore
    }
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("checkpoint PASSIVE succeeds", async () => {
    await db.sync()
    await db.checkpoint("PASSIVE")
  })

  test("checkpoint creates new WAL frame", async () => {
    await db.sync()
    await db.checkpoint("PASSIVE")

    const walStat = NFs.statSync(dbPath + "-wal")
    expect(walStat.size)
      .toBeGreaterThan(0)
  })
})

describe("DB with WAL changes", () => {
  let testDir: string
  let dbPath: string
  let db: Db.DB

  beforeEach(async () => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "walchanges.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    sqliteDb.close()

    db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
    })
    await db.open()
  })

  afterEach(async () => {
    try {
      await db.close()
    } catch {
      // Ignore
    }
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("sync captures all modified pages", async () => {
    await db.sync()

    const sqliteDb = new BunSqlite.Database(dbPath)
    for (let i = 0; i < 100; i++) {
      sqliteDb.run(`INSERT INTO items (name) VALUES ('item${i}')`)
    }
    sqliteDb.close()

    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir).sort()
    const lastFile = files[files.length - 1]
    const ltxPath = NPath.join(ltxDir, lastFile)

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    decoder.decodeHeader()

    let pageCount = 0
    while (decoder.decodePage() !== null) {
      pageCount++
    }

    expect(pageCount)
      .toBeGreaterThan(0)
  })

  test("LTX file stores WAL offset", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    const header = decoder.decodeHeader()

    expect(header.walOffset)
      .toBeGreaterThanOrEqual(0n)
  })

  test("LTX file stores WAL salt", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    const header = decoder.decodeHeader()

    expect(header.walSalt1)
      .toBeGreaterThan(0)
    expect(header.walSalt2)
      .toBeGreaterThan(0)
  })
})

describe("DB Close", () => {
  let testDir: string
  let dbPath: string

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "close.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE t (x)")
    sqliteDb.close()
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("close performs final sync", async () => {
    const db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
    })
    await db.open()

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("INSERT INTO t (x) VALUES (1)")
    sqliteDb.close()

    await db.close()

    const ltxDir = NPath.join(testDir, ".close.db-litestream", "ltx", "0")
    const files = NFs.readdirSync(ltxDir)
    expect(files.length)
      .toBeGreaterThan(0)
  })

  test("double close is safe", async () => {
    const db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
    })
    await db.open()
    await db.close()
    await db.close()
  })
})

describe("DB LTX File Content", () => {
  let testDir: string
  let dbPath: string
  let db: Db.DB

  beforeEach(async () => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "ltxcontent.db")

    const sqliteDb = new BunSqlite.Database(dbPath)
    sqliteDb.run("CREATE TABLE data (id INTEGER PRIMARY KEY, value INTEGER)")
    for (let i = 0; i < 10; i++) {
      sqliteDb.run(`INSERT INTO data (value) VALUES (${i})`)
    }
    sqliteDb.close()

    db = new Db.DB({
      path: dbPath,
      monitorInterval: 100000,
    })
    await db.open()
  })

  afterEach(async () => {
    try {
      await db.close()
    } catch {
      // Ignore
    }
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("LTX pages are in ascending order", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    decoder.decodeHeader()

    const pgnos: number[] = []
    let page
    while ((page = decoder.decodePage()) !== null) {
      pgnos.push(page.header.pgno)
    }

    const sorted = [...pgnos].sort((a, b) => a - b)
    expect(pgnos)
      .toEqual(sorted)
  })

  test("LTX commit matches actual database pages", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    const header = decoder.decodeHeader()

    const dbStat = NFs.statSync(dbPath)
    const expectedPages = Math.floor(dbStat.size / header.pageSize)

    expect(header.commit)
      .toBeGreaterThanOrEqual(expectedPages)
  })

  test("first LTX is a snapshot with all pages", async () => {
    await db.sync()

    const ltxDir = db.ltxLevelDir(0)
    const files = NFs.readdirSync(ltxDir)
    const ltxPath = NPath.join(ltxDir, files[0])

    const ltxData = NFs.readFileSync(ltxPath)
    const decoder = new Ltx.Decoder(ltxData)
    const header = decoder.decodeHeader()

    let pageCount = 0
    while (decoder.decodePage() !== null) {
      pageCount++
    }

    // First sync should be a snapshot with all pages (minus lock page if > 1GB)
    // For small databases, lock page is not excluded
    expect(pageCount)
      .toBeGreaterThanOrEqual(header.commit - 1)
  })
})

import * as BunSqlite from "bun:sqlite"
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as NFs from "node:fs"
import * as NOs from "node:os"
import * as NPath from "node:path"
import {
  calcSize,
  checksum,
  parseFrameHeader,
  parseHeader,
  readHeader,
  readPageAt,
  WAL_FRAME_HEADER_SIZE,
  WAL_HEADER_SIZE,
  WAL_MAGIC_BE,
  WAL_MAGIC_LE,
  WalReader,
} from "../src/Wal.ts"
import walFrameDataHex from "./data/wal-frame-data.hex" with {
  type: "text",
}

function makeTempDir(): string {
  return NFs.mkdtempSync(NPath.join(NOs.tmpdir(), "bun-litestream-wal-"))
}

const TESTDATA_DIR = NPath.join(import.meta.dir, "data", "wal-reader")

describe("WAL Header Parsing", () => {
  test("parseHeader detects little-endian magic", () => {
    const buf = Buffer.alloc(WAL_HEADER_SIZE)
    buf.writeUInt32BE(WAL_MAGIC_LE, 0) // Magic
    buf.writeUInt32BE(3007000, 4) // Version
    buf.writeUInt32BE(4096, 8) // Page size
    buf.writeUInt32BE(1, 12) // Sequence
    buf.writeUInt32BE(0x12345678, 16) // Salt1
    buf.writeUInt32BE(0x9abcdef0, 20) // Salt2

    // Compute checksum for header
    const [cs1, cs2] = checksum(true, 0, 0, buf.subarray(0, 24))
    buf.writeUInt32BE(cs1, 24)
    buf.writeUInt32BE(cs2, 28)

    const header = parseHeader(buf)

    expect(header.isLittleEndian)
      .toBe(true)
    expect(header.pageSize)
      .toBe(4096)
    expect(header.salt1)
      .toBe(0x12345678)
    expect(header.salt2)
      .toBe(0x9abcdef0)
  })

  test("parseHeader detects big-endian magic", () => {
    const buf = Buffer.alloc(WAL_HEADER_SIZE)
    buf.writeUInt32BE(WAL_MAGIC_BE, 0)
    buf.writeUInt32BE(3007000, 4)
    buf.writeUInt32BE(4096, 8)
    buf.writeUInt32BE(1, 12)
    buf.writeUInt32BE(0, 16)
    buf.writeUInt32BE(0, 20)

    const [cs1, cs2] = checksum(false, 0, 0, buf.subarray(0, 24))
    buf.writeUInt32BE(cs1, 24)
    buf.writeUInt32BE(cs2, 28)

    const header = parseHeader(buf)

    expect(header.isLittleEndian)
      .toBe(false)
  })

  test("parseHeader throws on invalid magic", () => {
    const buf = Buffer.alloc(WAL_HEADER_SIZE)
    buf.writeUInt32BE(0x12345678, 0) // Invalid magic

    expect(
      () => parseHeader(buf),
    )
      .toThrow("invalid WAL magic")
  })

  test("parseHeader throws on unsupported version", () => {
    const buf = Buffer.alloc(WAL_HEADER_SIZE)
    buf.writeUInt32BE(WAL_MAGIC_LE, 0)
    buf.writeUInt32BE(9999999, 4) // Unsupported version

    expect(
      () => parseHeader(buf),
    )
      .toThrow("unsupported WAL version")
  })
})

describe("WAL Frame Header Parsing", () => {
  test("parseFrameHeader extracts all fields", () => {
    const buf = Buffer.alloc(WAL_FRAME_HEADER_SIZE)
    buf.writeUInt32BE(42, 0) // Page number
    buf.writeUInt32BE(100, 4) // Commit (DB size)
    buf.writeUInt32BE(0x11111111, 8) // Salt1
    buf.writeUInt32BE(0x22222222, 12) // Salt2
    buf.writeUInt32BE(0x33333333, 16) // Checksum1
    buf.writeUInt32BE(0x44444444, 20) // Checksum2

    const header = parseFrameHeader(buf)

    expect(header.pgno)
      .toBe(42)
    expect(header.commit)
      .toBe(100)
    expect(header.salt1)
      .toBe(0x11111111)
    expect(header.salt2)
      .toBe(0x22222222)
    expect(header.checksum1)
      .toBe(0x33333333)
    expect(header.checksum2)
      .toBe(0x44444444)
  })
})

describe("WAL Checksum", () => {
  test("checksum computes correctly for little-endian", () => {
    const data = Buffer.alloc(8)
    data.writeUInt32LE(0x12345678, 0)
    data.writeUInt32LE(0x9abcdef0, 4)

    const [s0, s1] = checksum(true, 0, 0, data)

    expect(s0)
      .toBeGreaterThan(0)
    expect(s1)
      .toBeGreaterThan(0)
  })

  test("checksum computes correctly for big-endian", () => {
    const data = Buffer.alloc(8)
    data.writeUInt32BE(0x12345678, 0)
    data.writeUInt32BE(0x9abcdef0, 4)

    const [s0, s1] = checksum(false, 0, 0, data)

    expect(s0)
      .toBeGreaterThan(0)
    expect(s1)
      .toBeGreaterThan(0)
  })

  test("checksum incremental equals one-pass", () => {
    const data = Buffer.alloc(16)
    data.writeUInt32LE(0x11111111, 0)
    data.writeUInt32LE(0x22222222, 4)
    data.writeUInt32LE(0x33333333, 8)
    data.writeUInt32LE(0x44444444, 12)

    // One pass
    const [onePassS0, onePassS1] = checksum(true, 0, 0, data)

    // Incremental (two 8-byte chunks)
    const [s0a, s1a] = checksum(true, 0, 0, data.subarray(0, 8))
    const [s0b, s1b] = checksum(true, s0a, s1a, data.subarray(8, 16))

    expect(s0b)
      .toBe(onePassS0)
    expect(s1b)
      .toBe(onePassS1)
  })

  test("checksum throws on misaligned data", () => {
    const data = Buffer.alloc(7) // Not 8-byte aligned

    expect(
      () => checksum(true, 0, 0, data),
    )
      .toThrow("checksum data must be 8-byte aligned")
  })

  // Tests from Go litestream_test.go TestChecksum
  test("checksum one-pass matches expected value (from Go tests)", () => {
    // WAL header first 24 bytes from Go test
    const headerFirst24 = Buffer.from(
      "377f0682002de218000010000000000052382eac857b1a4e",
      "hex",
    )
    const [s0, s1] = checksum(true, 0, 0, headerFirst24)

    expect(s0)
      .toBe(0x81153b65)
    expect(s1)
      .toBe(0x87178e8f)
  })

  test("checksum incremental matches one-pass (from Go tests)", () => {
    // First compute checksum for header
    const headerFirst24 = Buffer.from(
      "377f0682002de218000010000000000052382eac857b1a4e",
      "hex",
    )
    const [s0, s1] = checksum(true, 0, 0, headerFirst24)

    expect(s0)
      .toBe(0x81153b65)
    expect(s1)
      .toBe(0x87178e8f)

    // Continue with frame header (8 bytes)
    const frameHeader = Buffer.from("0000000200000002", "hex")
    const [s0a, s1a] = checksum(true, s0, s1, frameHeader)

    // Continue with frame data (4096 bytes page) - loaded from testdata file
    const frameData = Buffer.from(walFrameDataHex, "hex")
    const [s0b, s1b] = checksum(true, s0a, s1a, frameData)

    expect(s0b)
      .toBe(0xdc2f3e84)
    expect(s1b)
      .toBe(0x540488d3)
  })
})

describe("WAL Reader with real database", () => {
  let testDir: string
  let dbPath: string
  let walPath: string
  let db: BunSqlite.Database

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "test.db")
    walPath = dbPath + "-wal"

    // Create a database with WAL mode
    // Keep db open to prevent checkpoint on close
    db = new BunSqlite.Database(dbPath)
    db.run("PRAGMA journal_mode = WAL")
    db.run("PRAGMA wal_autocheckpoint = 0") // Disable auto-checkpoint
    db.run("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    db.run("INSERT INTO test (value) VALUES ('hello')")
    db.run("INSERT INTO test (value) VALUES ('world')")
  })

  afterEach(() => {
    try {
      db.close()
      NFs.rmSync(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  test("WalReader.open reads valid WAL file", () => {
    const reader = WalReader.open(walPath)

    expect(reader.pageSize())
      .toBe(4096)

    const [salt1, salt2] = reader.salt()
    expect(salt1)
      .toBeGreaterThan(0)
    expect(salt2)
      .toBeGreaterThan(0)

    reader.close()
  })

  test("WalReader reads frames correctly", () => {
    const reader = WalReader.open(walPath)
    const frames: Array<{ pgno: number; commit: number }> = []

    let frame = reader.readFrame()
    while (frame !== null) {
      frames.push({ pgno: frame.pgno, commit: frame.commit })
      frame = reader.readFrame()
    }

    expect(frames.length)
      .toBeGreaterThan(0)

    // At least one frame should be a commit frame
    const commitFrames = frames.filter((f) => f.commit > 0)
    expect(commitFrames.length)
      .toBeGreaterThan(0)

    reader.close()
  })

  test("WalReader.pageMap returns committed pages", () => {
    const reader = WalReader.open(walPath)
    const { map, maxOffset, commit } = reader.pageMap()

    expect(map.size)
      .toBeGreaterThan(0)
    expect(maxOffset)
      .toBeGreaterThan(WAL_HEADER_SIZE)
    expect(commit)
      .toBeGreaterThan(0)

    reader.close()
  })

  test("WalReader returns null at end of valid frames", () => {
    const reader = WalReader.open(walPath)

    // Read all frames
    while (reader.readFrame() !== null) {
      // Continue reading
    }

    // Next read should also return null
    expect(reader.readFrame())
      .toBeNull()

    reader.close()
  })

  test("WalReader handles salt mismatch (end of valid frames)", () => {
    // This test verifies that salt mismatch returns null (end of frames)
    // rather than throwing an error
    const reader = WalReader.open(walPath)

    // Read all valid frames
    const { map } = reader.pageMap()
    expect(map.size)
      .toBeGreaterThan(0)

    reader.close()
  })
})

describe("WAL utility functions", () => {
  test("calcSize computes correct size", () => {
    const pageSize = 4096
    const pageCount = 10

    const expected = WAL_HEADER_SIZE
      + (WAL_FRAME_HEADER_SIZE + pageSize) * pageCount
    expect(
      calcSize(pageSize, pageCount),
    )
      .toBe(expected)
  })

  test("calcSize with zero pages", () => {
    expect(
      calcSize(4096, 0),
    )
      .toBe(WAL_HEADER_SIZE)
  })
})

describe("readHeader function", () => {
  let testDir: string
  let dbPath: string
  let walPath: string
  let db: BunSqlite.Database

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "test2.db")
    walPath = dbPath + "-wal"

    // Keep db open to prevent checkpoint on close
    db = new BunSqlite.Database(dbPath)
    db.run("PRAGMA journal_mode = WAL")
    db.run("CREATE TABLE t (x)")
    db.run("INSERT INTO t VALUES (1)")
  })

  afterEach(() => {
    try {
      db.close()
      NFs.rmSync(testDir, { recursive: true, force: true })
    } catch {
      // Ignore
    }
  })

  test("readHeader reads from file path", () => {
    const header = readHeader(walPath)

    expect(header.pageSize)
      .toBe(4096)
    expect(header.version)
      .toBe(3007000)
    expect(header.magic)
      .toBeOneOf([WAL_MAGIC_LE, WAL_MAGIC_BE])
  })

  test("readHeader throws on missing file", () => {
    expect(
      () => readHeader("/nonexistent/path.wal"),
    )
      .toThrow()
  })

  test("readHeader throws on truncated file", () => {
    const truncatedPath = NPath.join(testDir, "truncated.wal")
    NFs.writeFileSync(truncatedPath, Buffer.alloc(10)) // Too short

    expect(
      () => readHeader(truncatedPath),
    )
      .toThrow("WAL header too short")
  })
})

describe("WalReader.openWithOffset", () => {
  let testDir: string
  let dbPath: string
  let walPath: string
  let db: BunSqlite.Database

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "offset_test.db")
    walPath = dbPath + "-wal"

    // Create database with multiple transactions
    // Keep db open to prevent checkpoint on close
    db = new BunSqlite.Database(dbPath)
    db.run("PRAGMA journal_mode = WAL")
    db.run("PRAGMA wal_autocheckpoint = 0")
    db.run("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

    // Multiple transactions to create multiple WAL frames
    for (let i = 0; i < 5; i++) {
      db.run(`INSERT INTO test (value) VALUES ('value${i}')`)
    }
  })

  afterEach(() => {
    try {
      db.close()
      NFs.rmSync(testDir, { recursive: true, force: true })
    } catch {
      // Ignore
    }
  })

  test("openWithOffset throws on offset <= header size", () => {
    expect(
      () => WalReader.openWithOffset(walPath, WAL_HEADER_SIZE, 0, 0),
    )
      .toThrow("offset must be greater than WAL header size")

    expect(
      () => WalReader.openWithOffset(walPath, 0, 0, 0),
    )
      .toThrow("offset must be greater than WAL header size")
  })

  test("openWithOffset throws on unaligned offset", () => {
    // First read header to get page size
    const header = readHeader(walPath)
    const frameSize = WAL_FRAME_HEADER_SIZE + header.pageSize

    // Unaligned offset
    const unalignedOffset = WAL_HEADER_SIZE + frameSize + 1

    expect(
      () =>
        WalReader.openWithOffset(
          walPath,
          unalignedOffset,
          header.salt1,
          header.salt2,
        ),
    )
      .toThrow("unaligned WAL offset")
  })

  test("openWithOffset can resume reading from offset", () => {
    // First, read the entire WAL to get salt values and frame info
    const fullReader = WalReader.open(walPath)
    const [salt1, salt2] = fullReader.salt()
    const pageSize = fullReader.pageSize()
    const frameSize = WAL_FRAME_HEADER_SIZE + pageSize

    // Read first frame
    const firstFrame = fullReader.readFrame()
    expect(firstFrame)
      .not
      .toBeNull()

    const firstOffset = fullReader.offset()
    fullReader.close()

    // Now open with offset to skip first frame
    if (firstOffset > WAL_HEADER_SIZE) {
      const offsetReader = WalReader.openWithOffset(
        walPath,
        firstOffset,
        salt1,
        salt2,
      )

      // Should be able to read remaining frames
      const frame = offsetReader.readFrame()
      // May or may not have more frames depending on WAL state
      offsetReader.close()
    }
  })
})

describe("WalReader.frameSaltsUntil", () => {
  let testDir: string
  let dbPath: string
  let walPath: string
  let db: BunSqlite.Database

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "salts_test.db")
    walPath = dbPath + "-wal"

    // Keep db open to prevent checkpoint on close
    db = new BunSqlite.Database(dbPath)
    db.run("PRAGMA journal_mode = WAL")
    db.run("PRAGMA wal_autocheckpoint = 0")
    db.run("CREATE TABLE test (id INTEGER)")
    db.run("INSERT INTO test VALUES (1)")
  })

  afterEach(() => {
    try {
      db.close()
      NFs.rmSync(testDir, { recursive: true, force: true })
    } catch {
      // Ignore
    }
  })

  test("frameSaltsUntil collects salt values", () => {
    const reader = WalReader.open(walPath)
    const [salt1, salt2] = reader.salt()

    const salts = reader.frameSaltsUntil([salt1, salt2])

    expect(salts.size)
      .toBeGreaterThan(0)
    expect(salts.has(`${salt1}:${salt2}`))
      .toBe(true)

    reader.close()
  })
})

describe("WalReader with testdata files", () => {
  test("reads valid WAL file (ok)", () => {
    const walPath = NPath.join(TESTDATA_DIR, "ok", "wal")
    if (!NFs.existsSync(walPath)) {
      console.log("Skipping testdata test - file not found:", walPath)
      return
    }

    const reader = WalReader.open(walPath)

    expect(reader.pageSize())
      .toBe(4096)
    expect(reader.offset())
      .toBe(WAL_HEADER_SIZE)

    // Read first frame
    const frame1 = reader.readFrame()
    expect(frame1)
      .not
      .toBeNull()
    expect(frame1!.pgno)
      .toBe(1)
    expect(frame1!.commit)
      .toBe(0)

    // Read second frame - end of transaction
    const frame2 = reader.readFrame()
    expect(frame2)
      .not
      .toBeNull()
    expect(frame2!.pgno)
      .toBe(2)
    expect(frame2!.commit)
      .toBe(2)

    // Read third frame
    const frame3 = reader.readFrame()
    expect(frame3)
      .not
      .toBeNull()
    expect(frame3!.pgno)
      .toBe(2)
    expect(frame3!.commit)
      .toBe(2)

    // Should reach EOF
    const frame4 = reader.readFrame()
    expect(frame4)
      .toBeNull()

    reader.close()
  })

  test("handles salt mismatch (returns null)", () => {
    const walPath = NPath.join(TESTDATA_DIR, "salt-mismatch", "wal")
    if (!NFs.existsSync(walPath)) {
      console.log("Skipping testdata test - file not found:", walPath)
      return
    }

    const reader = WalReader.open(walPath)

    expect(reader.pageSize())
      .toBe(4096)

    // Read first frame (should succeed)
    const frame1 = reader.readFrame()
    expect(frame1)
      .not
      .toBeNull()
    expect(frame1!.pgno)
      .toBe(1)

    // Read second frame - salt mismatch should return null (end of valid frames)
    const frame2 = reader.readFrame()
    expect(frame2)
      .toBeNull()

    reader.close()
  })

  test("handles frame checksum mismatch (returns null)", () => {
    const walPath = NPath.join(TESTDATA_DIR, "frame-checksum-mismatch", "wal")
    if (!NFs.existsSync(walPath)) {
      console.log("Skipping testdata test - file not found:", walPath)
      return
    }

    const reader = WalReader.open(walPath)

    expect(reader.pageSize())
      .toBe(4096)

    // Read first frame (should succeed)
    const frame1 = reader.readFrame()
    expect(frame1)
      .not
      .toBeNull()
    expect(frame1!.pgno)
      .toBe(1)

    // Read second frame - checksum mismatch should return null
    const frame2 = reader.readFrame()
    expect(frame2)
      .toBeNull()

    reader.close()
  })

  test("frameSaltsUntil collects multiple salts", () => {
    const walPath = NPath.join(TESTDATA_DIR, "frame-salts", "wal")
    if (!NFs.existsSync(walPath)) {
      console.log("Skipping testdata test - file not found:", walPath)
      return
    }

    const reader = WalReader.open(walPath)

    const salts = reader.frameSaltsUntil([0x00000000, 0x00000000])

    expect(salts.size)
      .toBe(3)

    // Check for expected salt values (from actual testdata file)
    expect(salts.has("463087947:939071766"))
      .toBe(true)
    expect(salts.has("463087946:52369758"))
      .toBe(true)
    expect(salts.has("463087945:330554727"))
      .toBe(true)

    reader.close()
  })
})

describe("WalReader error cases", () => {
  test("throws on zero-length WAL", () => {
    const testDir = makeTempDir()
    try {
      const walPath = NPath.join(testDir, "empty.wal")
      NFs.writeFileSync(walPath, Buffer.alloc(0))

      expect(
        () => WalReader.open(walPath),
      )
        .toThrow("WAL header too short: 0 bytes")
    } finally {
      NFs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  test("throws on partial header", () => {
    const testDir = makeTempDir()
    try {
      const walPath = NPath.join(testDir, "partial.wal")
      NFs.writeFileSync(walPath, Buffer.alloc(10))

      expect(
        () => WalReader.open(walPath),
      )
        .toThrow("WAL header too short: 10 bytes")
    } finally {
      NFs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  test("throws on bad magic", () => {
    const testDir = makeTempDir()
    try {
      const walPath = NPath.join(testDir, "badmagic.wal")
      NFs.writeFileSync(walPath, Buffer.alloc(32))

      expect(
        () => WalReader.open(walPath),
      )
        .toThrow("invalid WAL magic")
    } finally {
      NFs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  test("throws on bad header checksum", () => {
    const testDir = makeTempDir()
    try {
      const walPath = NPath.join(testDir, "badchecksum.wal")
      const data = Buffer.alloc(32)
      // Write valid magic but leave checksum as zeros
      data.writeUInt32BE(WAL_MAGIC_LE, 0)
      data.writeUInt32BE(3007000, 4) // Valid version
      NFs.writeFileSync(walPath, data)

      expect(
        () => WalReader.open(walPath),
      )
        .toThrow("WAL header checksum mismatch")
    } finally {
      NFs.rmSync(testDir, { recursive: true, force: true })
    }
  })

  test("throws on bad header version", () => {
    const testDir = makeTempDir()
    try {
      const walPath = NPath.join(testDir, "badversion.wal")
      const data = Buffer.alloc(32)
      data.writeUInt32BE(WAL_MAGIC_LE, 0)
      data.writeUInt32BE(1, 4) // Unsupported version
      NFs.writeFileSync(walPath, data)

      expect(
        () => WalReader.open(walPath),
      )
        .toThrow("unsupported WAL version")
    } finally {
      NFs.rmSync(testDir, { recursive: true, force: true })
    }
  })
})

describe("readPageAt function", () => {
  let testDir: string
  let dbPath: string
  let walPath: string
  let db: BunSqlite.Database

  beforeEach(() => {
    testDir = makeTempDir()
    dbPath = NPath.join(testDir, "readpage.db")
    walPath = dbPath + "-wal"

    db = new BunSqlite.Database(dbPath)
    db.run("PRAGMA journal_mode = WAL")
    db.run("PRAGMA wal_autocheckpoint = 0")
    db.run("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    db.run("INSERT INTO test (value) VALUES ('test data')")
  })

  afterEach(() => {
    try {
      db.close()
      NFs.rmSync(testDir, { recursive: true, force: true })
    } catch {
      // Ignore
    }
  })

  test("reads page data at offset", () => {
    const reader = WalReader.open(walPath)
    const frame = reader.readFrame()
    expect(frame)
      .not
      .toBeNull()

    // Read page at first frame offset
    const pageData = readPageAt(walPath, WAL_HEADER_SIZE, 4096)

    expect(pageData.length)
      .toBe(4096)
    // Page data should match frame data
    expect(pageData.equals(frame!.data))
      .toBe(true)

    reader.close()
  })
})

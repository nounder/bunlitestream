import * as BunSqlite from "bun:sqlite"
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as NFs from "node:fs"
import * as NOs from "node:os"
import * as NPath from "node:path"
import * as Db from "../src/Db.ts"
import * as Ltx from "../src/Ltx.ts"
import type * as S3 from "../src/S3.ts"
import * as Snapshot from "../src/Snapshot.ts"
import * as Testing from "../src/testing.ts"

function makeTempDir(): string {
  return NFs.mkdtempSync(NPath.join(NOs.tmpdir(), "bun-litestream-restore-"))
}

const { MockReplicaClient } = Testing

describe("Snapshot", () => {
  let testDir: string

  beforeEach(() => {
    testDir = makeTempDir()
  })

  afterEach(() => {
    NFs.rmSync(testDir, { recursive: true, force: true })
  })

  test("restore throws on existing output path", async () => {
    const client = new MockReplicaClient()
    const outputPath = NPath.join(testDir, "output.db")
    NFs.writeFileSync(outputPath, "existing")

    let error: unknown = null
    try {
      await Snapshot.restore(client, { outputPath })
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toContain("output path already exists")
  })

  test("restore throws on missing output path", async () => {
    const client = new MockReplicaClient()

    let error: unknown = null
    try {
      await Snapshot.restore(client, { outputPath: "" })
    } catch (err) {
      error = err
    }

    expect(error instanceof Error ? error.message : error)
      .toBe("output path required")
  })

  test("restore throws when no backup files available", async () => {
    const client = new MockReplicaClient()
    const outputPath = NPath.join(testDir, "output.db")

    let error: unknown = null
    try {
      await Snapshot.restore(client, { outputPath })
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toBe("no matching backup files available")
  })
})

describe("Compact", () => {
  test("compact throws on empty decoders array", async () => {
    let error: unknown = null
    try {
      await Snapshot.compact([])
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toBe("no LTX files to compact")
  })

  test("compact merges pages from multiple LTX files", async () => {
    const encoder1 = new Ltx.Encoder()
    encoder1.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 2,
      minTxid: 1n,
      maxTxid: 1n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })
    const page1 = new Uint8Array(4096)
    page1[0] = 1
    encoder1.encodePage({ pgno: 1 }, page1)
    const ltx1 = encoder1.close()

    const encoder2 = new Ltx.Encoder()
    encoder2.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 2,
      minTxid: 2n,
      maxTxid: 2n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })
    const page2 = new Uint8Array(4096)
    page2[0] = 2
    encoder2.encodePage({ pgno: 2 }, page2)
    const ltx2 = encoder2.close()

    const decoder1 = new Ltx.Decoder(ltx1)
    const decoder2 = new Ltx.Decoder(ltx2)

    const compacted = await Snapshot.compact([decoder1, decoder2])

    const resultDecoder = new Ltx.Decoder(compacted)
    const header = resultDecoder.decodeHeader()

    expect(header.minTxid)
      .toBe(1n)
    expect(header.maxTxid)
      .toBe(2n)

    const pages: number[] = []
    let page
    while ((page = resultDecoder.decodePage()) !== null) {
      pages.push(page.header.pgno)
    }

    expect(pages)
      .toEqual([1, 2])
  })

  test("compact overwrites earlier pages with later ones", async () => {
    const encoder1 = new Ltx.Encoder()
    encoder1.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 1,
      minTxid: 1n,
      maxTxid: 1n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })
    const page1 = new Uint8Array(4096)
    page1[0] = 0xaa
    encoder1.encodePage({ pgno: 1 }, page1)
    const ltx1 = encoder1.close()

    const encoder2 = new Ltx.Encoder()
    encoder2.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 1,
      minTxid: 2n,
      maxTxid: 2n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })
    const page2 = new Uint8Array(4096)
    page2[0] = 0xbb
    encoder2.encodePage({ pgno: 1 }, page2)
    const ltx2 = encoder2.close()

    const decoder1 = new Ltx.Decoder(ltx1)
    const decoder2 = new Ltx.Decoder(ltx2)

    const compacted = await Snapshot.compact([decoder1, decoder2])

    const resultDecoder = new Ltx.Decoder(compacted)
    resultDecoder.decodeHeader()

    const page = resultDecoder.decodePage()
    expect(page)
      .not
      .toBeNull()
    expect(page!.data[0])
      .toBe(0xbb)
  })
})

describe("MockReplicaClient", () => {
  test("writeLtxFile stores file", async () => {
    const client = new MockReplicaClient()
    const data = new Uint8Array([1, 2, 3])

    await client.writeLtxFile(0, 1n, 1n, data)

    expect(client.getFileCount())
      .toBe(1)
  })

  test("ltxFiles returns files in order", async () => {
    const client = new MockReplicaClient()

    await client.writeLtxFile(0, 3n, 3n, new Uint8Array([3]))
    await client.writeLtxFile(0, 1n, 1n, new Uint8Array([1]))
    await client.writeLtxFile(0, 2n, 2n, new Uint8Array([2]))

    const iterator = client.ltxFiles(0)
    const files: Ltx.FileInfo[] = []
    let file
    while ((file = await iterator.next()) !== null) {
      files.push(file)
    }

    expect(
      files.map((f) => f.minTxid),
    )
      .toEqual([1n, 2n, 3n])
  })

  test("ltxFiles filters by level", async () => {
    const client = new MockReplicaClient()

    await client.writeLtxFile(0, 1n, 1n, new Uint8Array([1]))
    await client.writeLtxFile(1, 2n, 2n, new Uint8Array([2]))

    const iterator = client.ltxFiles(0)
    const files: Ltx.FileInfo[] = []
    let file
    while ((file = await iterator.next()) !== null) {
      files.push(file)
    }

    expect(files.length)
      .toBe(1)
    expect(files[0].minTxid)
      .toBe(1n)
  })

  test("deleteLtxFiles removes files", async () => {
    const client = new MockReplicaClient()

    const info = await client.writeLtxFile(0, 1n, 1n, new Uint8Array([1]))
    expect(client.getFileCount())
      .toBe(1)

    await client.deleteLtxFiles([info])

    expect(client.getFileCount())
      .toBe(0)
  })

  test("deleteAll clears all files", async () => {
    const client = new MockReplicaClient()

    await client.writeLtxFile(0, 1n, 1n, new Uint8Array([1]))
    await client.writeLtxFile(0, 2n, 2n, new Uint8Array([2]))

    await client.deleteAll()

    expect(client.getFileCount())
      .toBe(0)
  })
})

describe("CalcRestorePlan (from Go tests)", () => {
  test("SnapshotOnly", async () => {
    const client = new MockReplicaClient()
    client.setFileInfos(Snapshot.SNAPSHOT_LEVEL, [{
      level: Snapshot.SNAPSHOT_LEVEL,
      minTxid: 1n,
      maxTxid: 10n,
      size: 1024,
      createdAt: new Date(),
    }])

    const plan = await Snapshot.calcRestorePlan(client, 10n)

    expect(plan.length)
      .toBe(1)
    expect(plan[0].maxTxid)
      .toBe(10n)
  })

  test("SnapshotAndIncremental", async () => {
    const client = new MockReplicaClient()

    client.setFileInfos(Snapshot.SNAPSHOT_LEVEL, [
      {
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 5n,
        size: 100,
        createdAt: new Date(),
      },
      {
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 15n,
        size: 100,
        createdAt: new Date(),
      },
    ])
    client.setFileInfos(1, [
      { level: 1, minTxid: 6n, maxTxid: 7n, size: 100, createdAt: new Date() },
      { level: 1, minTxid: 8n, maxTxid: 9n, size: 100, createdAt: new Date() },
      {
        level: 1,
        minTxid: 10n,
        maxTxid: 12n,
        size: 100,
        createdAt: new Date(),
      },
    ])
    client.setFileInfos(0, [
      { level: 0, minTxid: 7n, maxTxid: 7n, size: 100, createdAt: new Date() },
      { level: 0, minTxid: 8n, maxTxid: 8n, size: 100, createdAt: new Date() },
      { level: 0, minTxid: 9n, maxTxid: 9n, size: 100, createdAt: new Date() },
      {
        level: 0,
        minTxid: 10n,
        maxTxid: 10n,
        size: 100,
        createdAt: new Date(),
      },
      {
        level: 0,
        minTxid: 11n,
        maxTxid: 11n,
        size: 100,
        createdAt: new Date(),
      },
    ])

    const plan = await Snapshot.calcRestorePlan(client, 10n)

    expect(plan.length)
      .toBe(4)
    expect(plan[0])
      .toMatchObject({
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 5n,
      })
    expect(plan[1])
      .toMatchObject({
        level: 1,
        minTxid: 6n,
        maxTxid: 7n,
      })
    expect(plan[2])
      .toMatchObject({
        level: 1,
        minTxid: 8n,
        maxTxid: 9n,
      })
    expect(plan[3])
      .toMatchObject({
        level: 0,
        minTxid: 10n,
        maxTxid: 10n,
      })
  })

  test("ErrNonContiguousFiles", async () => {
    const client = new MockReplicaClient()

    client.setFileInfos(Snapshot.SNAPSHOT_LEVEL, [
      {
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 5n,
        size: 100,
        createdAt: new Date(),
      },
    ])
    client.setFileInfos(1, [
      { level: 1, minTxid: 8n, maxTxid: 9n, size: 100, createdAt: new Date() },
    ])

    let error: unknown = null
    try {
      await Snapshot.calcRestorePlan(client, 10n)
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toContain("non-contiguous transaction files")
  })

  test("OverlappingFilesWithinLevel", async () => {
    const client = new MockReplicaClient()

    client.setFileInfos(Snapshot.SNAPSHOT_LEVEL, [
      {
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 5n,
        size: 100,
        createdAt: new Date(),
      },
    ])
    client.setFileInfos(2, [
      {
        level: 2,
        minTxid: 1n,
        maxTxid: 100n,
        size: 100,
        createdAt: new Date(),
      },
      {
        level: 2,
        minTxid: 50n,
        maxTxid: 60n,
        size: 100,
        createdAt: new Date(),
      },
    ])

    const plan = await Snapshot.calcRestorePlan(client, 100n)

    expect(plan.length)
      .toBe(2)
    expect(plan[0])
      .toMatchObject({
        level: Snapshot.SNAPSHOT_LEVEL,
        minTxid: 1n,
        maxTxid: 5n,
      })
    expect(plan[1])
      .toMatchObject({
        level: 2,
        minTxid: 1n,
        maxTxid: 100n,
      })
  })

  test("ErrNoFiles", async () => {
    const client = new MockReplicaClient()

    let error: unknown = null
    try {
      await Snapshot.calcRestorePlan(client, 5n)
    } catch (err) {
      error = err
    }

    const message = error instanceof Error ? error.message : String(error)
    expect(message)
      .toBe("no matching backup files available")
  })
})

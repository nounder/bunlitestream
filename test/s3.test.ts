import * as BunSqlite from "bun:sqlite"
import * as test from "bun:test"
import * as Ltx from "../src/Ltx.ts"
import * as S3 from "../src/S3.ts"

test.describe("S3ReplicaClient URL Parsing", () => {
  test.test("make parses basic s3:// URL", () => {
    const client = S3.make("s3://mybucket/mypath")

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("make throws on non-s3 protocol", () => {
    test
      .expect(
        () => S3.make("http://mybucket/mypath"),
      )
      .toThrow("Invalid S3 URL protocol")
  })

  test.test("make parses URL with region", () => {
    const client = S3.make(
      "s3://mybucket/mypath?region=us-west-2",
    )

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("make parses URL with endpoint", () => {
    const client = S3.make(
      "s3://mybucket/mypath?endpoint=https://s3.example.com",
    )

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("make parses URL with credentials", () => {
    const client = S3.make(
      "s3://mybucket/mypath?accessKeyId=AKID&secretAccessKey=SECRET",
    )

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("make handles empty path", () => {
    const client = S3.make("s3://mybucket")

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("make handles path with leading slash", () => {
    const client = S3.make("s3://mybucket/path/to/backup")

    test
      .expect(client.type())
      .toBe("s3")
  })
})

test.describe("S3ReplicaClient Configuration", () => {
  test.test("S3ReplicaClient constructor accepts config", () => {
    const client = new S3.S3ReplicaClient({
      bucket: "test-bucket",
      path: "backups",
      region: "us-east-1",
    })

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("S3ReplicaClient constructor accepts minimal config", () => {
    const client = new S3.S3ReplicaClient({
      bucket: "test-bucket",
    })

    test
      .expect(client.type())
      .toBe("s3")
  })

  test.test("S3ReplicaClient constructor accepts full config", () => {
    const client = new S3.S3ReplicaClient({
      bucket: "test-bucket",
      path: "backups/db",
      region: "eu-west-1",
      endpoint: "https://s3.custom.endpoint",
      accessKeyId: "AKIAIOSFODNN7EXAMPLE",
      secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    })

    test
      .expect(client.type())
      .toBe("s3")
  })
})

test.test("S3ReplicaClient matches BunSqlite.Database interface", () => {
  test
    .expectTypeOf<S3.S3ReplicaClient>()
    .toMatchTypeOf<BunSqlite.Database>()
})

// Integration tests - only run when BUCKET_NAME env var is set
const S3_TEST_BUCKET = process.env.BUCKET_NAME
const S3_TEST_ENDPOINT = process.env.AWS_ENDPOINT_URL_S3
const S3_TEST_REGION = process.env.AWS_REGION || "us-east-1"
const S3_ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID
const S3_SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY

test.describe.skipIf(!S3_TEST_BUCKET)("S3ReplicaClient Integration", () => {
  let client: S3.S3ReplicaClient
  const testPath = `test-${Date.now()}`

  test.afterEach(async () => {
    if (client) {
      await client.deleteAll()
    }
  })

  test.test("init connects to S3", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })

    await client.init()
  })

  test.test("writeLtxFile uploads file to S3", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
    await client.init()

    const encoder = new Ltx.Encoder()
    encoder.encodeHeader({
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
    const page = new Uint8Array(4096)
    page[0] = 0x42
    encoder.encodePage({ pgno: 1 }, page)
    const ltxData = encoder.close()

    const info = await client.writeLtxFile(0, 1n, 1n, ltxData)

    test
      .expect(info.level)
      .toBe(0)
    test
      .expect(info.minTxid)
      .toBe(1n)
    test
      .expect(info.maxTxid)
      .toBe(1n)
    test
      .expect(info.size)
      .toBe(ltxData.length)
  })

  test.test("openLtxFile reads file from S3", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
    await client.init()

    // Write a file first
    const encoder = new Ltx.Encoder()
    encoder.encodeHeader({
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
    const page = new Uint8Array(4096)
    page[0] = 0xaa
    page[1] = 0xbb
    encoder.encodePage({ pgno: 1 }, page)
    const ltxData = encoder.close()

    await client.writeLtxFile(0, 2n, 2n, ltxData)

    // Read it back
    const stream = await client.openLtxFile(0, 2n, 2n)
    const reader = stream.getReader()
    const chunks: Uint8Array[] = []

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(value)
    }

    const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
    const downloaded = new Uint8Array(totalLength)
    let offset = 0
    for (const chunk of chunks) {
      downloaded.set(chunk, offset)
      offset += chunk.length
    }

    test
      .expect(downloaded.length)
      .toBe(ltxData.length)

    // Verify content
    const decoder = new Ltx.Decoder(downloaded)
    const header = decoder.decodeHeader()
    test
      .expect(header.minTxid)
      .toBe(2n)

    const decodedPage = decoder.decodePage()
    test
      .expect(decodedPage)
      .not
      .toBeNull()
    test
      .expect(decodedPage!.data[0])
      .toBe(0xaa)
    test
      .expect(decodedPage!.data[1])
      .toBe(0xbb)
  })

  test.test("ltxFiles lists files from S3", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
    await client.init()

    // Write multiple files
    for (let i = 1n; i <= 3n; i++) {
      const encoder = new Ltx.Encoder()
      encoder.encodeHeader({
        version: 1,
        flags: 0,
        pageSize: 4096,
        commit: 1,
        minTxid: i,
        maxTxid: i,
        timestamp: BigInt(Date.now()),
        preApplyChecksum: 0n,
        walOffset: 0n,
        walSize: 0n,
        walSalt1: 0,
        walSalt2: 0,
        nodeID: 0n,
      })
      encoder.encodePage({ pgno: 1 }, new Uint8Array(4096))
      await client.writeLtxFile(0, i, i, encoder.close())
    }

    // List files
    const iterator = client.ltxFiles(0)
    const files = []
    let file
    while ((file = await iterator.next()) !== null) {
      files.push(file)
    }

    test
      .expect(files.length)
      .toBe(3)
    test
      .expect(
        files.map((f) => f.minTxid),
      )
      .toEqual([1n, 2n, 3n])
  })

  test.test("deleteLtxFiles removes files from S3", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
    await client.init()

    // Write a file
    const encoder = new Ltx.Encoder()
    encoder.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 1,
      minTxid: 10n,
      maxTxid: 10n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })
    encoder.encodePage({ pgno: 1 }, new Uint8Array(4096))
    const info = await client.writeLtxFile(0, 10n, 10n, encoder.close())

    // Verify it exists
    const iterator1 = client.ltxFiles(0)
    const beforeFiles = []
    let f
    while ((f = await iterator1.next()) !== null) {
      beforeFiles.push(f)
    }
    test
      .expect(beforeFiles.length)
      .toBeGreaterThan(0)

    // Delete it
    await client.deleteLtxFiles([info])

    // Verify it's gone
    const iterator2 = client.ltxFiles(0)
    const afterFiles = []
    while ((f = await iterator2.next()) !== null) {
      afterFiles.push(f)
    }
    test
      .expect(
        afterFiles.find((file) => file.minTxid === 10n),
      )
      .toBeUndefined()
  })

  test.test("round-trip write and read stream", async () => {
    client = new S3.S3ReplicaClient({
      bucket: S3_TEST_BUCKET!,
      path: testPath,
      region: S3_TEST_REGION,
      endpoint: S3_TEST_ENDPOINT,
      accessKeyId: S3_ACCESS_KEY,
      secretAccessKey: S3_SECRET_KEY,
    })
    await client.init()

    // Create a larger LTX file with multiple pages
    const encoder = new Ltx.Encoder()
    encoder.encodeHeader({
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 10,
      minTxid: 100n,
      maxTxid: 100n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 0n,
      walSize: 0n,
      walSalt1: 12345,
      walSalt2: 67890,
      nodeID: 0n,
    })

    for (let i = 1; i <= 10; i++) {
      const page = new Uint8Array(4096)
      page[0] = i
      page[4095] = i
      encoder.encodePage({ pgno: i }, page)
    }
    const ltxData = encoder.close()

    // Write using stream
    const writeStream = new ReadableStream({
      start(controller) {
        controller.enqueue(ltxData)
        controller.close()
      },
    })
    await client.writeLtxFile(0, 100n, 100n, writeStream)

    // Read back
    const readStream = await client.openLtxFile(0, 100n, 100n)
    const reader = readStream.getReader()
    const chunks: Uint8Array[] = []

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(value)
    }

    const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
    const downloaded = new Uint8Array(totalLength)
    let offset = 0
    for (const chunk of chunks) {
      downloaded.set(chunk, offset)
      offset += chunk.length
    }

    // Verify
    const decoder = new Ltx.Decoder(downloaded)
    const header = decoder.decodeHeader()

    test
      .expect(header.minTxid)
      .toBe(100n)
    test
      .expect(header.commit)
      .toBe(10)
    test
      .expect(header.walSalt1)
      .toBe(12345)
    test
      .expect(header.walSalt2)
      .toBe(67890)

    let pageCount = 0
    let page
    while ((page = decoder.decodePage()) !== null) {
      pageCount++
      test
        .expect(page.data[0])
        .toBe(page.header.pgno)
      test
        .expect(page.data[4095])
        .toBe(page.header.pgno)
    }

    test
      .expect(pageCount)
      .toBe(10)
  })
})

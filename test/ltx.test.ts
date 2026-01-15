import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import * as Ltx from "../src/Ltx.ts"

describe("LTX TXID functions", () => {
  test("formatTXID formats correctly", () => {
    expect(Ltx.formatTxid(1n))
      .toBe("0000000000000001")

    expect(Ltx.formatTxid(255n))
      .toBe("00000000000000ff")

    expect(Ltx.formatTxid(0x123456789abcdef0n))
      .toBe("123456789abcdef0")
  })

  test("parseTXID parses correctly", () => {
    expect(Ltx.parseTxid("0000000000000001"))
      .toBe(1n)

    expect(Ltx.parseTxid("00000000000000ff"))
      .toBe(255n)

    expect(Ltx.parseTxid("123456789abcdef0"))
      .toBe(0x123456789abcdef0n)
  })

  test("formatTXID and parseTXID are inverse operations", () => {
    const values = [0n, 1n, 100n, 0xffffffffffffffffn]
    for (const v of values) {
      expect(Ltx.parseTxid(Ltx.formatTxid(v)))
        .toBe(v)
    }
  })
})

describe("LTX filename functions", () => {
  test("formatFilename creates correct format", () => {
    expect(
      Ltx.formatFilename(1n, 10n),
    )
      .toBe("0000000000000001-000000000000000a.ltx")
  })

  test("parseFilename extracts TXIDs", () => {
    const result = Ltx.parseFilename("0000000000000001-000000000000000a.ltx")
    expect(result)
      .toEqual({
        minTxid: 1n,
        maxTxid: 10n,
      })
  })

  test("parseFilename throws on invalid filename", () => {
    expect(() => Ltx.parseFilename("invalid.ltx")).toThrow(
      "invalid LTX filename",
    )
    expect(() => Ltx.parseFilename("not-an-ltx-file")).toThrow(
      "invalid LTX filename",
    )
  })
})

describe("isContiguous", () => {
  test("first file is contiguous if minTxid is 1", () => {
    expect(
      Ltx.isContiguous(0n, 1n, 5n),
    )
      .toBe(true)
  })

  test("adjacent ranges are contiguous", () => {
    expect(
      Ltx.isContiguous(5n, 6n, 10n),
    )
      .toBe(true)
  })

  test("overlapping ranges are contiguous", () => {
    expect(
      Ltx.isContiguous(5n, 3n, 10n),
    )
      .toBe(true)
  })

  test("gap is not contiguous", () => {
    expect(
      Ltx.isContiguous(5n, 8n, 10n),
    )
      .toBe(false)
  })
})

describe("lockPgno", () => {
  test("calculates lock page correctly for 4KB pages", () => {
    expect(Ltx.lockPgno(4096))
      .toBe(262145)
  })

  test("calculates lock page correctly for 1KB pages", () => {
    expect(Ltx.lockPgno(1024))
      .toBe(1048577)
  })
})

describe("CRC-64", () => {
  test("crc64 returns consistent results", () => {
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
    const result1 = Ltx.crc64(data)
    const result2 = Ltx.crc64(data)
    expect(result1)
      .toBe(result2)
  })

  test("crc64 produces different results for different data", () => {
    const data1 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])
    const data2 = new Uint8Array([8, 7, 6, 5, 4, 3, 2, 1])
    expect(Ltx.crc64(data1))
      .not
      .toBe(Ltx.crc64(data2))
  })

  test("checksumPage includes page number", () => {
    const data = new Uint8Array(4096).fill(0)
    const cs1 = Ltx.checksumPage(1, data)
    const cs2 = Ltx.checksumPage(2, data)
    expect(cs1)
      .not
      .toBe(cs2)
  })
})

describe("HeaderCodec", () => {
  test("encode produces correct size", () => {
    const header: Ltx.Header = {
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 100,
      minTxid: 1n,
      maxTxid: 10n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 32n,
      walSize: 1000n,
      walSalt1: 12345,
      walSalt2: 67890,
      nodeID: 0n,
    }

    const encoded = Ltx.HeaderCodec.encode(header)
    expect(encoded.length)
      .toBe(Ltx.HEADER_SIZE)
  })

  test("encode and decode are inverse operations", () => {
    const header: Ltx.Header = {
      version: 1,
      flags: 0,
      pageSize: 4096,
      commit: 100,
      minTxid: 1n,
      maxTxid: 10n,
      timestamp: 1234567890000n,
      preApplyChecksum: 0xabcdef0123456789n,
      walOffset: 32n,
      walSize: 1000n,
      walSalt1: 12345,
      walSalt2: 67890,
      nodeID: 999n,
    }

    const encoded = Ltx.HeaderCodec.encode(header)
    const decoded = Ltx.HeaderCodec.decode(encoded)

    expect(decoded)
      .toEqual({
        ...header,
        version: 1, // Always set to LTX_VERSION
      })
  })

  test("decode throws on invalid magic", () => {
    const data = new Uint8Array(Ltx.HEADER_SIZE)
    expect(() => Ltx.HeaderCodec.decode(data)).toThrow("invalid LTX magic")
  })
})

describe("TrailerCodec", () => {
  test("encode produces correct size", () => {
    const trailer: Ltx.Trailer = {
      postApplyChecksum: 0xabcdef0123456789n,
      fileChecksum: 0x123456789abcdef0n,
    }

    const encoded = Ltx.TrailerCodec.encode(trailer)
    expect(encoded.length)
      .toBe(Ltx.TRAILER_SIZE)
  })

  test("encode and decode are inverse operations", () => {
    const trailer: Ltx.Trailer = {
      postApplyChecksum: 0xabcdef0123456789n,
      fileChecksum: 0x123456789abcdef0n,
    }

    const encoded = Ltx.TrailerCodec.encode(trailer)
    const decoded = Ltx.TrailerCodec.decode(encoded)

    expect(decoded)
      .toEqual(trailer)
  })
})

describe("Encoder and Decoder", () => {
  test("encode and decode round-trip", () => {
    const encoder = new Ltx.Encoder()

    encoder.encodeHeader({
      version: 1,
      flags: Ltx.HEADER_FLAG_NO_CHECKSUM, // Skip checksum for test
      pageSize: 4096,
      commit: 3,
      minTxid: 1n,
      maxTxid: 1n,
      timestamp: BigInt(Date.now()),
      preApplyChecksum: 0n,
      walOffset: 32n,
      walSize: 0n,
      walSalt1: 0,
      walSalt2: 0,
      nodeID: 0n,
    })

    // Add some pages
    const page1 = new Uint8Array(4096).fill(1)
    const page2 = new Uint8Array(4096).fill(2)
    const page3 = new Uint8Array(4096).fill(3)

    encoder.encodePage({ pgno: 1 }, page1)
    encoder.encodePage({ pgno: 2 }, page2)
    encoder.encodePage({ pgno: 3 }, page3)

    const encoded = encoder.close()

    // Decode
    const decoder = new Ltx.Decoder(encoded)
    const header = decoder.decodeHeader()

    expect(header.pageSize)
      .toBe(4096)
    expect(header.commit)
      .toBe(3)
    expect(header.minTxid)
      .toBe(1n)

    const pages = decoder.decodeAllPages()
    expect(pages.size)
      .toBe(3)

    expect(pages.get(1))
      .toEqual(page1)
    expect(pages.get(2))
      .toEqual(page2)
    expect(pages.get(3))
      .toEqual(page3)
  })

  test("encoder enforces page order", () => {
    const encoder = new Ltx.Encoder()

    encoder.encodeHeader({
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

    encoder.encodePage({ pgno: 2 }, new Uint8Array(4096))

    // Should throw when trying to write page 1 after page 2
    expect(
      () => encoder.encodePage({ pgno: 1 }, new Uint8Array(4096)),
    )
      .toThrow("pages must be in ascending order")
  })

  test("decodeDatabase reconstructs database", () => {
    const encoder = new Ltx.Encoder()

    encoder.encodeHeader({
      version: 1,
      flags: Ltx.HEADER_FLAG_NO_CHECKSUM,
      pageSize: 4096,
      commit: 3,
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
    page1.fill(0x11)
    const page2 = new Uint8Array(4096)
    page2.fill(0x22)
    const page3 = new Uint8Array(4096)
    page3.fill(0x33)

    encoder.encodePage({ pgno: 1 }, page1)
    encoder.encodePage({ pgno: 2 }, page2)
    encoder.encodePage({ pgno: 3 }, page3)

    const encoded = encoder.close()

    const decoder = new Ltx.Decoder(encoded)
    const db = decoder.decodeDatabase()

    expect(db.length)
      .toBe(4096 * 3)

    // Check page 1
    expect(
      db.slice(0, 4096),
    )
      .toEqual(page1)

    // Check page 2
    expect(
      db.slice(4096, 8192),
    )
      .toEqual(page2)

    // Check page 3
    expect(
      db.slice(8192, 12288),
    )
      .toEqual(page3)
  })
})

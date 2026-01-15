/**
 * LTX (Lite Transaction) file format implementation
 *
 * LTX files store SQLite database changes in a compact, streamable format.
 * Each file contains:
 * - Header (100 bytes): metadata about the transaction
 * - Page Block: series of page frames (4-byte pgno + page data)
 * - Trailer (16 bytes): checksums for verification
 */

export const LTX_MAGIC = 0x4c545831
export const LTX_VERSION = 1

export const HEADER_SIZE = 100
export const TRAILER_SIZE = 16
export const PAGE_HEADER_SIZE = 4

export const HEADER_FLAG_COMPRESS = 0x00000001
export const HEADER_FLAG_NO_CHECKSUM = 0x00000002

export type TXID = bigint // 64-bit unsigned

export type Checksum = bigint // CRC-64 ISO

/**
 * LTX Header structure (100 bytes)
 */
export interface Header {
  version: number
  flags: number
  pageSize: number
  commit: number // DB pages after transaction
  minTxid: TXID
  maxTxid: TXID
  timestamp: bigint
  preApplyChecksum: Checksum
  walOffset: bigint
  walSize: bigint
  walSalt1: number
  walSalt2: number
  nodeID: bigint
}

export interface Trailer {
  postApplyChecksum: Checksum
  fileChecksum: Checksum
}

export interface PageHeader {
  pgno: number
}

export interface FileInfo {
  level: number
  minTxid: TXID
  maxTxid: TXID
  size: number
  createdAt: Date
}

/**
 * Replication position
 */
export interface Pos {
  txid: TXID
  postApplyChecksum: Checksum
}

/**
 * Format TXID as zero-padded hex string (16 chars)
 */
export function formatTxid(txid: TXID): string {
  return txid.toString(16).padStart(16, "0")
}

/**
 * Parse TXID from hex string
 */
export function parseTxid(s: string): TXID {
  return BigInt("0x" + s)
}

export function formatFilename(minTxid: TXID, maxTxid: TXID): string {
  return `${formatTxid(minTxid)}-${formatTxid(maxTxid)}.ltx`
}

/**
 * Parse LTX filename to extract min/max TXID
 */
export function parseFilename(
  filename: string,
): { minTxid: TXID; maxTxid: TXID } {
  const match = filename.match(/^([0-9a-f]{16})-([0-9a-f]{16})\.ltx$/i)
  if (!match) {
    throw new Error(`invalid LTX filename: ${filename}`)
  }
  return {
    minTxid: parseTxid(match[1]),
    maxTxid: parseTxid(match[2]),
  }
}

export function isContiguous(
  prevMaxTXID: TXID,
  minTxid: TXID,
): boolean {
  if (prevMaxTXID === 0n) {
    return minTxid === 1n
  }
  return minTxid === prevMaxTXID + 1n || minTxid <= prevMaxTXID
}

/**
 * Calculate lock page number for a given page size
 * SQLite reserves this page for locking
 */
export function lockPgno(pageSize: number): number {
  return Math.floor(1073741824 / pageSize) + 1
}

// CRC-64 ISO polynomial
const CRC64_ISO_POLY = 0xd800000000000000n

// Pre-computed CRC-64 ISO table
let crc64Table: BigUint64Array | null = null

function getCRC64Table(): BigUint64Array {
  if (crc64Table) return crc64Table

  crc64Table = new BigUint64Array(256)
  for (let i = 0; i < 256; i++) {
    let crc = BigInt(i)
    for (let j = 0; j < 8; j++) {
      if (crc & 1n) {
        crc = (crc >> 1n) ^ CRC64_ISO_POLY
      } else {
        crc = crc >> 1n
      }
    }
    crc64Table[i] = crc
  }
  return crc64Table
}

/**
 * Calculate CRC-64 ISO checksum
 */
export function crc64(data: Uint8Array, initial: Checksum = 0n): Checksum {
  const table = getCRC64Table()
  let crc = initial ^ 0xffffffffffffffffn

  for (let i = 0; i < data.length; i++) {
    const index = Number((crc ^ BigInt(data[i])) & 0xffn)
    crc = (crc >> 8n) ^ table[index]
  }

  return crc ^ 0xffffffffffffffffn
}

/**
 * Calculate checksum for a database page
 */
export function checksumPage(
  pgno: number,
  data: Uint8Array,
  h?: Checksum,
): Checksum {
  // Include page number in checksum
  const pgnoBytes = new Uint8Array(4)
  new DataView(pgnoBytes.buffer).setUint32(0, pgno, false) // big-endian

  let checksum = h ?? 0n
  checksum = crc64(pgnoBytes, checksum)
  checksum = crc64(data, checksum)
  return checksum
}

/**
 * LTX Header encoder/decoder
 */
export class HeaderCodec {
  /**
   * Encode header to bytes
   */
  static encode(header: Header): Uint8Array {
    const buf = new ArrayBuffer(HEADER_SIZE)
    const view = new DataView(buf)
    const bytes = new Uint8Array(buf)

    let offset = 0

    // Magic (4 bytes)
    view.setUint32(offset, LTX_MAGIC, false)
    offset += 4

    // Flags (4 bytes)
    view.setUint32(offset, header.flags, false)
    offset += 4

    // Page size (4 bytes)
    view.setUint32(offset, header.pageSize, false)
    offset += 4

    // Commit (4 bytes)
    view.setUint32(offset, header.commit, false)
    offset += 4

    // Min TXID (8 bytes)
    view.setBigUint64(offset, header.minTxid, false)
    offset += 8

    // Max TXID (8 bytes)
    view.setBigUint64(offset, header.maxTxid, false)
    offset += 8

    // Timestamp (8 bytes)
    view.setBigInt64(offset, header.timestamp, false)
    offset += 8

    // Pre-apply checksum (8 bytes)
    view.setBigUint64(offset, header.preApplyChecksum, false)
    offset += 8

    // WAL offset (8 bytes)
    view.setBigInt64(offset, header.walOffset, false)
    offset += 8

    // WAL size (8 bytes)
    view.setBigInt64(offset, header.walSize, false)
    offset += 8

    // WAL salt1 (4 bytes)
    view.setUint32(offset, header.walSalt1, false)
    offset += 4

    // WAL salt2 (4 bytes)
    view.setUint32(offset, header.walSalt2, false)
    offset += 4

    // Node ID (8 bytes)
    view.setBigUint64(offset, header.nodeID, false)
    offset += 8

    // Reserved bytes (remaining to 100)
    // Already zero-initialized

    return bytes
  }

  /**
   * Decode header from bytes
   */
  static decode(bytes: Uint8Array): Header {
    if (bytes.length < HEADER_SIZE) {
      throw new Error(`header too short: ${bytes.length} < ${HEADER_SIZE}`)
    }

    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    let offset = 0

    // Magic (4 bytes)
    const magic = view.getUint32(offset, false)
    offset += 4

    if (magic !== LTX_MAGIC) {
      throw new Error(`invalid LTX magic: 0x${magic.toString(16)}`)
    }

    // Flags (4 bytes)
    const flags = view.getUint32(offset, false)
    offset += 4

    // Page size (4 bytes)
    const pageSize = view.getUint32(offset, false)
    offset += 4

    // Commit (4 bytes)
    const commit = view.getUint32(offset, false)
    offset += 4

    // Min TXID (8 bytes)
    const minTxid = view.getBigUint64(offset, false)
    offset += 8

    // Max TXID (8 bytes)
    const maxTxid = view.getBigUint64(offset, false)
    offset += 8

    // Timestamp (8 bytes)
    const timestamp = view.getBigInt64(offset, false)
    offset += 8

    // Pre-apply checksum (8 bytes)
    const preApplyChecksum = view.getBigUint64(offset, false)
    offset += 8

    // WAL offset (8 bytes)
    const walOffset = view.getBigInt64(offset, false)
    offset += 8

    // WAL size (8 bytes)
    const walSize = view.getBigInt64(offset, false)
    offset += 8

    // WAL salt1 (4 bytes)
    const walSalt1 = view.getUint32(offset, false)
    offset += 4

    // WAL salt2 (4 bytes)
    const walSalt2 = view.getUint32(offset, false)
    offset += 4

    // Node ID (8 bytes)
    const nodeID = view.getBigUint64(offset, false)
    offset += 8

    return {
      version: LTX_VERSION,
      flags,
      pageSize,
      commit,
      minTxid,
      maxTxid,
      timestamp,
      preApplyChecksum,
      walOffset,
      walSize,
      walSalt1,
      walSalt2,
      nodeID,
    }
  }
}

export class TrailerCodec {
  /**
   * Encode trailer to bytes
   */
  static encode(trailer: Trailer): Uint8Array {
    const buf = new ArrayBuffer(TRAILER_SIZE)
    const view = new DataView(buf)
    const bytes = new Uint8Array(buf)

    // Post-apply checksum (8 bytes)
    view.setBigUint64(0, trailer.postApplyChecksum, false)

    // File checksum (8 bytes)
    view.setBigUint64(8, trailer.fileChecksum, false)

    return bytes
  }

  /**
   * Decode trailer from bytes
   */
  static decode(bytes: Uint8Array): Trailer {
    if (bytes.length < TRAILER_SIZE) {
      throw new Error(`trailer too short: ${bytes.length} < ${TRAILER_SIZE}`)
    }

    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)

    return {
      postApplyChecksum: view.getBigUint64(0, false),
      fileChecksum: view.getBigUint64(8, false),
    }
  }
}

export class Encoder {
  #buffer: Uint8Array[] = []
  #_header: Header | null = null
  #_trailer: Trailer | null = null
  #fileChecksum: Checksum = 0n
  #bytesWritten = 0
  #pagesWritten = 0
  #lastPgno = 0
  #postApplyChecksum: Checksum = 0n

  // must be called firt
  encodeHeader(header: Header): void {
    if (this.#_header) {
      throw new Error("header already encoded")
    }

    this.#_header = header
    const bytes = HeaderCodec.encode(header)
    this.#buffer.push(bytes)
    this.#fileChecksum = crc64(bytes, this.#fileChecksum)
    this.#bytesWritten += bytes.length
  }

  encodePage(pageHeader: PageHeader, data: Uint8Array): void {
    if (!this.#_header) {
      throw new Error("header must be encoded first")
    }

    if (data.length !== this.#_header.pageSize) {
      throw new Error(
        `page data size mismatch: ${data.length} != ${this.#_header.pageSize}`,
      )
    }

    if (pageHeader.pgno <= this.#lastPgno) {
      throw new Error(
        `pages must be in ascending order: ${pageHeader.pgno} <= ${this.#lastPgno}`,
      )
    }

    // Encode page header (4 bytes for pgno)
    const headerBytes = new Uint8Array(PAGE_HEADER_SIZE)
    new DataView(headerBytes.buffer).setUint32(0, pageHeader.pgno, false)

    // Write header and data
    this.#buffer.push(headerBytes)
    this.#buffer.push(data)

    // Update checksum
    this.#fileChecksum = crc64(headerBytes, this.#fileChecksum)
    this.#fileChecksum = crc64(data, this.#fileChecksum)

    this.#bytesWritten += headerBytes.length + data.length
    this.#pagesWritten++
    this.#lastPgno = pageHeader.pgno
  }

  setPostApplyChecksum(checksum: Checksum): void {
    this.#postApplyChecksum = checksum
  }

  close(): Uint8Array {
    if (!this.#_header) {
      throw new Error("header must be encoded first")
    }

    // Create trailer
    this.#_trailer = {
      postApplyChecksum: this.#postApplyChecksum,
      fileChecksum: this.#fileChecksum,
    }

    const trailerBytes = TrailerCodec.encode(this.#_trailer)
    this.#buffer.push(trailerBytes)
    this.#bytesWritten += trailerBytes.length

    // Concatenate all buffers
    const result = new Uint8Array(this.#bytesWritten)
    let offset = 0
    for (const buf of this.#buffer) {
      result.set(buf, offset)
      offset += buf.length
    }

    return result
  }

  header(): Header {
    if (!this.#_header) {
      throw new Error("header not encoded yet")
    }
    return { ...this.#_header }
  }

  trailer(): Trailer {
    if (!this.#_trailer) {
      throw new Error("trailer not created yet")
    }
    return { ...this.#_trailer }
  }

  n(): number {
    return this.#bytesWritten
  }

  pageN(): number {
    return this.#pagesWritten
  }
}

export class Decoder {
  #data: Uint8Array
  #offset = 0
  #_header: Header | null = null
  #_trailer: Trailer | null = null
  #pagesRead = 0

  constructor(data: Uint8Array) {
    this.#data = data
  }

  decodeHeader(): Header {
    if (this.#_header) {
      return this.#_header
    }

    this.#_header = HeaderCodec.decode(this.#data.subarray(this.#offset))
    this.#offset += HEADER_SIZE
    return this.#_header
  }

  decodePage(): { header: PageHeader; data: Uint8Array } | null {
    if (!this.#_header) {
      throw new Error("header must be decoded first")
    }

    // Check if we've reached the trailer
    const remaining = this.#data.length - this.#offset
    if (remaining <= TRAILER_SIZE) {
      return null
    }

    // Read page header
    const view = new DataView(
      this.#data.buffer,
      this.#data.byteOffset + this.#offset,
    )
    const pgno = view.getUint32(0, false)
    this.#offset += PAGE_HEADER_SIZE

    // Read page data
    const pageData = this.#data.subarray(
      this.#offset,
      this.#offset + this.#_header.pageSize,
    )
    this.#offset += this.#_header.pageSize

    this.#pagesRead++

    return {
      header: { pgno },
      data: pageData,
    }
  }

  decodeAllPages(): Map<number, Uint8Array> {
    const pages = new Map<number, Uint8Array>()

    let page: ReturnType<typeof this.decodePage>
    while ((page = this.decodePage()) !== null) {
      pages.set(page.header.pgno, page.data)
    }

    return pages
  }

  verify(): void {
    // Ensure header is decoded
    this.decodeHeader()

    // Read all pages to advance to trailer
    while (this.decodePage() !== null) {
      // Just reading through
    }

    // Verify checksum
    this.close()
  }

  close(): Trailer {
    if (!this.#_header) {
      throw new Error("header must be decoded first")
    }

    // Decode trailer
    const trailerOffset = this.#data.length - TRAILER_SIZE
    this.#_trailer = TrailerCodec.decode(this.#data.subarray(trailerOffset))

    // Verify file checksum (if not disabled by flag)
    if (!(this.#_header.flags & HEADER_FLAG_NO_CHECKSUM)) {
      const dataToCheck = this.#data.subarray(0, trailerOffset)
      const computed = crc64(dataToCheck)
      if (computed !== this.#_trailer.fileChecksum) {
        throw new Error(
          `checksum mismatch: computed=0x${computed.toString(16)} expected=0x${
            this.#_trailer.fileChecksum.toString(16)
          }`,
        )
      }
    }

    return this.#_trailer
  }

  decodeDatabase(): Uint8Array {
    const header = this.decodeHeader()

    // Create database buffer
    const dbSize = header.commit * header.pageSize
    const db = new Uint8Array(dbSize)

    // Fill with pages
    let page: ReturnType<typeof this.decodePage>
    while ((page = this.decodePage()) !== null) {
      const offset = (page.header.pgno - 1) * header.pageSize
      db.set(page.data, offset)
    }

    return db
  }

  header(): Header {
    if (!this.#_header) {
      throw new Error("header not decoded yet")
    }
    return { ...this.#_header }
  }

  trailer(): Trailer {
    if (!this.#_trailer) {
      throw new Error("trailer not decoded yet")
    }
    return { ...this.#_trailer }
  }

  n(): number {
    return this.#offset
  }

  pageN(): number {
    return this.#pagesRead
  }

  postApplyPos(): Pos {
    if (!this.#_header || !this.#_trailer) {
      throw new Error("must decode header and close first")
    }
    return {
      txid: this.#_header.maxTxid,
      postApplyChecksum: this.#_trailer.postApplyChecksum,
    }
  }
}

export function peekHeader(data: Uint8Array): Header {
  return HeaderCodec.decode(data)
}

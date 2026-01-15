/**
 * SQLite WAL (Write-Ahead Log) reader implementation
 *
 * WAL file structure:
 * - Header (32 bytes): magic, version, page size, sequence, salt, checksum
 * - Frames: series of frame header (24 bytes) + page data
 *
 * Frame header structure:
 * - Page number (4 bytes)
 * - Commit marker (4 bytes) - DB size in pages at commit, 0 if not a commit frame
 * - Salt-1 (4 bytes)
 * - Salt-2 (4 bytes)
 * - Checksum-1 (4 bytes)
 * - Checksum-2 (4 bytes)
 */

import * as NFs from "node:fs"
import * as NPath from "node:path"

// WAL constants
export const WAL_HEADER_SIZE = 32
export const WAL_FRAME_HEADER_SIZE = 24

// WAL magic numbers
export const WAL_MAGIC_LE = 0x377f0682 // Little-endian checksums
export const WAL_MAGIC_BE = 0x377f0683 // Big-endian checksums

/**
 * WAL header structure
 */
export interface WalHeader {
  magic: number
  version: number
  pageSize: number
  sequence: number
  salt1: number
  salt2: number
  checksum1: number
  checksum2: number
  isLittleEndian: boolean
}

/**
 * WAL frame header structure
 */
export interface WalFrameHeader {
  pgno: number
  commit: number // DB size in pages if commit frame, 0 otherwise
  salt1: number
  salt2: number
  checksum1: number
  checksum2: number
}

/**
 * Read WAL header from file
 */
export function readHeader(walPath: string): WalHeader {
  const fd = NFs.openSync(walPath, "r")
  try {
    const buf = Buffer.alloc(WAL_HEADER_SIZE)
    const bytesRead = NFs.readSync(fd, buf, 0, WAL_HEADER_SIZE, 0)
    if (bytesRead < WAL_HEADER_SIZE) {
      throw new Error(`WAL header too short: ${bytesRead} bytes`)
    }
    return parseHeader(buf)
  } finally {
    NFs.closeSync(fd)
  }
}

/**
 * Parse WAL header from bytes
 */
export function parseHeader(buf: Buffer): WalHeader {
  const magic = buf.readUInt32BE(0)

  let isLittleEndian: boolean
  if (magic === WAL_MAGIC_LE) {
    isLittleEndian = true
  } else if (magic === WAL_MAGIC_BE) {
    isLittleEndian = false
  } else {
    throw new Error(`invalid WAL magic: 0x${magic.toString(16)}`)
  }

  const version = buf.readUInt32BE(4)
  if (version !== 3007000) {
    throw new Error(`unsupported WAL version: ${version}`)
  }

  return {
    magic,
    version,
    pageSize: buf.readUInt32BE(8),
    sequence: buf.readUInt32BE(12),
    salt1: buf.readUInt32BE(16),
    salt2: buf.readUInt32BE(20),
    checksum1: buf.readUInt32BE(24),
    checksum2: buf.readUInt32BE(28),
    isLittleEndian,
  }
}

/**
 * Parse WAL frame header from bytes
 */
export function parseFrameHeader(buf: Buffer): WalFrameHeader {
  return {
    pgno: buf.readUInt32BE(0),
    commit: buf.readUInt32BE(4),
    salt1: buf.readUInt32BE(8),
    salt2: buf.readUInt32BE(12),
    checksum1: buf.readUInt32BE(16),
    checksum2: buf.readUInt32BE(20),
  }
}

/**
 * Calculate WAL checksum
 * SQLite uses a non-standard checksum algorithm
 */
export function checksum(
  isLittleEndian: boolean,
  s0: number,
  s1: number,
  data: Buffer,
): [number, number] {
  if (data.length % 8 !== 0) {
    throw new Error("checksum data must be 8-byte aligned")
  }

  for (let i = 0; i < data.length; i += 8) {
    const v0 = isLittleEndian ? data.readUInt32LE(i) : data.readUInt32BE(i)
    const v1 = isLittleEndian
      ? data.readUInt32LE(i + 4)
      : data.readUInt32BE(i + 4)

    s0 = (s0 + v0 + s1) >>> 0
    s1 = (s1 + v1 + s0) >>> 0
  }

  return [s0, s1]
}

/**
 * WAL Reader - parses SQLite WAL files
 */
export class WalReader {
  #fd: number
  #header: WalHeader
  #frameN = 0
  #_offset: number = WAL_HEADER_SIZE
  #checksum1: number
  #checksum2: number

  constructor(
    fd: number,
    header: WalHeader,
  ) {
    this.#fd = fd
    this.#header = header
    this.#checksum1 = header.checksum1
    this.#checksum2 = header.checksum2
  }

  /**
   * Create a new WAL reader from a file path
   */
  static open(walPath: string): WalReader {
    const fd = NFs.openSync(walPath, "r")
    try {
      const headerBuf = Buffer.alloc(WAL_HEADER_SIZE)
      const bytesRead = NFs.readSync(fd, headerBuf, 0, WAL_HEADER_SIZE, 0)
      if (bytesRead < WAL_HEADER_SIZE) {
        throw new Error(`WAL header too short: ${bytesRead} bytes`)
      }

      const header = parseHeader(headerBuf)

      // Verify header checksum
      const [cs1, cs2] = checksum(
        header.isLittleEndian,
        0,
        0,
        headerBuf.subarray(0, 24),
      )
      if (cs1 !== header.checksum1 || cs2 !== header.checksum2) {
        throw new Error("WAL header checksum mismatch")
      }

      return new WalReader(fd, header)
    } catch (err) {
      NFs.closeSync(fd)
      throw err
    }
  }

  /**
   * Create a WAL reader starting at a specific offset with known salt values
   */
  static openWithOffset(
    walPath: string,
    offset: number,
    salt1: number,
    salt2: number,
  ): WalReader {
    if (offset <= WAL_HEADER_SIZE) {
      throw new Error(
        `offset must be greater than WAL header size: ${offset} <= ${WAL_HEADER_SIZE}`,
      )
    }

    const fd = NFs.openSync(walPath, "r")
    try {
      const headerBuf = Buffer.alloc(WAL_HEADER_SIZE)
      NFs.readSync(fd, headerBuf, 0, WAL_HEADER_SIZE, 0)
      const header = parseHeader(headerBuf)

      // Override salt with provided values (in case beginning was overwritten)
      header.salt1 = salt1
      header.salt2 = salt2

      const reader = new WalReader(fd, header)

      // Calculate frame number from offset
      const frameSize = WAL_FRAME_HEADER_SIZE + header.pageSize
      if ((offset - WAL_HEADER_SIZE) % frameSize !== 0) {
        throw new Error(
          `unaligned WAL offset ${offset} for page size ${header.pageSize}`,
        )
      }
      reader.#frameN = Math.floor((offset - WAL_HEADER_SIZE) / frameSize)
      reader.#_offset = offset

      // Read previous frame to get checksum state
      reader.#frameN--
      reader.#_offset -= frameSize

      const prevFrame = reader.#readFrameInternal(false)
      if (!prevFrame) {
        throw new Error("failed to read previous frame for checksum")
      }

      return reader
    } catch (err) {
      NFs.closeSync(fd)
      throw err
    }
  }

  /**
   * Get page size
   */
  pageSize(): number {
    return this.#header.pageSize
  }

  /**
   * Get current offset
   */
  offset(): number {
    return this.#_offset
  }

  /**
   * Get salt values
   */
  salt(): [number, number] {
    return [this.#header.salt1, this.#header.salt2]
  }

  /**
   * Read next frame
   * Returns null at end of valid WAL
   */
  readFrame(): { pgno: number; commit: number; data: Buffer } | null {
    return this.#readFrameInternal(true)
  }

  #readFrameInternal = (
    verifyChecksum: boolean,
  ): { pgno: number; commit: number; data: Buffer } | null => {
    const frameSize = WAL_FRAME_HEADER_SIZE + this.#header.pageSize
    const frameBuf = Buffer.alloc(frameSize)

    const bytesRead = NFs.readSync(
      this.#fd,
      frameBuf,
      0,
      frameSize,
      this.#_offset,
    )
    if (bytesRead < frameSize) {
      return null // EOF
    }

    const frameHeader = parseFrameHeader(frameBuf)

    // Verify salt matches
    if (
      frameHeader.salt1 !== this.#header.salt1
      || frameHeader.salt2 !== this.#header.salt2
    ) {
      return null // Salt mismatch indicates end of valid frames
    }

    const data = frameBuf.subarray(WAL_FRAME_HEADER_SIZE)

    // Verify checksum
    if (verifyChecksum) {
      const [cs1, cs2] = checksum(
        this.#header.isLittleEndian,
        this.#checksum1,
        this.#checksum2,
        frameBuf.subarray(0, 8),
      )
      const [cs3, cs4] = checksum(this.#header.isLittleEndian, cs1, cs2, data)

      if (cs3 !== frameHeader.checksum1 || cs4 !== frameHeader.checksum2) {
        return null // Checksum mismatch indicates end of valid frames
      }

      this.#checksum1 = cs3
      this.#checksum2 = cs4
    } else {
      this.#checksum1 = frameHeader.checksum1
      this.#checksum2 = frameHeader.checksum2
    }

    this.#_offset += frameSize
    this.#frameN++

    return {
      pgno: frameHeader.pgno,
      commit: frameHeader.commit,
      data,
    }
  }

  /**
   * Build a page map of all committed pages
   * Returns map of pgno -> offset, max offset, and final commit size
   */
  pageMap(): { map: Map<number, number>; maxOffset: number; commit: number } {
    const map = new Map<number, number>()
    const txMap = new Map<number, number>()
    let maxOffset = 0
    let commit = 0

    let frame: ReturnType<typeof this.readFrame>
    while ((frame = this.readFrame()) !== null) {
      // Track page offset for this transaction
      txMap.set(
        frame.pgno,
        this.#_offset - this.#header.pageSize - WAL_FRAME_HEADER_SIZE,
      )

      // If this is a commit frame, copy to main map
      if (frame.commit !== 0) {
        for (const [pgno, offset] of txMap) {
          map.set(pgno, offset)
        }
        commit = frame.commit
        maxOffset = this.#_offset
        txMap.clear()
      }
    }

    // Remove pages beyond final commit size
    for (const pgno of map.keys()) {
      if (pgno > commit) {
        map.delete(pgno)
      }
    }

    return { map, maxOffset, commit }
  }

  /**
   * Get all unique frame salts up to a target salt
   */
  frameSaltsUntil(targetSalt: [number, number]): Set<string> {
    const salts = new Set<string>()
    const frameSize = WAL_FRAME_HEADER_SIZE + this.#header.pageSize

    let offset = WAL_HEADER_SIZE
    const frameBuf = Buffer.alloc(WAL_FRAME_HEADER_SIZE)

    while (true) {
      const bytesRead = NFs.readSync(
        this.#fd,
        frameBuf,
        0,
        WAL_FRAME_HEADER_SIZE,
        offset,
      )
      if (bytesRead < WAL_FRAME_HEADER_SIZE) {
        break
      }

      const salt1 = frameBuf.readUInt32BE(8)
      const salt2 = frameBuf.readUInt32BE(12)

      salts.add(`${salt1}:${salt2}`)

      if (salt1 === targetSalt[0] && salt2 === targetSalt[1]) {
        break
      }

      offset += frameSize
    }

    return salts
  }

  /**
   * Close the reader
   */
  close(): void {
    NFs.closeSync(this.#fd)
  }
}

/**
 * Read page data from WAL at a specific offset
 */
export function readPageAt(
  walPath: string,
  offset: number,
  pageSize: number,
): Buffer {
  const fd = NFs.openSync(walPath, "r")
  try {
    const data = Buffer.alloc(pageSize)
    NFs.readSync(fd, data, 0, pageSize, offset + WAL_FRAME_HEADER_SIZE)
    return data
  } finally {
    NFs.closeSync(fd)
  }
}

/**
 * Calculate WAL size for given page size and page count
 */
export function calcSize(pageSize: number, pageCount: number): number {
  return WAL_HEADER_SIZE + (WAL_FRAME_HEADER_SIZE + pageSize) * pageCount
}

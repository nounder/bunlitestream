/**
 * S3 Replica Client using Bun's built-in S3 module
 *
 * This implements the ReplicaClient interface for storing LTX files in S3-compatible storage.
 */

import { S3Client } from "bun"
import * as Ltx from "./Ltx.ts"

export interface S3ReplicaClientConfig {
  bucket: string
  path?: string
  region?: string
  endpoint?: string
  accessKeyId?: string
  secretAccessKey?: string
}

export interface FileIterator {
  next(): Promise<Ltx.FileInfo | null>
  close(): void
}

export interface ReplicaClient {
  type(): string
  init(): Promise<void>
  ltxFiles(level: number, seek?: Ltx.TXID): FileIterator
  openLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
    offset?: number,
    size?: number,
  ): Promise<ReadableStream<Uint8Array>>
  writeLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
    data: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<Ltx.FileInfo>
  deleteLtxFiles(files: Ltx.FileInfo[]): Promise<void>
  deleteAll(): Promise<void>
}

/**
 * S3 Replica Client implementation using Bun's S3
 */
export class S3ReplicaClient implements ReplicaClient {
  #config: S3ReplicaClientConfig
  #s3: S3Client | null = null

  constructor(config: S3ReplicaClientConfig) {
    this.#config = config
  }

  type(): string {
    return "s3"
  }

  async init(): Promise<void> {
    if (this.#s3) return

    this.#s3 = new S3Client({
      bucket: this.#config.bucket,
      region: this.#config.region,
      endpoint: this.#config.endpoint,
      accessKeyId: this.#config.accessKeyId,
      secretAccessKey: this.#config.secretAccessKey,
    })
  }

  /**
   * Get the S3 key for an LTX file
   */
  #ltxKey = (level: number, minTxid: Ltx.TXID, maxTxid: Ltx.TXID): string => {
    const levelStr = level.toString(16).padStart(4, "0")
    const filename = Ltx.formatFilename(minTxid, maxTxid)
    const basePath = this.#config.path || ""
    return basePath
      ? `${basePath}/${levelStr}/${filename}`
      : `${levelStr}/${filename}`
  }

  /**
   * Get the prefix for listing LTX files at a level
   */
  #ltxPrefix = (level: number): string => {
    const levelStr = level.toString(16).padStart(4, "0")
    const basePath = this.#config.path || ""
    return basePath ? `${basePath}/${levelStr}/` : `${levelStr}/`
  }

  /**
   * List LTX files at a given level
   */
  ltxFiles(level: number, seek?: Ltx.TXID): FileIterator {
    const prefix = this.#ltxPrefix(level)
    const client = this

    // Use an async generator internally
    async function* listFiles() {
      if (!client.#s3) {
        throw new Error("S3 client not initialized")
      }

      let startAfter: string | undefined
      while (true) {
        const result = await client.#s3.list({
          prefix,
          startAfter,
        })

        const files: Ltx.FileInfo[] = []
        if (result.contents) {
          for (const obj of result.contents) {
            const key = obj.key
            const filename = key.split("/").pop()
            if (!filename || !filename.endsWith(".ltx")) continue

            try {
              const { minTxid, maxTxid } = Ltx.parseFilename(filename)
              files.push({
                level,
                minTxid,
                maxTxid,
                size: obj.size || 0,
                createdAt: obj.lastModified || new Date(),
              })
            } catch {
              // Skip invalid filenames
            }
          }
        }

        // Sort by minTxid
        files.sort((a, b) => Number(a.minTxid - b.minTxid))

        for (const file of files) {
          // Skip files before seek Ltx.TXID
          if (seek && file.minTxid < seek) {
            continue
          }
          yield file
        }

        if (!result.isTruncated) break

        // Use last key as marker for next page
        if (result.contents && result.contents.length > 0) {
          startAfter = result.contents[result.contents.length - 1].key
        } else {
          break
        }
      }
    }

    const generator = listFiles()
    return {
      async next(): Promise<Ltx.FileInfo | null> {
        const result = await generator.next()
        return result.done ? null : result.value
      },
      close(): void {
        // Generator cleanup
      },
    }
  }

  /**
   * Open an LTX file for reading
   */
  async openLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
    offset = 0,
    size = 0,
  ): Promise<ReadableStream<Uint8Array>> {
    await this.init()
    if (!this.#s3) {
      throw new Error("S3 client not initialized")
    }

    const key = this.#ltxKey(level, minTxid, maxTxid)
    const file = this.#s3.file(key)

    // Use Bun's S3 slice for range requests
    if (offset > 0 || size > 0) {
      const end = size > 0 ? offset + size : undefined
      const sliced = file.slice(offset, end)
      return sliced.stream()
    }

    return file.stream()
  }

  /**
   * Write an LTX file
   */
  async writeLtxFile(
    level: number,
    minTxid: Ltx.TXID,
    maxTxid: Ltx.TXID,
    data: Uint8Array | ReadableStream<Uint8Array>,
  ): Promise<Ltx.FileInfo> {
    await this.init()
    if (!this.#s3) {
      throw new Error("S3 client not initialized")
    }

    const key = this.#ltxKey(level, minTxid, maxTxid)

    // Get data as Uint8Array for header extraction
    let bytes: Uint8Array
    if (data instanceof Uint8Array) {
      bytes = data
    } else {
      // Read stream to buffer
      const chunks: Uint8Array[] = []
      const reader = data.getReader()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        chunks.push(value)
      }
      const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
      bytes = new Uint8Array(totalLength)
      let offset = 0
      for (const chunk of chunks) {
        bytes.set(chunk, offset)
        offset += chunk.length
      }
    }

    // Extract timestamp from LTX header
    let timestamp: Date
    try {
      const header = Ltx.peekHeader(bytes)
      timestamp = new Date(Number(header.timestamp))
    } catch {
      timestamp = new Date()
    }

    // Write to S3 with metadata
    const file = this.#s3.file(key)
    await file.write(bytes, {
      type: "application/octet-stream",
    })

    return {
      level,
      minTxid,
      maxTxid,
      size: bytes.length,
      createdAt: timestamp,
    }
  }

  /**
   * Delete LTX files
   */
  async deleteLtxFiles(files: Ltx.FileInfo[]): Promise<void> {
    await this.init()
    if (!this.#s3) {
      throw new Error("S3 client not initialized")
    }

    // Delete files in parallel
    await Promise.all(
      files.map(async (file) => {
        const key = this.#ltxKey(file.level, file.minTxid, file.maxTxid)
        const s3File = this.#s3!.file(key)
        await s3File.delete()
      }),
    )
  }

  /**
   * Delete all files
   */
  async deleteAll(): Promise<void> {
    await this.init()
    if (!this.#s3) {
      throw new Error("S3 client not initialized")
    }

    const prefix = this.#config.path || ""

    // List and delete all objects with prefix
    let startAfter: string | undefined
    while (true) {
      const result = await this.#s3.list({
        prefix,
        startAfter,
      })

      if (!result.contents || result.contents.length === 0) break

      // Delete files
      await Promise.all(
        result.contents.map(async (obj) => {
          const file = this.#s3!.file(obj.key)
          await file.delete()
        }),
      )

      if (!result.isTruncated) break

      startAfter = result.contents[result.contents.length - 1].key
    }
  }
}

/**
 * Create S3 replica client from URL
 * URL format: s3://bucket/path?region=us-east-1&endpoint=...
 */
export function make(url: string): S3ReplicaClient {
  const parsed = new URL(url)

  if (parsed.protocol !== "s3:") {
    throw new Error(`Invalid S3 URL protocol: ${parsed.protocol}`)
  }

  const config: S3ReplicaClientConfig = {
    bucket: parsed.hostname,
    path: parsed.pathname.replace(/^\//, ""),
    region: parsed.searchParams.get("region") || undefined,
    endpoint: parsed.searchParams.get("endpoint") || undefined,
    accessKeyId: parsed.searchParams.get("accessKeyId")
      || process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: parsed.searchParams.get("secretAccessKey")
      || process.env.AWS_SECRET_ACCESS_KEY,
  }

  return new S3ReplicaClient(config)
}

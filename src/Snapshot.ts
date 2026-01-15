/**
 * Snapshot functionality for Litestream
 *
 * Restores a SQLite database from LTX files stored in a replica.
 */

import * as NFs from "node:fs"
import * as NPath from "node:path"
import * as Ltx from "./Ltx.ts"
import type * as S3 from "./S3.ts"

// Snapshot level (L9 in litestream)
export const SNAPSHOT_LEVEL = 9

/**
 * Snapshot options
 */
export interface RestoreOptions {
  outputPath: string
  txid?: Ltx.TXID
  timestamp?: Date
}

/**
 * Calculate restore plan - find LTX files needed to restore to a given state
 */
export async function calcRestorePlan(
  client: S3.ReplicaClient,
  txid?: Ltx.TXID,
  timestamp?: Date,
): Promise<Ltx.FileInfo[]> {
  if (txid && timestamp) {
    throw new Error("cannot specify both txid and timestamp")
  }

  const infos: Ltx.FileInfo[] = []

  // Find snapshot before target
  const snapshots = await findLtxFiles(client, SNAPSHOT_LEVEL, (info) => {
    if (txid) {
      return info.maxTxid <= txid
    }
    if (timestamp) {
      return info.createdAt <= timestamp
    }
    return true
  })

  if (snapshots.length > 0) {
    // Use the most recent snapshot
    infos.push(snapshots[snapshots.length - 1])
  }

  // Find LTX files from higher levels down to L0
  const maxLevel = SNAPSHOT_LEVEL - 1
  for (let level = maxLevel; level >= 0; level--) {
    const maxTxid = infos.length > 0 ? infos[infos.length - 1].maxTxid : 0n

    const files = await findLtxFiles(client, level, (info) => {
      // Skip if already covered
      if (info.maxTxid <= maxTxid) {
        return false
      }

      // Filter by target
      if (txid) {
        return info.maxTxid <= txid
      }
      if (timestamp) {
        return info.createdAt <= timestamp
      }
      return true
    })

    for (const file of files) {
      const prevMax = infos.length > 0 ? infos[infos.length - 1].maxTxid : 0n

      // Skip if this file's range is already covered by previously added files.
      // This can happen when a larger compacted file at the same level covers
      // a smaller file's entire range (see issue #847).
      if (file.maxTxid <= prevMax) {
        continue
      }

      // Check contiguity
      if (!Ltx.isContiguous(prevMax, file.minTxid)) {
        throw new Error(
          `non-contiguous transaction files: prev=${prevMax} file=${
            Ltx.formatFilename(file.minTxid, file.maxTxid)
          }`,
        )
      }

      infos.push(file)
    }
  }

  if (infos.length === 0) {
    throw new Error("no matching backup files available")
  }

  return infos
}

async function findLtxFiles(
  client: S3.ReplicaClient,
  level: number,
  filter: (info: Ltx.FileInfo) => boolean,
): Promise<Ltx.FileInfo[]> {
  const files: Ltx.FileInfo[] = []
  const iterator = client.ltxFiles(level)

  let info = await iterator.next()
  while (info) {
    if (filter(info)) {
      files.push(info)
    }
    info = await iterator.next()
  }
  iterator.close()

  return files
}

export async function restore(
  client: S3.ReplicaClient,
  options: RestoreOptions,
): Promise<void> {
  // Validate options
  if (!options.outputPath) {
    throw new Error("output path required")
  }

  // Ensure output doesn't exist
  if (NFs.existsSync(options.outputPath)) {
    throw new Error(`output path already exists: ${options.outputPath}`)
  }

  // Calculate restore plan
  const plan = await calcRestorePlan(client, options.txid, options.timestamp)

  console.log(`Restore plan: ${plan.length} files`)

  // Read all LTX files
  const decoders: Ltx.Decoder[] = []

  for (const info of plan) {
    console.log(`Reading ${Ltx.formatFilename(info.minTxid, info.maxTxid)}...`)

    const stream = await client.openLtxFile(
      info.level,
      info.minTxid,
      info.maxTxid,
    )
    const chunks: Uint8Array[] = []
    const reader = stream.getReader()

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(value)
    }

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
    const data = new Uint8Array(totalLength)
    let offset = 0
    for (const chunk of chunks) {
      data.set(chunk, offset)
      offset += chunk.length
    }

    decoders.push(new Ltx.Decoder(data))
  }

  // Compact LTX files into database
  console.log("Compacting into database...")

  // Get header from first file for page size and commit
  const firstHeader = decoders[0].decodeHeader()
  const pageSize = firstHeader.pageSize

  // Find final commit size
  let commit = firstHeader.commit
  for (const decoder of decoders) {
    const header = decoder.decodeHeader()
    if (header.commit > 0) {
      commit = header.commit
    }
  }

  // Create database buffer
  const dbSize = commit * pageSize
  const db = new Uint8Array(dbSize)

  // Apply pages from all LTX files
  // Later files override earlier ones
  for (const decoder of decoders) {
    decoder.decodeHeader() // Already decoded, but reset position

    let page: ReturnType<typeof decoder.decodePage>
    while ((page = decoder.decodePage()) !== null) {
      const offset = (page.header.pgno - 1) * pageSize
      if (offset + page.data.length <= db.length) {
        db.set(page.data, offset)
      }
    }
  }

  // Create parent directory
  NFs.mkdirSync(NPath.dirname(options.outputPath), { recursive: true })

  // Write to temp file and rename
  const tmpPath = options.outputPath + ".tmp"
  NFs.writeFileSync(tmpPath, db)
  NFs.renameSync(tmpPath, options.outputPath)

  console.log(`Restored to ${options.outputPath}`)
}

export async function compact(decoders: Ltx.Decoder[]): Promise<Uint8Array> {
  if (decoders.length === 0) {
    throw new Error("no LTX files to compact")
  }

  // Collect all pages, later files override earlier
  const pages = new Map<number, Uint8Array>()

  let pageSize = 0
  let commit = 0
  let minTxid = 0n
  let maxTxid = 0n

  for (const decoder of decoders) {
    const header = decoder.decodeHeader()

    if (pageSize === 0) {
      pageSize = header.pageSize
      minTxid = header.minTxid
    }
    if (header.commit > 0) {
      commit = header.commit
    }
    if (header.maxTxid > maxTxid) {
      maxTxid = header.maxTxid
    }

    let page: ReturnType<typeof decoder.decodePage>
    while ((page = decoder.decodePage()) !== null) {
      // Copy data to avoid reference issues
      const data = new Uint8Array(page.data.length)
      data.set(page.data)
      pages.set(page.header.pgno, data)
    }
  }

  // Create compacted LTX file
  const encoder = new Ltx.Encoder()

  encoder.encodeHeader({
    version: 1,
    flags: 0,
    pageSize,
    commit,
    minTxid,
    maxTxid,
    timestamp: BigInt(Date.now()),
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  })

  // Write pages in order
  const sortedPgnos = Array.from(pages.keys()).sort((a, b) => a - b)
  for (const pgno of sortedPgnos) {
    encoder.encodePage({ pgno }, pages.get(pgno)!)
  }

  return encoder.close()
}

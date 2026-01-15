import * as Ltx from "./Ltx.ts"
import type * as S3 from "./S3.ts"

export class MockReplicaClient implements S3.ReplicaClient {
  #files: Map<string, { data: Uint8Array; info: Ltx.FileInfo }> = new Map()
  #fileInfosByLevel: Map<number, Ltx.FileInfo[]> = new Map()

  type(): string {
    return "mock"
  }

  async init(): Promise<void> {}

  #key = (level: number, minTxid: Ltx.TXID, maxTxid: Ltx.TXID): string => {
    return `${level}/${minTxid.toString(16)}-${maxTxid.toString(16)}`
  }

  setFileInfos(level: number, infos: Ltx.FileInfo[]): void {
    this.#fileInfosByLevel.set(level, infos)
  }

  ltxFiles(level: number, seek?: Ltx.TXID): S3.FileIterator {
    const predefinedInfos = this.#fileInfosByLevel.get(level)
    if (predefinedInfos) {
      const filtered = predefinedInfos
        .filter((f) => !seek || f.minTxid >= seek)
        .sort((a, b) => Number(a.minTxid - b.minTxid))
      let index = 0
      return {
        async next(): Promise<Ltx.FileInfo | null> {
          if (index >= filtered.length) return null
          return filtered[index++]
        },
        close(): void {},
      }
    }

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
}

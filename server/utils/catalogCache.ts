import { brotliCompressSync } from 'node:zlib'

const CACHE_TTL = 15 * 60 * 1000 // 15 minutes

const cache = new Map<string, { data: Buffer, timestamp: number }>()

export function getCachedCatalog(termId: string): Buffer | null {
  const entry = cache.get(termId)
  if (!entry) return null
  if (Date.now() - entry.timestamp > CACHE_TTL) {
    cache.delete(termId)
    return null
  }
  return entry.data
}

export function setCachedCatalog(termId: string, json: string): Buffer {
  const compressed = brotliCompressSync(Buffer.from(json))
  cache.set(termId, { data: compressed, timestamp: Date.now() })
  return compressed
}

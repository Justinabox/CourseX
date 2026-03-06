const CACHE_TTL = 15 * 60 * 1000 // 15 minutes

const cache = new Map<string, { json: string, timestamp: number }>()

export function getCachedCatalog(termId: string): string | null {
  const entry = cache.get(termId)
  if (!entry) return null
  if (Date.now() - entry.timestamp > CACHE_TTL) {
    cache.delete(termId)
    return null
  }
  return entry.json
}

export function setCachedCatalog(termId: string, json: string): string {
  cache.set(termId, { json, timestamp: Date.now() })
  return json
}

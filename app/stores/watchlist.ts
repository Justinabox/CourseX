import { defineStore } from 'pinia'
import { computed, ref, watch, onMounted } from 'vue'
import { useTermId } from '@/composables/useTermId'
import type { UICourse } from '@/composables/api/types'
import { listAllCourses } from '@/composables/api/queries'
import { normalizeCourseCode, normalizeSectionId } from '@/utils/normalize'

export const useWatchlistStore = defineStore('watchlist', () => {
  const byTerm = ref<Record<string, Record<string, UICourse>>>({})
  const keysByTerm = ref<Record<string, string[]>>({})
  const { termId } = useTermId()

  function keyFor(term: string) { return `cx:watchlist:${term}` }

  function normalizeKeysRaw(raw: unknown): { keys: string[]; migrated: boolean } {
    try {
      const obj: any = raw || {}
      let value: any = (obj && obj.watchlistsByTerm && obj.watchlistsByTerm[termId.value]) ? obj.watchlistsByTerm[termId.value] : obj
      if (value && value.watchlistsByTerm && value.watchlistsByTerm[termId.value]) {
        value = value.watchlistsByTerm[termId.value]
      }

      // Case 1: Already new format - array of keys (strings containing ::)
      if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'string') {
        if ((value[0] as string).includes('::')) {
          return { keys: value as string[], migrated: false }
        }
        // Legacy format - array of {code, sectionId} objects
        return migratePairsToKeys(value as any[])
      }

      // Case 2: Legacy map format
      if (value && typeof value === 'object') {
        return migrateMapToKeys(value)
      }

      return { keys: [], migrated: false }
    } catch {
      return { keys: [], migrated: false }
    }
  }

  function migratePairsToKeys(pairs: any[]): { keys: string[]; migrated: boolean } {
    const keys: string[] = []
    const seen = new Set<string>()

    // Fallback: use code-only keys (legacy pair format migration should already be complete for most users)
    for (const pair of pairs) {
      const code = normalizeCourseCode((pair?.code || '').toString())
      if (!code || seen.has(code)) continue
      seen.add(code)
      keys.push(code)
    }
    return { keys, migrated: true }
  }

  function migrateMapToKeys(value: any): { keys: string[]; migrated: boolean } {
    const keys: string[] = []
    const seen = new Set<string>()

    try {
      for (const [k, course] of Object.entries<any>(value || {})) {
        const code = normalizeCourseCode((k || course?.code || '').toString())
        const title = (course?.title || course?.name || k).toString().trim()
        const key = `${code}::${title.toUpperCase()}`
        if (!seen.has(key)) {
          seen.add(key)
          keys.push(key)
        }
      }
      return { keys, migrated: true }
    } catch {
      return { keys: [], migrated: false }
    }
  }

  function currentMap(): Record<string, UICourse> {
    return byTerm.value[termId.value] || {}
  }

  function setCurrentMap(next: Record<string, UICourse>) {
    byTerm.value = { ...byTerm.value, [termId.value]: next }
  }

  function currentKeys(): string[] {
    return keysByTerm.value[termId.value] || []
  }

  function setCurrentKeys(next: string[]) {
    keysByTerm.value = { ...keysByTerm.value, [termId.value]: next }
  }

  async function hydrateForCurrentTerm() {
    try {
      const allCourses = await listAllCourses()
      const keys = currentKeys()
      const map: Record<string, UICourse> = {}

      for (const course of allCourses) {
        const key = `${normalizeCourseCode(course.code)}::${course.title.trim().toUpperCase()}`
        if (keys.includes(key)) {
          map[key] = course
        }
      }

      setCurrentMap(map)
    } catch {
      setCurrentMap({})
    }
  }

  if (process.client) {
    onMounted(() => {
      const loadFromStorageForCurrentTerm = async () => {
        try {
          const raw = localStorage.getItem(keyFor(termId.value))
          if (raw != null) {
            const parsed = JSON.parse(raw)
            const { keys, migrated } = normalizeKeysRaw(parsed)
            setCurrentKeys(keys)
            if (migrated) {
              try { localStorage.setItem(keyFor(termId.value), JSON.stringify({ watchlistsByTerm: { [termId.value]: keys } })) } catch {}
            }
          }
        } catch {}
        await hydrateForCurrentTerm()
      }

      loadFromStorageForCurrentTerm()

      watch(termId, () => {
        loadFromStorageForCurrentTerm()
      })

      watch(() => keysByTerm.value[termId.value], (v) => {
        const list = Array.isArray(v) ? v : []
        const normalized = list.map((k) => (k || '').toString().trim()).filter(Boolean)
        try { localStorage.setItem(keyFor(termId.value), JSON.stringify({ watchlistsByTerm: { [termId.value]: normalized } })) } catch {}
        hydrateForCurrentTerm()
      }, { deep: true })
    })
  }

  const watchlistCourses = computed<UICourse[]>(() => Object.values(currentMap() || {}))

  const totalWatchlistCourses = computed<number>(() => {
    return currentKeys().length
  })

  function createKey(code: string, title: string): string {
    return `${normalizeCourseCode(code)}::${title.trim().toUpperCase()}`
  }

  function upsertWatchlistItem(code: string, title: string) {
    const key = createKey(code, title)
    if (!key) return
    const keys = currentKeys()
    const exists = keys.includes(key)
    if (exists) {
      removeFromWatchlist(code, title)
      return
    }
    setCurrentKeys([...keys, key])
  }

  function hasInWatchlist(code: string, title: string): boolean {
    const key = createKey(code, title)
    if (!key) return false
    const keys = currentKeys()
    return keys.includes(key)
  }

  function removeFromWatchlist(code: string, title: string) {
    const key = createKey(code, title)
    if (!key) return
    const keys = currentKeys()
    const next = keys.filter((k) => k !== key)
    setCurrentKeys(next)
  }

  return {
    byTerm,
    watchlistCourses,
    upsertWatchlistItem,
    hasInWatchlist,
    removeFromWatchlist,
    totalWatchlistCourses,
  }
})

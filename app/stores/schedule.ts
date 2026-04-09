import { defineStore } from 'pinia'
import { computed, ref, triggerRef, watch, onMounted } from 'vue'
import { useTermId } from '@/composables/useTermId'
import type { UICourse, UICourseSection, SchedulePair, Schedule } from '@/composables/api/types'
import { scheduleToBlocks, type ScheduleBlock } from '@/utils/scheduleUtils'
import { normalizeCourseCode, normalizeSectionId } from '@/utils/normalize'
import { hydrateScheduledCourses } from '@/utils/scheduleHydration'
import { fetchScheduleData, putScheduleEntries, postScheduleEntry, deleteScheduleEntry } from '@/composables/api/queries'

export const useScheduleStore = defineStore('schedule', () => {
  const byTerm = ref<Record<string, Record<string, UICourse>>>({})
  const pairsByTerm = ref<Record<string, SchedulePair[]>>({})
  const manualBlocks = ref<ScheduleBlock[]>([])
  const { termId } = useTermId()

  // Auth state — resolved once during setup, reused by all mutations
  const { loggedIn } = useUserSession()

  // Sequential promise chain for server sync (prevents race conditions)
  let syncChain = Promise.resolve()
  function enqueue<T>(fn: () => Promise<T>): Promise<T> {
    const p = syncChain.then(fn, fn)
    syncChain = p.then(() => {}, () => {})
    return p
  }

  function keyFor(term: string) { return `cx:schedule:${term}` }
  function manualKeyFor(term: string) { return `cx:scheduleManual:${term}` }

  function normalizeManualBlocksRaw(raw: unknown, term: string): ScheduleBlock[] {
    try {
      const obj: any = raw || []
      let list: any = (obj && obj.schedulesByTerm && obj.schedulesByTerm[term]) ? obj.schedulesByTerm[term] : obj
      if (list && list.schedulesByTerm && list.schedulesByTerm[term]) {
        list = list.schedulesByTerm[term]
      }
      return Array.isArray(list) ? list as ScheduleBlock[] : []
    } catch {
      return []
    }
  }

  function normalizePairsRaw(raw: unknown, term: string): { pairs: SchedulePair[]; migrated: boolean } {
    try {
      const obj: any = raw || {}
      let value: any = (obj && obj.schedulesByTerm && obj.schedulesByTerm[term]) ? obj.schedulesByTerm[term] : obj
      if (value && value.schedulesByTerm && value.schedulesByTerm[term]) {
        value = value.schedulesByTerm[term]
      }
      if (Array.isArray(value)) {
        const pairs = (value as any[]).map((v) => ({
          code: normalizeCourseCode((v?.code || '').toString()),
          sectionId: normalizeSectionId((v?.sectionId || '').toString()),
        })).filter((p) => p.code && p.sectionId)
        return { pairs, migrated: false }
      }
      if (value && typeof value === 'object') {
        const pairs: SchedulePair[] = []
        for (const [k, course] of Object.entries<any>(value || {})) {
          const code = normalizeCourseCode((k || course?.code || '').toString())
          const sections = (course?.sections || []) as any[]
          for (const s of sections) {
            const sid = normalizeSectionId((s?.sectionId || '').toString())
            if (code && sid) pairs.push({ code, sectionId: sid })
          }
        }
        if (pairs.length > 0) return { pairs, migrated: true }
      }
      return { pairs: [], migrated: false }
    } catch {
      return { pairs: [], migrated: false }
    }
  }

  function currentMap(term?: string): Record<string, UICourse> {
    return byTerm.value[term ?? termId.value] || {}
  }

  function setCurrentMap(next: Record<string, UICourse>, term?: string) {
    const t = term ?? termId.value
    byTerm.value = { ...byTerm.value, [t]: next }
    // Force trigger: ensure Vue sees the ref value replacement
    triggerRef(byTerm)
  }

  function currentPairs(term?: string): SchedulePair[] {
    return pairsByTerm.value[term ?? termId.value] || []
  }

  function setCurrentPairs(next: SchedulePair[], term?: string) {
    pairsByTerm.value = { ...pairsByTerm.value, [term ?? termId.value]: next }
  }

  function courseMapKey(code: string, title: string): string {
    return `${normalizeCourseCode(code)}::${title.trim().toUpperCase()}`
  }

  async function hydrateForTerm(term: string) {
    try {
      const pairs = currentPairs(term)
      const hydrated = await hydrateScheduledCourses(pairs, term)
      setCurrentMap(hydrated, term)
    } catch {
      setCurrentMap({}, term)
    }
  }

  if (process.client) {
    onMounted(() => {
      const loadFromStorageForCurrentTerm = async () => {
        // Capture term at start to avoid race conditions with async operations
        const term = termId.value

        // Load localStorage data
        let localPairs: SchedulePair[] = []
        try {
          const raw = localStorage.getItem(keyFor(term))
          if (raw != null) {
            const parsed = JSON.parse(raw)
            const { pairs, migrated } = normalizePairsRaw(parsed, term)
            localPairs = pairs
            if (migrated) {
              try { localStorage.setItem(keyFor(term), JSON.stringify({ schedulesByTerm: { [term]: pairs } })) } catch {}
            }
          }
        } catch {}

        if (loggedIn.value) {
          try {
            const { entries: serverEntries, courses: serverCourses } = await fetchScheduleData(term)
            // Bail out if term changed during fetch
            if (termId.value !== term) return

            // Build pairs and course map from server-hydrated courses
            const serverPairs: SchedulePair[] = serverEntries.map((e) => ({
              code: normalizeCourseCode(e.courseId),
              sectionId: normalizeSectionId(String(e.sectionId)),
            })).filter((p) => p.code && p.sectionId)

            const courseMap: Record<string, UICourse> = {}
            for (const course of serverCourses) {
              const key = courseMapKey(course.code, course.title)
              courseMap[key] = course
            }

            // Merge pairs: server pairs + local-only pairs (by code+sectionId)
            const serverPairSet = new Set(serverPairs.map((p) => `${p.code}:${p.sectionId}`))
            const mergedPairs: SchedulePair[] = [...serverPairs]
            const localOnlyPairs: SchedulePair[] = []
            for (const p of localPairs) {
              const key = `${normalizeCourseCode(p.code)}:${normalizeSectionId(p.sectionId)}`
              if (!serverPairSet.has(key)) {
                mergedPairs.push(p)
                localOnlyPairs.push(p)
              }
            }

            setCurrentMap(courseMap, term)
            setCurrentPairs(mergedPairs, term)

            // If local had extra pairs, push merged set and re-hydrate
            if (localOnlyPairs.length > 0) {
              const entriesToPut = mergedPairs.map((p) => ({
                courseId: p.code,
                sectionId: parseInt(p.sectionId, 10),
              })).filter((e) => e.courseId && Number.isFinite(e.sectionId) && e.sectionId > 0)
              await enqueue(() => putScheduleEntries(term, entriesToPut))
              if (termId.value !== term) return
              await hydrateForTerm(term)
            }

            // Clear localStorage
            try { localStorage.removeItem(keyFor(term)) } catch {}
          } catch {
            setCurrentPairs(localPairs, term)
            await hydrateForTerm(term)
          }
        } else {
          setCurrentPairs(localPairs, term)
          await hydrateForTerm(term)
        }
      }

      const loadManualBlocks = () => {
        try {
          const raw = localStorage.getItem(manualKeyFor(termId.value))
          if (raw != null) {
            manualBlocks.value = normalizeManualBlocksRaw(JSON.parse(raw), termId.value)
          } else {
            manualBlocks.value = []
          }
        } catch {
          manualBlocks.value = []
        }
      }

      loadFromStorageForCurrentTerm()
      loadManualBlocks()

      watch(termId, () => {
        loadFromStorageForCurrentTerm()
        loadManualBlocks()
      })

      // For unauthenticated users: persist to localStorage and re-hydrate from batch endpoint.
      // For authenticated users: skip — map is managed directly by mutations + initial server load.
      watch(() => pairsByTerm.value[termId.value], (v) => {
        if (loggedIn.value) return
        const list = Array.isArray(v) ? v : []
        const normalized = list
          .map((p) => ({ code: normalizeCourseCode((p?.code || '').toString()), sectionId: normalizeSectionId((p?.sectionId || '').toString()) }))
          .filter((p) => p.code && p.sectionId)
        try { localStorage.setItem(keyFor(termId.value), JSON.stringify({ schedulesByTerm: { [termId.value]: normalized } })) } catch {}
        hydrateForTerm(termId.value)
      }, { deep: true })

      watch(manualBlocks, (v) => {
        const normalized = normalizeManualBlocksRaw(v as any, termId.value)
        try { localStorage.setItem(manualKeyFor(termId.value), JSON.stringify({ schedulesByTerm: { [termId.value]: normalized } })) } catch {}
      }, { deep: true })
    })
  }

  const scheduledCourses = computed<UICourse[]>(() => Object.values(currentMap()))

  const totalScheduledUnits = computed<number>(() => {
    try {
      let sum = 0
      for (const course of Object.values(currentMap())) {
        for (const section of course.sections || []) {
          const u = section.units.length > 0 ? Math.max(...section.units) : 0
          sum += Number.isFinite(u) ? u : 0
        }
      }
      return Number.isFinite(sum) ? sum : 0
    } catch {
      return 0
    }
  })

  const totalScheduledUnitsLabel = computed<string>(() => `${totalScheduledUnits.value.toFixed(1)} units`)

  function upsertScheduledSection(course: { code: string; title: string; description: string }, section: UICourseSection) {
    const code = normalizeCourseCode((course.code || '').toString())
    const sid = normalizeSectionId((section.sectionId || '').toString())
    if (!code || !sid) return
    const list = currentPairs()
    const exists = list.some((p) => normalizeCourseCode(p.code) === code && normalizeSectionId(p.sectionId) === sid)
    if (exists) return

    const prev = [...list]
    const prevMap = { ...currentMap() }

    // Optimistically update pairs
    setCurrentPairs([...list, { code, sectionId: sid }])

    // Optimistically update course map
    const key = courseMapKey(code, course.title)
    const map = { ...currentMap() }
    const existing = map[key] || {
      title: course.title,
      code,
      description: course.description,
      sections: [],
      ges: [],
    } as UICourse
    const uiSection: UICourseSection = {
      sectionId: sid,
      instructors: section.instructors || [],
      enrolled: section.enrolled ?? 0,
      capacity: section.capacity ?? 0,
      waitlisted: section.waitlisted ?? 0,
      schedules: section.schedules || [],
      hasDClearance: section.hasDClearance ?? false,
      hasPrerequisites: section.hasPrerequisites ?? false,
      hasDuplicatedCredit: section.hasDuplicatedCredit ?? false,
      units: section.units || [],
      type: section.type ?? null,
      isCancelled: section.isCancelled ?? false,
    }
    map[key] = { ...existing, sections: [...existing.sections, uiSection] }
    setCurrentMap(map)

    if (loggedIn.value) {
      const numericId = parseInt(sid, 10)
      if (Number.isFinite(numericId) && numericId > 0) {
        enqueue(async () => {
          try {
            await postScheduleEntry(termId.value, code, numericId)
          } catch (e) {
            console.error('Failed to add schedule section to server:', e)
            setCurrentPairs(prev)
            setCurrentMap(prevMap)
          }
        })
      }
    }
  }

  function hasScheduled(courseCode?: string | null, sectionId?: string | null): boolean {
    const code = normalizeCourseCode((courseCode || '').toString())
    if (!code) return false
    const sid = normalizeSectionId((sectionId || '').toString())
    const list = currentPairs()
    if (!sid) return list.some((p) => normalizeCourseCode(p.code) === code)
    return list.some((p) => normalizeCourseCode(p.code) === code && normalizeSectionId(p.sectionId) === sid)
  }

  function removeScheduledSection(courseCode?: string | null, sectionId?: string | null) {
    const code = normalizeCourseCode((courseCode || '').toString())
    if (!code) return
    const sid = normalizeSectionId((sectionId || '').toString())
    const list = currentPairs()
    const prev = [...list]
    const prevMap = { ...currentMap() }

    const removedPairs: SchedulePair[] = []
    const next = list.filter((p) => {
      const pc = normalizeCourseCode(p.code)
      const ps = normalizeSectionId(p.sectionId)
      if (pc !== code) return true
      if (!sid) { removedPairs.push(p); return false }
      if (ps === sid) { removedPairs.push(p); return false }
      return true
    })
    setCurrentPairs(next)

    // Optimistically update course map
    const map = { ...currentMap() }
    for (const [mapKey, mapCourse] of Object.entries(map)) {
      if (normalizeCourseCode(mapCourse.code) !== code) continue
      if (!sid) {
        delete map[mapKey]
      } else {
        const remaining = mapCourse.sections.filter((s) => normalizeSectionId(s.sectionId) !== sid)
        if (remaining.length === 0) {
          delete map[mapKey]
        } else {
          map[mapKey] = { ...mapCourse, sections: remaining }
        }
      }
    }
    setCurrentMap(map)

    if (loggedIn.value && removedPairs.length > 0) {
      if (sid) {
        const numericId = parseInt(sid, 10)
        if (Number.isFinite(numericId) && numericId > 0) {
          enqueue(async () => {
            try {
              await deleteScheduleEntry(termId.value, code, numericId)
            } catch (e) {
              console.error('Failed to remove schedule section from server:', e)
              setCurrentPairs(prev)
              setCurrentMap(prevMap)
            }
          })
        }
      } else {
        const remainingEntries = next.map((p) => ({
          courseId: normalizeCourseCode(p.code),
          sectionId: parseInt(normalizeSectionId(p.sectionId), 10),
        })).filter((e) => e.courseId && Number.isFinite(e.sectionId) && e.sectionId > 0)
        enqueue(async () => {
          try {
            await putScheduleEntries(termId.value, remainingEntries)
          } catch (e) {
            console.error('Failed to update schedule on server:', e)
            setCurrentPairs(prev)
            setCurrentMap(prevMap)
          }
        })
      }
    }
  }

  function checkScheduleCollision(input: Schedule[]): string[] {
    const inputBlocks = scheduleToBlocks(input)
    if (inputBlocks.length === 0) return []

    const scheduledBlocks: ScheduleBlock[] = []
    for (const course of Object.values(currentMap())) {
      for (const section of course.sections || []) {
        scheduledBlocks.push(...scheduleToBlocks(section.schedules, course.title, undefined, course.code, section.sectionId))
      }
    }

    const collidingCodes = new Set<string>()
    for (const a of inputBlocks) {
      for (const b of scheduledBlocks) {
        if (a.dayIndex !== b.dayIndex) continue
        const overlap = a.startMinutes < b.endMinutes && a.endMinutes > b.startMinutes
        if (overlap && b.courseCode) collidingCodes.add((b.courseCode || '').toString().trim().toUpperCase())
      }
    }
    return Array.from(collidingCodes)
  }

  return {
    byTerm,
    manualBlocks,
    scheduledCourses,
    upsertScheduledSection,
    hasScheduled,
    removeScheduledSection,
    totalScheduledUnits,
    totalScheduledUnitsLabel,
    checkScheduleCollision,
  }
})

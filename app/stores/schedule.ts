import { defineStore } from 'pinia'
import { computed, ref, watch, onMounted } from 'vue'
import { useTermId } from '@/composables/useTermId'
import type { UICourse, UICourseSection } from '@/composables/api/types'
import { parseBlocksFromApiSpec, parseBlocksFromString, type ScheduleBlock } from '@/composables/scheduleUtils'
import { ensureIndex, ensureIndexAsync, getAggregatedCourseDetails, getSectionDetailsIndexed } from '@/composables/api/indexer'
import { normalizeCourseCode, normalizeSectionId } from '@/utils/normalize'

export const useScheduleStore = defineStore('schedule', () => {
  // Hydrated in-memory map for UI consumption: { [termId]: { [COURSE_CODE]: UICourse } }
  const byTerm = ref<Record<string, Record<string, UICourse>>>({})
  // Persisted minimal state: pairs per term [{ code, sectionId }]
  const pairsByTerm = ref<Record<string, { code: string; sectionId: string }[]>>({})
  const { termId } = useTermId()

  function keyFor(term: string) { return `cx:schedule:${term}` }

  function normalizeCourseMapRaw(raw: unknown, term: string): Record<string, UICourse> {
    try {
      const obj: any = raw || {}
      // Unwrap { schedulesByTerm: { [term]: { ...courses } } }
      let map: any = (obj && obj.schedulesByTerm && obj.schedulesByTerm[term]) ? obj.schedulesByTerm[term] : obj
      // If the map itself accidentally contains a nested schedulesByTerm, unwrap again
      if (map && map.schedulesByTerm && map.schedulesByTerm[term]) {
        map = map.schedulesByTerm[term]
      }
      // Defensive: strip accidental wrapper key from course map
      if (map && typeof map === 'object' && 'schedulesByTerm' in map) {
        const cloned = { ...(map as any) }
        delete (cloned as any).schedulesByTerm
        map = cloned
      }
      return (map && typeof map === 'object') ? map as Record<string, UICourse> : {}
    } catch {
      return {}
    }
  }

  function normalizePairsRaw(raw: unknown, term: string): { pairs: { code: string; sectionId: string }[]; migrated: boolean } {
    try {
      const obj: any = raw || {}
      // Unwrap { schedulesByTerm: { [term]: value } }
      let value: any = (obj && obj.schedulesByTerm && obj.schedulesByTerm[term]) ? obj.schedulesByTerm[term] : obj
      if (value && value.schedulesByTerm && value.schedulesByTerm[term]) {
        value = value.schedulesByTerm[term]
      }
      // Case 1: Already an array of pairs
      if (Array.isArray(value)) {
        const pairs = (value as any[]).map((v) => ({
          code: normalizeCourseCode((v?.code || '').toString()),
          sectionId: normalizeSectionId((v?.sectionId || '').toString()),
        })).filter((p) => p.code && p.sectionId)
        return { pairs, migrated: false }
      }
      // Case 2: Legacy map of UICourse objects -> extract all section pairs
      if (value && typeof value === 'object') {
        const pairs: { code: string; sectionId: string }[] = []
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
      // Fallback
      return { pairs: [], migrated: false }
    } catch {
      return { pairs: [], migrated: false }
    }
  }

  function currentMap(): Record<string, UICourse> {
    return byTerm.value[termId.value] || {}
  }

  function setCurrentMap(next: Record<string, UICourse>) {
    byTerm.value = { ...byTerm.value, [termId.value]: next }
  }

  function currentPairs(): { code: string; sectionId: string }[] {
    return pairsByTerm.value[termId.value] || []
  }

  function setCurrentPairs(next: { code: string; sectionId: string }[]) {
    pairsByTerm.value = { ...pairsByTerm.value, [termId.value]: next }
  }

  async function hydrateForCurrentTerm() {
    try {
      await ensureIndexAsync(termId.value)
    } catch {
      // If index fails to build, keep map empty
      setCurrentMap({})
      return
    }
    const pairs = currentPairs()
    if (!pairs || pairs.length === 0) {
      setCurrentMap({})
      return
    }
    const byCode: Record<string, UICourse> = {}
    // De-duplicate pairs
    const seen = new Set<string>()
    for (const raw of pairs) {
      const code = normalizeCourseCode((raw?.code || '').toString())
      const sid = normalizeSectionId((raw?.sectionId || '').toString())
      if (!code || !sid) continue
      const key = `${code}#${sid}`
      if (seen.has(key)) continue
      seen.add(key)
      const courseDetails = getAggregatedCourseDetails(code, termId.value)
      const sectionDetails = getSectionDetailsIndexed(code, sid, termId.value)
      if (!courseDetails || !sectionDetails) continue
      // Derive GE from index for parity with normal tiles
      const idx = ensureIndex(termId.value)
      const ge = Array.from(new Set(
        (idx.allUICourses || [])
          .filter((c) => normalizeCourseCode(c.code) === code)
          .flatMap((c) => (c.ge || []))
          .filter(Boolean)
      ))
      const existing = byCode[code] || {
        title: courseDetails.title,
        code: courseDetails.code,
        description: courseDetails.description,
        sections: [],
        ge,
      } as UICourse
      const section: UICourseSection = {
        sectionId: sid,
        instructors: Array.from(new Set(sectionDetails.instructors || [])),
        enrolled: Number(sectionDetails.enrolled || 0),
        capacity: Number(sectionDetails.capacity || 0),
        schedule: (sectionDetails.times || [])[0] || '',
        location: (sectionDetails.locations || [])[0] || '',
        hasDClearance: !!sectionDetails.dClearance,
        hasPrerequisites: !!(sectionDetails.prerequisites && sectionDetails.prerequisites.length > 0),
        hasDuplicatedCredit: !!(sectionDetails.duplicatedCredits && sectionDetails.duplicatedCredits.length > 0),
        units: sectionDetails.units ?? null,
        type: sectionDetails.type ?? null,
      }
      const nextGe = Array.from(new Set([...(existing.ge || []), ...ge]))
      existing.sections = [...(existing.sections || []).filter((s) => s.sectionId !== sid), section]
      byCode[code] = { ...existing, ge: nextGe }
    }
    setCurrentMap(byCode)
  }

  if (process.client) {
    onMounted(() => {
      const loadFromStorageForCurrentTerm = async () => {
        try {
          const raw = localStorage.getItem(keyFor(termId.value))
          if (raw != null) {
            const parsed = JSON.parse(raw)
            // Prefer pairs; migrate if legacy map detected
            const { pairs, migrated } = normalizePairsRaw(parsed, termId.value)
            setCurrentPairs(pairs)
            if (migrated) {
              try { localStorage.setItem(keyFor(termId.value), JSON.stringify({ schedulesByTerm: { [termId.value]: pairs } })) } catch {}
            }
          }
        } catch {}
        await hydrateForCurrentTerm()
      }

      loadFromStorageForCurrentTerm()

      // Reload from storage whenever the term changes (client-side navigation)
      watch(termId, () => {
        loadFromStorageForCurrentTerm()
      })
      watch(() => pairsByTerm.value[termId.value], (v) => {
        // Persist pairs per term
        const list = Array.isArray(v) ? v : []
        const normalized = list
          .map((p) => ({ code: normalizeCourseCode((p?.code || '').toString()), sectionId: normalizeSectionId((p?.sectionId || '').toString()) }))
          .filter((p) => p.code && p.sectionId)
        try { localStorage.setItem(keyFor(termId.value), JSON.stringify({ schedulesByTerm: { [termId.value]: normalized } })) } catch {}
        // Re-hydrate UI map from pairs
        hydrateForCurrentTerm()
      }, { deep: true })
    })
  }

  const scheduledCourses = computed<UICourse[]>(() => Object.values(currentMap() || {}))

  const totalScheduledUnits = computed<number>(() => {
    try {
      let sum = 0
      for (const course of Object.values(currentMap() || {})) {
        for (const section of course.sections || []) {
          const u = typeof section.units === 'number' ? section.units : 0
          sum += Number.isFinite(u) ? u : 0
        }
      }
      return Number.isFinite(sum) ? sum : 0
    } catch {
      return 0
    }
  })

  const totalScheduledUnitsLabel = computed<string>(() => `${totalScheduledUnits.value.toFixed(1)} credits`)

  function upsertScheduledSection(course: { code: string; title: string; description: string }, section: UICourseSection) {
    const code = normalizeCourseCode((course.code || '').toString())
    const sid = normalizeSectionId((section.sectionId || '').toString())
    if (!code || !sid) return
    const list = currentPairs()
    const exists = list.some((p) => normalizeCourseCode(p.code) === code && normalizeSectionId(p.sectionId) === sid)
    if (exists) return
    setCurrentPairs([...list, { code, sectionId: sid }])
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
    const next = list.filter((p) => {
      const pc = normalizeCourseCode(p.code)
      const ps = normalizeSectionId(p.sectionId)
      if (pc !== code) return true
      if (!sid) return false // remove all pairs for this course
      return ps !== sid
    })
    setCurrentPairs(next)
  }

  function checkScheduleCollision(spec: string): string[] {
    const inputBlocksRaw: ScheduleBlock[] = (() => {
      const color = undefined
      const label = undefined
      let parsed = parseBlocksFromString(spec, label, color)
      if (!parsed || parsed.length === 0) parsed = parseBlocksFromApiSpec(spec, label, color)
      return parsed
    })()
    if (!inputBlocksRaw || inputBlocksRaw.length === 0) return []

    const scheduledBlocks: ScheduleBlock[] = []
    for (const course of Object.values(currentMap() || {})) {
      for (const section of course.sections || []) {
        const blocks = parseBlocksFromApiSpec((section.schedule || '').toString(), course.title, undefined, course.code, section.sectionId)
        scheduledBlocks.push(...blocks)
      }
    }

    const collidingCodes = new Set<string>()
    for (const a of inputBlocksRaw) {
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
    scheduledCourses,
    upsertScheduledSection,
    hasScheduled,
    removeScheduledSection,
    totalScheduledUnits,
    totalScheduledUnitsLabel,
    checkScheduleCollision,
  }
})

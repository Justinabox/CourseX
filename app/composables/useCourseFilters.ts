import { computed, reactive, watch } from 'vue'
import type { Ref } from 'vue'
import type { UICourse, UICourseSection } from '@/composables/useAPI'
import type { CourseFiltersState, TriState, EnrollmentFilter } from '@/composables/api/types'
import { useStore } from '@/composables/useStore'
import { useTermId } from '@/composables/useTermId'
import { parseUnitsToNumber } from '@/composables/filters/units'
import { normalizeString, normalizeSectionType } from '@/composables/filters/normalize'
import { courseMatchesSearch } from '@/composables/filters/search'
import { sectionMatchesEnrollment } from '@/composables/filters/enrollment'
import { sectionMatchesScheduleFilters, sectionMatchesTriState } from '@/composables/filters/schedule'

function extractCourseLevel(code: string | null | undefined): number | null {
  const m = (code || '').toString().match(/(\d{3})/)
  const levelStr = (m && m[1]) ? m[1].toString() : ''
  if (!levelStr) return null
  return parseInt(levelStr, 10)
}

function sectionMatchesTypes(section: UICourseSection, types: Set<string>): boolean {
  if (!types || types.size === 0) return true
  const normalized = normalizeSectionType(section.type)
  // Direct match against normalized canonical type
  if (normalized && types.has(normalized)) return true
  // Gracefully handle composite labels like "lecture/lab" or "lec & lab"
  const raw = normalizeString(section.type as any)
  if (!raw) return false
  if (types.has('lecture') && /\blec(ture)?\b/.test(raw)) return true
  if (types.has('lab') && /\blab\b/.test(raw)) return true
  if (types.has('discussion') && /\b(dis(cussion)?|disc)\b/.test(raw)) return true
  return false
}

function someSectionMatches(
  course: UICourse,
  predicate: (section: UICourseSection) => boolean
): boolean {
  for (const s of course.sections || []) {
    if (predicate(s)) return true
  }
  return false
}

export function useCourseFilters(courses?: Ref<UICourse[]>) {
  const { checkScheduleCollision } = useStore()
  const { termId } = useTermId()

  const filters = reactive<CourseFiltersState>({
    searchText: '',
    days: [],
    timeStartMinutes: null,
    timeEndMinutes: null,
    unitsMin: null,
    unitsMax: null,
    courseLevelMin: null,
    courseLevelMax: null,
    dClearance: 'any',
    prerequisites: 'any',
    duplicatedCredit: 'any',
    conflicts: 'any',
    enrollment: 'any',
    sectionTypes: [],
  })

  const compileSectionPredicate = () => {
    const typesSet = new Set((filters.sectionTypes || []).map((t) => normalizeSectionType(t)))
    const hasUnitsFilter = filters.unitsMin != null || filters.unitsMax != null

    // Memoize expensive checks by schedule spec
    const conflictMemo = new Map<string, boolean>()
    const scheduleMemo = new Map<string, boolean>()

    const hasConflict = (sec: UICourseSection): boolean => {
      if (filters.conflicts === 'any') return false
      const spec = (sec.schedule || '').toString()
      if (!spec) return false
      const cached = conflictMemo.get(spec)
      if (cached != null) return cached
      const v = checkScheduleCollision(spec).length > 0
      conflictMemo.set(spec, v)
      return v
    }

    const matchesSchedule = (sec: UICourseSection): boolean => {
      const spec = (sec.schedule || '').toString()
      const key = spec
      const cached = scheduleMemo.get(key)
      if (cached != null) return cached
      const v = sectionMatchesScheduleFilters(sec, filters.days, filters.timeStartMinutes, filters.timeEndMinutes)
      scheduleMemo.set(key, v)
      return v
    }

    return (sec: UICourseSection): boolean => {
      if (!matchesSchedule(sec)) return false
      if (!sectionMatchesTypes(sec, typesSet)) return false
      if (!sectionMatchesTriState(sec.hasDClearance, filters.dClearance)) return false
      if (!sectionMatchesTriState(sec.hasPrerequisites, filters.prerequisites)) return false
      if (!sectionMatchesTriState(sec.hasDuplicatedCredit, filters.duplicatedCredit)) return false
      if (!sectionMatchesEnrollment(sec, filters.enrollment)) return false
      if (filters.conflicts !== 'any') {
        const conf = hasConflict(sec)
        if (filters.conflicts === 'only' && !conf) return false
        if (filters.conflicts === 'exclude' && conf) return false
      }
      if (hasUnitsFilter) {
        const u = parseUnitsToNumber(sec.units)
        if (filters.unitsMin != null && u < filters.unitsMin) return false
        if (filters.unitsMax != null && u > filters.unitsMax) return false
      }
      return true
    }
  }

  const filterSectionsForCourse = (course: UICourse): UICourseSection[] => {
    const sections = course.sections || []
    const hasUnitsFilter = filters.unitsMin != null || filters.unitsMax != null

    // If units filter is active, require at least one section to satisfy it (coarse pre-check)
    if (hasUnitsFilter) {
      const anyUnitMatch = sections.some((s) => {
        const u = parseUnitsToNumber(s.units)
        if (filters.unitsMin != null && u < filters.unitsMin) return false
        if (filters.unitsMax != null && u > filters.unitsMax) return false
        return true
      })
      if (!anyUnitMatch) return []
    }

    const isLecture = (s: UICourseSection) => normalizeSectionType(s.type) === 'lecture'
    const isSpecial = (s: UICourseSection) => {
      const t = normalizeSectionType(s.type)
      return t === 'lab' || t === 'discussion'
    }
    const sectionPasses = compileSectionPredicate()

    // Gate by lecture only: find lectures that satisfy all active predicates
    const lecturesPassing: UICourseSection[] = sections.filter((s) => isLecture(s) && sectionPasses(s))
    if (lecturesPassing.length === 0) return []

    // If any lecture passes, include all labs/discussions regardless of conflicts or schedule
    const specialsIncluded: UICourseSection[] = sections.filter((s) => isSpecial(s))

    // Include only the passing lectures (do not include other non-special types here)
    return [...lecturesPassing, ...specialsIncluded]
  }

  const courseMatchesLevel = (course: UICourse, min: number | null, max: number | null): boolean => {
    if (min == null && max == null) return true
    const lvl = extractCourseLevel(course.code)
    if (lvl == null) return false
    if (min != null && lvl < min) return false
    if (max != null && lvl > max) return false
    return true
  }

  const apply = (list: UICourse[]): UICourse[] => {
    if (!list || list.length === 0) return []
    const out: UICourse[] = []
    for (const course of list) {
      if (!courseMatchesSearch(course, filters.searchText)) continue
      if (!courseMatchesLevel(course, filters.courseLevelMin, filters.courseLevelMax)) continue
      const passingSections = filterSectionsForCourse(course)
      if (passingSections.length === 0) continue
      out.push({ ...course, sections: passingSections })
    }
    return out
  }

  const filteredCourses = computed<UICourse[]>(() => {
    if (!courses) return []
    return apply(courses.value || [])
  })

  // Reset filters when term changes (prevents stale conflicts cache)
  watch(() => termId.value, () => {
    reset()
  })

  const reset = () => {
    filters.searchText = ''
    filters.days = []
    filters.timeStartMinutes = null
    filters.timeEndMinutes = null
    filters.unitsMin = null
    filters.unitsMax = null
    filters.courseLevelMin = null
    filters.courseLevelMax = null
    filters.dClearance = 'any'
    filters.prerequisites = 'any'
    filters.duplicatedCredit = 'any'
    filters.conflicts = 'any'
    filters.enrollment = 'any'
    filters.sectionTypes = []
  }

  return {
    filters,
    filteredCourses,
    applyFilters: apply,
    reset,
  }
}



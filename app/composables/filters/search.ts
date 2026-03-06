import type { UICourse } from '@/composables/api/types'
import { normalizeString } from './normalize'
import { formatSchedulesAsString } from '@/composables/api/transforms'
import { formatUnitsRange } from '@/composables/api/transforms'

export function courseMatchesSearch(course: UICourse, search: string): boolean {
  const s = normalizeString(search)
  if (!s) return true
  const sectionStrings = (course.sections || [])
    .flatMap((sec) => [
      sec.sectionId,
      ...sec.instructors,
      formatSchedulesAsString(sec.schedules),
      formatUnitsRange(sec.units),
      sec.type ?? '',
    ])
    .filter(Boolean)
  const geLetters = Array.from(new Set(course.ges || [])).filter(Boolean) as string[]
  const geTokens = geLetters.flatMap((g) => [
    `GE-${g}`,
    `GE ${g}`,
  ])
  const isGESM = (course.code || '').toUpperCase().startsWith('GESM')
  const gesmTokens = isGESM ? ['GESM', 'GESM-'] : []
  const haystack = [course.title, course.code, course.description, ...geTokens, ...gesmTokens, ...sectionStrings]
    .join(' ')
    .toLowerCase()
  return haystack.includes(s)
}

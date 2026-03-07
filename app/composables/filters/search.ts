import type { UICourse } from '@/composables/api/types'
import { normalizeString } from './normalize'
import { formatSchedulesAsString, formatUnitsRange } from '@/composables/api/transforms'

function codeVariants(code: string): string[] {
  // "ACAD-320" → ["ACAD-320", "ACAD 320", "ACAD320"]
  const m = code.match(/^([A-Za-z]+)[- ]?(\d+\w*)$/)
  if (!m) return [code]
  const [, prefix, number] = m
  return [`${prefix}-${number}`, `${prefix} ${number}`, `${prefix}${number}`]
}

function geVariants(ges: string[]): string[] {
  const letters = Array.from(new Set(ges)).filter(Boolean).sort()
  if (letters.length === 0) return []
  // Individual: "GE-A", "GE A" for each letter
  const individual = letters.flatMap((g) => [`GE-${g}`, `GE ${g}`])
  // Combined (for multi-GE like A+H displayed as "AH"): "GE-AH", "GE AH", "GEAH"
  if (letters.length > 1) {
    const combined = letters.join('')
    individual.push(`GE-${combined}`, `GE ${combined}`, `GE${combined}`)
  }
  return individual
}

export function buildSearchHaystack(course: UICourse): string {
  const sectionStrings = (course.sections || [])
    .flatMap((sec) => [
      sec.sectionId,
      ...sec.instructors,
      formatSchedulesAsString(sec.schedules),
      formatUnitsRange(sec.units),
      sec.type ?? '',
    ])
    .filter(Boolean)

  const codeTokens = codeVariants(course.code || '')
  const geTokens = geVariants(course.ges || [])

  return [course.title, ...codeTokens, course.description, ...geTokens, ...sectionStrings]
    .join(' ')
    .toLowerCase()
}

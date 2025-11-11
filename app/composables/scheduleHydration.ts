import type { UICourse, UICourseSection, SchedulePair } from '@/composables/api/types'
import { ensureIndex, ensureIndexAsync, getSectionDetailsIndexed } from '@/composables/api/indexer'
import { normalizeCourseCode, normalizeSectionId } from '@/utils/normalize'

export async function hydrateScheduledCourses(pairs: SchedulePair[], termId: string): Promise<Record<string, UICourse>> {
  await ensureIndexAsync(termId)
  const idx = ensureIndex(termId)
  const byKey: Record<string, UICourse> = {}
  const seen = new Set<string>()
  for (const raw of pairs || []) {
    const code = normalizeCourseCode((raw?.code || '').toString())
    const sid = normalizeSectionId((raw?.sectionId || '').toString())
    if (!code || !sid) continue
    const dedupeKey = `${code}#${sid}`
    if (seen.has(dedupeKey)) continue
    seen.add(dedupeKey)

    const sectionDetails = getSectionDetailsIndexed(code, sid, termId)
    if (!sectionDetails) continue

    const title = (sectionDetails.title || '').toString().trim()
    const titleUpper = title.toUpperCase()
    const key = `${code}::${titleUpper}`

    const ge = Array.from(new Set(
      (idx.allUICourses || [])
        .filter((c) => normalizeCourseCode(c.code) === code && (c.title || '').toString().trim().toUpperCase() === titleUpper)
        .flatMap((c) => (c.ge || []))
        .filter(Boolean)
    ))

    const existing = byKey[key] || {
      title,
      code: sectionDetails.code,
      description: sectionDetails.description,
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
    byKey[key] = { ...existing, ge: nextGe }
  }
  return byKey
}



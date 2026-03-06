import type { UICourse, UICourseSection, SchedulePair, CourseDetails } from '@/composables/api/types'
import { getSectionDetailsBatch } from '@/composables/api/queries'
import { normalizeCourseCode, normalizeSectionId } from '@/utils/normalize'

export async function hydrateScheduledCourses(pairs: SchedulePair[], termId: string): Promise<Record<string, UICourse>> {
  const byKey: Record<string, UICourse> = {}
  if (!pairs || pairs.length === 0) return byKey

  // Deduplicate section IDs
  const sectionIdSet = new Set<string>()
  for (const raw of pairs) {
    const sid = normalizeSectionId((raw?.sectionId || '').toString())
    if (sid) sectionIdSet.add(sid)
  }
  const uniqueSectionIds = Array.from(sectionIdSet)
  if (uniqueSectionIds.length === 0) return byKey

  // Single batch API call
  const details = await getSectionDetailsBatch(uniqueSectionIds)

  // Build lookup by sectionId
  const detailsBySectionId = new Map<string, CourseDetails>()
  for (const d of details) {
    if (d.sectionId) detailsBySectionId.set(d.sectionId, d)
  }

  // Build course map from pairs
  const seen = new Set<string>()
  for (const raw of pairs) {
    const code = normalizeCourseCode((raw?.code || '').toString())
    const sid = normalizeSectionId((raw?.sectionId || '').toString())
    if (!code || !sid) continue
    const dedupeKey = `${code}#${sid}`
    if (seen.has(dedupeKey)) continue
    seen.add(dedupeKey)

    const sectionDetails = detailsBySectionId.get(sid)
    if (!sectionDetails) continue

    const title = (sectionDetails.title || '').toString().trim()
    const titleUpper = title.toUpperCase()
    const key = `${code}::${titleUpper}`

    const existing = byKey[key] || {
      title,
      code: sectionDetails.code,
      description: sectionDetails.description,
      sections: [],
      ges: sectionDetails.ges || [],
    } as UICourse

    const section: UICourseSection = {
      sectionId: sid,
      instructors: Array.from(new Set(sectionDetails.instructors || [])),
      enrolled: Number(sectionDetails.enrolled || 0),
      capacity: Number(sectionDetails.capacity || 0),
      waitlisted: sectionDetails.waitlisted ?? 0,
      schedules: sectionDetails.schedules,
      hasDClearance: !!sectionDetails.dClearance,
      hasPrerequisites: sectionDetails.prerequisites.length > 0,
      hasDuplicatedCredit: !!sectionDetails.dupeCreditComment,
      units: sectionDetails.units,
      type: sectionDetails.type ?? null,
      isCancelled: sectionDetails.isCancelled ?? false,
    }

    existing.sections = [...(existing.sections || []).filter((s) => s.sectionId !== sid), section]
    byKey[key] = existing
  }
  return byKey
}

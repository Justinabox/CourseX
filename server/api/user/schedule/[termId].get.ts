import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { getSchedulePairs, querySectionsBatch, hydrateScheduleCourses } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const entries = await getSchedulePairs(userId, termId)
  let courses: any[] = []
  if (entries.length > 0) {
    const sectionIds = [...new Set(entries.map((e) => e.sectionId))]
    const sectionDetails = await querySectionsBatch(termId, sectionIds)

    // Filter out crosslisted duplicates: only keep details matching stored pairs
    const pairSet = new Set(entries.map((e) => `${e.courseId}:${e.sectionId}`))
    const filtered = sectionDetails.filter((d: any) => pairSet.has(`${d.code}:${d.sectionId}`))

    courses = hydrateScheduleCourses(filtered)
  }
  return { entries: entries.map((e) => ({ courseId: e.courseId, sectionId: e.sectionId })), courses }
})

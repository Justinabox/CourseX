import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { getScheduleSectionIds, querySectionsBatch, hydrateScheduleCourses } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const sectionIds = await getScheduleSectionIds(userId, termId)
  let courses: any[] = []
  if (sectionIds.length > 0) {
    const sectionDetails = await querySectionsBatch(termId, sectionIds)
    courses = hydrateScheduleCourses(sectionDetails)
  }
  return { sectionIds, courses }
})

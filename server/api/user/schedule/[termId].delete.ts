import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { removeSchedulePair } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const query = getQuery(event)
  const courseId = String(query.courseId || '').trim()
  const sectionId = Number(query.sectionId)
  if (!courseId) {
    throw createError({ statusCode: 400, statusMessage: 'Query must contain courseId: non-empty string' })
  }
  if (!Number.isInteger(sectionId) || sectionId <= 0 || sectionId >= 2147483647) {
    throw createError({ statusCode: 400, statusMessage: 'Query must contain sectionId: positive integer' })
  }

  const entries = await removeSchedulePair(userId, termId, courseId, sectionId)
  return { entries }
})

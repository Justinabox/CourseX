import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { getWatchlistKeys, queryWatchlistCourses } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const keys = await getWatchlistKeys(userId, termId)
  const courses = keys.length > 0 ? await queryWatchlistCourses(termId, keys) : []
  return { keys, courses }
})

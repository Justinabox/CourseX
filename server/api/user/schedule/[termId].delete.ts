import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { removeScheduleSectionId } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const query = getQuery(event)
  const sectionId = Number(query.sectionId)
  if (typeof sectionId !== 'number' || !Number.isInteger(sectionId) || sectionId <= 0 || sectionId >= 2147483647) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain sectionId: positive integer' })
  }

  const sectionIds = await removeScheduleSectionId(userId, termId, sectionId)
  return { sectionIds }
})

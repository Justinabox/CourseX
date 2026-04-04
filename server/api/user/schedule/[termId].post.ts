import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { addSchedulePair } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const body = await readBody(event)
  const { courseId, sectionId } = body || {}
  if (typeof courseId !== 'string' || !courseId.trim()) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain courseId: non-empty string' })
  }
  if (typeof sectionId !== 'number' || !Number.isInteger(sectionId) || sectionId <= 0 || sectionId >= 2147483647) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain sectionId: positive integer' })
  }

  const entries = await addSchedulePair(userId, termId, courseId.trim(), sectionId)
  return { entries }
})

import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { replaceSchedule } from '~~/server/db/queries'

const MAX_SCHEDULE_SIZE = 500

function isValidSectionId(v: unknown): v is number {
  return typeof v === 'number' && Number.isInteger(v) && v > 0 && v < 2147483647
}

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const body = await readBody(event)
  if (!body || !Array.isArray(body.sectionIds)) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain sectionIds: number[]' })
  }
  if (body.sectionIds.length > MAX_SCHEDULE_SIZE) {
    throw createError({ statusCode: 400, statusMessage: `Maximum ${MAX_SCHEDULE_SIZE} schedule items allowed` })
  }
  for (const id of body.sectionIds) {
    if (!isValidSectionId(id)) {
      throw createError({ statusCode: 400, statusMessage: 'Each sectionId must be a positive integer' })
    }
  }

  const sectionIds = await replaceSchedule(userId, termId, body.sectionIds)
  return { sectionIds }
})

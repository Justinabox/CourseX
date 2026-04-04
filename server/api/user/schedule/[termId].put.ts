import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { replaceSchedulePairs } from '~~/server/db/queries'

const MAX_SCHEDULE_SIZE = 500

function isValidEntry(v: unknown): v is { courseId: string; sectionId: number } {
  if (!v || typeof v !== 'object') return false
  const e = v as any
  return typeof e.courseId === 'string' && e.courseId.trim().length > 0
    && typeof e.sectionId === 'number' && Number.isInteger(e.sectionId) && e.sectionId > 0 && e.sectionId < 2147483647
}

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const body = await readBody(event)
  if (!body || !Array.isArray(body.entries)) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain entries: {courseId, sectionId}[]' })
  }
  if (body.entries.length > MAX_SCHEDULE_SIZE) {
    throw createError({ statusCode: 400, statusMessage: `Maximum ${MAX_SCHEDULE_SIZE} schedule items allowed` })
  }
  for (const entry of body.entries) {
    if (!isValidEntry(entry)) {
      throw createError({ statusCode: 400, statusMessage: 'Each entry must have courseId (string) and sectionId (positive integer)' })
    }
  }

  const entries = await replaceSchedulePairs(userId, termId, body.entries)
  return { entries }
})

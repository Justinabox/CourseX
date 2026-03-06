import { querySectionsBatch } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const body = await readBody<{ sectionIds?: string[] }>(event)

  if (!Array.isArray(body?.sectionIds) || body.sectionIds.length === 0) {
    throw createError({ statusCode: 400, statusMessage: 'sectionIds must be a non-empty array' })
  }
  if (body.sectionIds.length > 50) {
    throw createError({ statusCode: 400, statusMessage: 'Maximum 50 section IDs per request' })
  }

  const numericIds = body.sectionIds.map((id) => parseInt(String(id), 10))
  if (numericIds.some((id) => Number.isNaN(id))) {
    throw createError({ statusCode: 400, statusMessage: 'All section IDs must be numeric' })
  }

  return querySectionsBatch(termId, numericIds)
})

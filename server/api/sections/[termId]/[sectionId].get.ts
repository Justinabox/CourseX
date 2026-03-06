import { querySectionDetail } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const sectionId = getRouterParam(event, 'sectionId')!
  if (!sectionId) throw createError({ statusCode: 400, statusMessage: 'Missing section ID' })

  const detail = await querySectionDetail(termId, sectionId)
  if (!detail) throw createError({ statusCode: 404, statusMessage: 'Section not found' })
  return detail
})

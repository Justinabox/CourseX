import { queryCoursesByProgram } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const programPrefix = decodeURIComponent(getRouterParam(event, 'programPrefix')!)
  if (!programPrefix) throw createError({ statusCode: 400, statusMessage: 'Missing program prefix' })

  return queryCoursesByProgram(termId, programPrefix)
})

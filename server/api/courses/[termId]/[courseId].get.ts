import { queryCourseDetail } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const courseId = decodeURIComponent(getRouterParam(event, 'courseId')!)
  if (!courseId) throw createError({ statusCode: 400, statusMessage: 'Missing course ID' })

  const detail = await queryCourseDetail(termId, courseId)
  if (!detail) throw createError({ statusCode: 404, statusMessage: 'Course not found' })
  return detail
})

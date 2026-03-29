import { queryCourseDetail, queryPreviousSyllabus } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const courseId = decodeURIComponent(getRouterParam(event, 'courseId')!)
  if (!courseId) throw createError({ statusCode: 400, statusMessage: 'Missing course ID' })

  const title = getQuery(event).title as string | undefined
  const detail = await queryCourseDetail(termId, courseId, title)
  if (!detail) throw createError({ statusCode: 404, statusMessage: 'Course not found' })

  let previousSyllabus: { termCode: number; filename: string } | null = null
  if (!detail.syllabus) {
    previousSyllabus = await queryPreviousSyllabus(
      detail.code,
      detail.title,
      parseInt(termId, 10)
    )
  }

  return { ...detail, previousSyllabus }
})

import { querySectionDetail, queryOtherSemesterSyllabus } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)
  const sectionId = getRouterParam(event, 'sectionId')!
  const courseId = (getQuery(event).courseId as string | undefined)?.trim()
  if (!sectionId) throw createError({ statusCode: 400, statusMessage: 'Missing section ID' })

  const detail = await querySectionDetail(termId, sectionId, courseId)
  if (!detail) throw createError({ statusCode: 404, statusMessage: 'Section not found' })

  let previousSyllabus: { termCode: number; filename: string } | null = null
  if (!detail.syllabus) {
    previousSyllabus = await queryOtherSemesterSyllabus(
      detail.code,
      detail.title,
      parseInt(termId, 10)
    )
  }

  return { ...detail, previousSyllabus }
})

import { queryCoursesByTerm } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'
import { getCachedCatalog, setCachedCatalog } from '~~/server/utils/catalogCache'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)

  let json = getCachedCatalog(termId)
  if (!json) {
    const courses = await queryCoursesByTerm(termId)
    json = setCachedCatalog(termId, JSON.stringify(courses))
  }

  setResponseHeader(event, 'Content-Type', 'application/json')
  return json
})

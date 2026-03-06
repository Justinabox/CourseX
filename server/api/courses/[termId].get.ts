import { queryCoursesByTerm } from '~~/server/db/queries'
import { validateTermCode } from '~~/server/utils/termValidator'
import { getCachedCatalog, setCachedCatalog } from '~~/server/utils/catalogCache'

export default defineEventHandler(async (event) => {
  const termId = validateTermCode(getRouterParam(event, 'termId')!)

  let compressed = getCachedCatalog(termId)
  if (!compressed) {
    const courses = await queryCoursesByTerm(termId)
    compressed = setCachedCatalog(termId, JSON.stringify(courses))
  }

  setResponseHeader(event, 'Content-Type', 'application/json')
  setResponseHeader(event, 'Content-Encoding', 'br')
  return compressed
})

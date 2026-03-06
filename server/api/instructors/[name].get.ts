import { queryInstructor } from '~~/server/db/queries'

export default defineEventHandler(async (event) => {
  const name = decodeURIComponent(getRouterParam(event, 'name')!)
  if (!name) throw createError({ statusCode: 400, statusMessage: 'Missing instructor name' })

  const instructor = await queryInstructor(name)
  if (!instructor) return null
  return instructor
})

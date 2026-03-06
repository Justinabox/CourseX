import { queryPrograms } from '~~/server/db/queries'

export default defineEventHandler(async () => {
  return queryPrograms()
})

import { queryTerms } from '~~/server/db/queries'

export default defineEventHandler(async () => {
  return queryTerms()
})

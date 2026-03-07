import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { removeWatchlistKey } from '~~/server/db/queries'

const WATCHLIST_KEY_RE = /^.+::.+$/

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const query = getQuery(event)
  if (typeof query.key !== 'string' || !WATCHLIST_KEY_RE.test(query.key)) {
    throw createError({ statusCode: 400, statusMessage: 'Query must contain key: string matching CODE::TITLE format' })
  }

  const keys = await removeWatchlistKey(userId, termId, query.key)
  return { keys }
})

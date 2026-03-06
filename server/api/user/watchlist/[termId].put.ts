import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { replaceWatchlist } from '~~/server/db/queries'

const MAX_WATCHLIST_SIZE = 500
const WATCHLIST_KEY_RE = /^.+::.+$/

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const body = await readBody(event)
  if (!body || !Array.isArray(body.keys)) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain keys: string[]' })
  }
  if (body.keys.length > MAX_WATCHLIST_SIZE) {
    throw createError({ statusCode: 400, statusMessage: `Maximum ${MAX_WATCHLIST_SIZE} watchlist items allowed` })
  }
  for (const key of body.keys) {
    if (typeof key !== 'string' || !WATCHLIST_KEY_RE.test(key)) {
      throw createError({ statusCode: 400, statusMessage: 'Each key must be a non-empty string matching CODE::TITLE format' })
    }
  }

  const keys = await replaceWatchlist(userId, termId, body.keys)
  return { keys }
})

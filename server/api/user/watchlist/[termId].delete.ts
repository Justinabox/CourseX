import { resolveUserId } from '~~/server/utils/resolveUserId'
import { validateTermCode } from '~~/server/utils/termValidator'
import { removeWatchlistKey } from '~~/server/db/queries'

const WATCHLIST_KEY_RE = /^.+::.+$/

export default defineEventHandler(async (event) => {
  const userId = await resolveUserId(event)
  const termId = validateTermCode(getRouterParam(event, 'termId') || '')

  const body = await readBody(event)
  if (!body || typeof body.key !== 'string' || !WATCHLIST_KEY_RE.test(body.key)) {
    throw createError({ statusCode: 400, statusMessage: 'Body must contain key: string matching CODE::TITLE format' })
  }

  const keys = await removeWatchlistKey(userId, termId, body.key)
  return { keys }
})

import type { H3Event } from 'h3'
import { getUserIdByGoogleId } from '~~/server/db/queries'

export async function resolveUserId(event: H3Event): Promise<number> {
  const session = await getUserSession(event)
  const googleId = session?.user?.googleId
  if (!googleId) {
    throw createError({ statusCode: 401, statusMessage: 'Not authenticated' })
  }

  const userId = await getUserIdByGoogleId(googleId)
  if (!userId) {
    throw createError({ statusCode: 401, statusMessage: 'User not found' })
  }

  return userId
}

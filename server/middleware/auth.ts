export default defineEventHandler(async (event) => {
  const path = getRequestURL(event).pathname

  // Allow auth-related routes and public endpoints
  if (path.startsWith('/api/_auth/') || path.startsWith('/auth/')) return
  if (path === '/api/terms' || path === '/api/programs') return

  // Protect all API routes
  if (path.startsWith('/api/')) {
    const session = await getUserSession(event)
    if (!session.user) {
      throw createError({ statusCode: 401, statusMessage: 'Unauthorized' })
    }
  }
})

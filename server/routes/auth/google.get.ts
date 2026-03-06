import { upsertUser } from '~~/server/db/queries'

export default defineOAuthGoogleEventHandler({
  config: {
    scope: ['openid', 'email', 'profile'],
  },
  async onSuccess(event, { user }) {
    const email: string = user.email || ''
    if (!email.endsWith('@usc.edu') && !/@[\w.-]+\.usc\.edu$/.test(email)) {
      return sendRedirect(event, '/login?error=unauthorized')
    }

    try {
      await upsertUser(user.sub, email, user.name || '', user.picture || null)
    }
    catch (err) {
      console.error('Failed to upsert user:', err)
      return sendRedirect(event, `/login?error=db`)
    }

    await setUserSession(event, {
      user: {
        googleId: user.sub,
        name: user.name,
        email: user.email,
        picture: user.picture,
      },
      loggedInAt: new Date(),
    })
    return sendRedirect(event, '/')
  },
  onError(event, error) {
    console.error('Google OAuth error:', error)
    const msg = encodeURIComponent(error instanceof Error ? error.message : String(error))
    return sendRedirect(event, `/login?error=oauth&message=${msg}`)
  },
})

interface Term { termCode: number; season: string; year: number; status: string }

export default defineNuxtRouteMiddleware(async (to) => {
  const path = to.path || ''
  if (!path.startsWith('/course')) return

  const parts = path.split('/')
  const maybeTerm = parts[2] || ''
  if (/^\d{5}$/.test(maybeTerm)) return

  const { data: terms } = await useAsyncData('terms', () => $fetch<Term[]>('/api/terms'), {
    default: () => [] as Term[],
  })

  const active = terms.value.find((t) => t.status === 'Active')
  const termCode = String(active?.termCode ?? terms.value[0]?.termCode ?? '')
  if (!termCode) return

  const rest = parts.slice(2).join('/')
  const next = ['/course', termCode, rest].filter(Boolean).join('/')
  return navigateTo(next, { redirectCode: 302 })
})



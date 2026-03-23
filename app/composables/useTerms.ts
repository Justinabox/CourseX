interface Term { termCode: number; season: string; year: number; status: string }

export function useTerms() {
  const { data: terms } = useAsyncData('terms', () => $fetch<Term[]>('/api/terms'), {
    default: () => [] as Term[],
  })

  const activeTermCode = computed(() => {
    const active = terms.value.find((t) => t.status === 'Active')
    return String(active?.termCode ?? terms.value[0]?.termCode ?? '')
  })

  function termLabel(termCode: number | string) {
    const t = terms.value.find((t) => String(t.termCode) === String(termCode))
    return t ? `${t.season} ${t.year}` : String(termCode)
  }

  return { terms, activeTermCode, termLabel }
}

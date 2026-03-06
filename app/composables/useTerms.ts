export function useTerms() {
  const { data: terms } = useAsyncData('terms', () => $fetch('/api/terms'), {
    default: () => [] as { termCode: number; season: string; year: number; status: string }[],
  })

  const activeTermCode = computed(() => {
    const active = terms.value.find((t) => t.status === 'Active')
    return String(active?.termCode ?? terms.value[0]?.termCode ?? '20261')
  })

  function termLabel(termCode: number | string) {
    const t = terms.value.find((t) => String(t.termCode) === String(termCode))
    return t ? `${t.season} ${t.year}` : String(termCode)
  }

  return { terms, activeTermCode, termLabel }
}

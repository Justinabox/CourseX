import { computed } from 'vue'
import { useTerms } from '@/composables/useTerms'

export function useTermId() {
  const route = useRoute()
  const { activeTermCode } = useTerms()
  const termId = computed(() => {
    const fromParams = (route.params as any)?.termId
    if (fromParams && /^\d{5}$/.test(String(fromParams))) return String(fromParams)
    const match = route.path.match(/^\/course\/(\d{5})/)
    if (match) return match[1]
    return activeTermCode.value
  })
  return { termId }
}

export function useTermNavigation() {
  const { termId } = useTermId()
  const toCoursePath = (...segments: string[]) => `/course/${termId.value}/${segments.join('/')}`
  return { termId, toCoursePath }
}



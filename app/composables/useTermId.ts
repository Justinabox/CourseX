import { computed } from 'vue'
import { useTerms } from '@/composables/useTerms'

export function useTermId() {
  const route = useRoute()
  const { activeTermCode } = useTerms()
  const termId = computed(() => String((route.params as any)?.termId || activeTermCode.value))
  return { termId }
}

export function useTermNavigation() {
  const { termId } = useTermId()
  const toCoursePath = (...segments: string[]) => `/course/${termId.value}/${segments.join('/')}`
  return { termId, toCoursePath }
}



import { computed } from 'vue'
import { useRouteMode } from '@/composables/useRouteMode'

export function useCourseSelection() {
  const { mode } = useRouteMode()
  const selectedCourseCode = computed(() => mode.value.mode !== 'unknown' ? (mode.value.courseCode ?? null) : null)
  const selectedSectionId = computed(() => mode.value.mode !== 'unknown' ? (mode.value.sectionId ?? null) : null)
  return { selectedCourseCode, selectedSectionId }
}

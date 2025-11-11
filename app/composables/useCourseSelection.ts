import { useStore } from '@/composables/useStore'

export function useCourseSelection() {
  const { selectedCourseCode, selectedSectionId } = useStore()

  const selectCourse = (code: string, sectionId: string | null = null) => {
    selectedCourseCode.value = code
    selectedSectionId.value = sectionId
  }

  const clearSelection = () => {
    selectedCourseCode.value = null
    selectedSectionId.value = null
  }

  return {
    selectedCourseCode,
    selectedSectionId,
    selectCourse,
    clearSelection,
  }
}


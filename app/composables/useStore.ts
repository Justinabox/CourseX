import { computed } from 'vue'
import type { UICourse, UICourseSection } from '@/composables/useAPI'
import { parseBlocksFromApiSpec, parseBlocksFromString, type ScheduleBlock } from '@/composables/scheduleUtils'
import { useUiStore } from '@/stores/ui'
import { useScheduleStore } from '@/stores/schedule'

type Initializer<T> = () => T

export function useStore() {
  // Legacy helper kept for compatibility; recommend migrating to Pinia directly
  function usePersistentState<T>(nuxtKey: string, storageKey: string, initialize: Initializer<T>) {
    // Delegate to Pinia where applicable, else fallback to simple ref initialized value
    const value = initialize()
    // No-op; persistence now handled in Pinia stores
    return useState<T>(nuxtKey, () => value)
  }

  const ui = useUiStore()
  const schedule = useScheduleStore()

  const selectedCourseCode = computed<string | null>({
    get: () => ui.selectedCourseCode,
    set: (v) => { ui.selectedCourseCode = v },
  })
  const selectedSectionId = computed<string | null>({
    get: () => ui.selectedSectionId,
    set: (v) => { ui.selectedSectionId = v },
  })

  const scheduledCourses = computed<UICourse[]>(() => schedule.scheduledCourses)

  const totalScheduledUnits = computed<number>(() => schedule.totalScheduledUnits)
  const totalScheduledUnitsLabel = computed<string>(() => schedule.totalScheduledUnitsLabel)

  function upsertScheduledSection(course: { code: string; title: string; description: string }, section: UICourseSection) {
    schedule.upsertScheduledSection(course, section)
  }

  function hasScheduled(courseCode?: string | null, sectionId?: string | null): boolean {
    return schedule.hasScheduled(courseCode, sectionId)
  }

  function removeScheduledSection(courseCode?: string | null, sectionId?: string | null) {
    schedule.removeScheduledSection(courseCode, sectionId)
  }

  function checkScheduleCollision(spec: string): string[] {
    // Use schedule store implementation
    return schedule.checkScheduleCollision(spec)
  }

  return {
    usePersistentState,
    selectedCourseCode,
    selectedSectionId,
    scheduledCourses,
    upsertScheduledSection,
    hasScheduled,
    removeScheduledSection,
    totalScheduledUnits,
    totalScheduledUnitsLabel,
    checkScheduleCollision,
  }
}


import { ref, watch, computed } from 'vue'
import type { Ref } from 'vue'
import { listAllCourses, getSchoolCourses, type UICourse } from '@/composables/useAPI'
import { useTermId } from '@/composables/useTermId'
import { useScheduleStore } from '@/stores/schedule'
import { useRouteMode, type RouteMode } from '@/composables/useRouteMode'
import { useWatchlistStore } from '@/stores/watchlist'

export function useCourseListSource() {
  const { scheduledCourses } = useScheduleStore()
  const { mode, scopeKey } = useRouteMode()
  const { termId } = useTermId()
  const watchlistStore = useWatchlistStore()

  const courses: Ref<UICourse[]> = ref([])

  const reload = async () => {
    const m = mode.value
    if (m.mode === 'all' || m.mode === 'unknown') {
      courses.value = await listAllCourses()
      return
    }
    if (m.mode === 'scheduled') {
      courses.value = [...scheduledCourses.value]
      return
    }
    if (m.mode === 'watchlist') {
      courses.value = [...watchlistStore.watchlistCourses]
      return
    }
    if (m.mode === 'program') {
      courses.value = await getSchoolCourses(m.school, m.program)
      return
    }
  }

  // Derive a stable "reload key" that only changes when we need to re-fetch the list,
  // not when the user selects a course/section within the same view.
  const reloadKey = computed(() => {
    const m = mode.value
    const t = termId.value
    if (m.mode === 'program') return `${t}:program:${m.school}:${m.program}`
    if (m.mode === 'scheduled') return `${t}:scheduled:${scheduledCourses.value.length}`
    if (m.mode === 'watchlist') return `${t}:watchlist:${watchlistStore.watchlistCourses.length}`
    return `${t}:${m.mode}`
  })

  watch(reloadKey, () => { reload() }, { immediate: true })

  return {
    courses,
    reload,
    mode,
    scopeKey,
  }
}

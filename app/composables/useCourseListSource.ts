import { ref, computed, watchEffect } from 'vue'
import type { Ref } from 'vue'
import { listAllCourses, getSchoolCourses, type UICourse } from '@/composables/useAPI'
import { useTermId } from '@/composables/useTermId'
import { useStore } from '@/composables/useStore'
import { useRouteMode, type RouteMode } from '@/composables/useRouteMode'

export function useCourseListSource() {
  const { scheduledCourses } = useStore()
  const { mode, scopeKey } = useRouteMode()
  const { termId } = useTermId()

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
    if (m.mode === 'program') {
      courses.value = await getSchoolCourses(m.school, m.program)
      return
    }
  }

  watchEffect(() => {
    const m = mode.value
    // react to scheduled list changes only when in scheduled mode
    const scheduleVersion = m.mode === 'scheduled' ? (scheduledCourses.value.length) : 0
    const t = termId.value
    void scheduleVersion // dependency capture
    void t
    reload()
  })

  // no secondary watcher needed; handled in watchEffect above

  return {
    courses,
    reload,
    mode,
    scopeKey,
  }
}



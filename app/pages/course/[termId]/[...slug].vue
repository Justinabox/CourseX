<template>
  <ThreePanelLayout />
</template>

<script setup lang="ts">
import ThreePanelLayout from '@/components/ThreePanelLayout.vue'

definePageMeta({
  keepalive: true,
  key: 'course-term'
})

const { mode } = useRouteMode()
const { selectCourse, clearSelection } = useCourseSelection()

watch(
  () => mode.value,
  (m) => {
    if (m.mode === 'unknown') {
      clearSelection()
      return
    }
    if (m.courseCode) {
      selectCourse(m.courseCode, m.sectionId || null)
    } else {
      clearSelection()
    }
  },
  { immediate: true, deep: true }
)

</script>

<style scoped>
</style>



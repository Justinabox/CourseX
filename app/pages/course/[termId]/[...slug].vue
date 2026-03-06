<template>
  <div class="w-full h-full flex">
    <LeftPanel />
    <MiddlePanel />
    <RightPanel />
  </div>
</template>

<script setup lang="ts">
definePageMeta({
  keepalive: true,
  key: 'course-term'
})

const { mode } = useRouteMode()
const ui = useUiStore()

watch(
  () => mode.value,
  (m) => {
    if (m.mode === 'unknown') {
      ui.clearSelection()
      return
    }
    if (m.courseCode) {
      ui.setSelection(m.courseCode, m.sectionId || null)
    } else {
      ui.clearSelection()
    }
  },
  { immediate: true, deep: true }
)

</script>

<style scoped>
</style>



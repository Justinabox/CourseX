<template>
  <ThreePanelLayout />
</template>

<script setup lang="ts">
import ThreePanelLayout from '@/components/ThreePanelLayout.vue'

definePageMeta({
  keepalive: true,
  key: 'course-legacy'
})

// This legacy page will be redirected by middleware to include a termId.
// Kept temporarily to avoid hard 404s if route registration still picks it up.
const router = useRouter()
onMounted(() => {
  const path = router.currentRoute.value.path || '/course'
  if (path.startsWith('/course/') && !/^\/course\/\d{5}\//.test(path + '/')) {
    const rest = path.replace(/^\/course\/?/, '')
    router.replace(`/course/20261/${rest}`)
  }
})
</script>

<style scoped>
</style>


<template>
  <ThreePanelLayout>
    <template #left><LeftPanel /></template>
    <template #middle><MiddlePanel /></template>
    <template #right><RightPanel /></template>
  </ThreePanelLayout>
</template>

<script setup lang="ts">
definePageMeta({
  keepalive: true,
  key: 'course-term'
})
</script>

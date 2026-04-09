<template>
  <div class="bg-cx-background-decorative min-w-fit w-screen h-screen p-6 text-cx-text-subtle bg-zebra text-center">
    <div class="bg-cx-background rounded-lg border border-cx-border w-full h-full max-w-[1376px] mx-auto text-left">
      <NuxtPage />
    </div>
    <span class="text-xs text-cx-text-subtle">Data is ~1 hour behind WebReg | Last updated: {{ dataTimestamp }}</span>
  </div>
</template>

<script setup lang="ts">
const { data: meta } = await useFetch('/api/meta')
const dataTimestamp = computed(() => {
  const ts = meta.value?.coursesLastSuccess
  if (!ts) return 'N/A'
  return new Date(ts).toLocaleString()
})
</script>

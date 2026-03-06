<template>
  <div class="w-full h-16 flex justify-between items-center">
    <div class="flex items-center gap-2 p-1 border-r border-cx-border shrink-0">
      <SettingsPopover />
    </div>
    <div class="w-full h-full flex flex-col items-center justify-center">
      <span class="text-sm text-cx-text-muted">Built with ❤️ by Korgo</span>
      <span class="text-[8px] text-cx-text-weak-muted">ver: <a href="https://github.com/MeloticZ/CourseX" class="underline hover:text-cx-text-secondary">{{ commitSha.slice(0, 7) }}</a> - data: 20260124 00:28 PST</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const runtimeConfig = useRuntimeConfig()
const commitSha = computed(() => runtimeConfig.public.WORKERS_CI_COMMIT_SHA || 'dev')
</script>

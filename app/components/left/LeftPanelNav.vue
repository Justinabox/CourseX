<template>
  <div class="w-full flex flex-col">
    <NuxtLink :to="`/course/${termId}/all`" class="w-full rounded-md flex items-center gap-2 hover:bg-cx-surface-800/40 px-1 py-1 rounded">
      <Icon name="uil:list-ul" class="h-5 w-5"/>
      <span class="text-md">All Courses</span>
    </NuxtLink>

    <NuxtLink :to="`/course/${termId}/watchlist`" class="w-full rounded-md flex justify-between items-center gap-2 hover:bg-cx-surface-800/40 px-1 py-1 rounded">
      <div class="flex gap-2 items-center shrink-0">
        <Icon name="uil:star" class="h-5 w-5"/>
        <span class="text-md">Watchlist</span>
      </div>
      <div class="text-xs font-semibold text-cx-text-weak-muted">
        {{ totalWatchlistCourses }}
      </div>
    </NuxtLink>

    <NuxtLink :to="`/course/${termId}/scheduled`" class="w-full rounded-md flex justify-between items-center gap-2 hover:bg-cx-surface-800/40 px-1 py-1 rounded">
      <div class="flex gap-2 items-center shrink-0">
        <Icon name="uil:calendar" class="h-5 w-5"/>
        <span class="text-md">Scheduled Courses</span>
      </div>
      <div class="text-xs font-semibold text-cx-text-weak-muted">
        <span class="lg:hidden">{{ totalScheduledUnits.toFixed(1) }}</span>
        <span class="hidden lg:inline truncate max-w-24">{{ totalScheduledUnitsLabel }}</span>
      </div>
    </NuxtLink>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useScheduleStore } from '@/stores/schedule'
import { useTermId } from '@/composables/useTermId'
import { useWatchlistStore } from '@/stores/watchlist'

const scheduleStore = useScheduleStore()
const { termId } = useTermId()
const watchlistStore = useWatchlistStore()
const totalWatchlistCourses = computed(() => watchlistStore.totalWatchlistCourses)
const totalScheduledUnits = computed(() => scheduleStore.totalScheduledUnits)
const totalScheduledUnitsLabel = computed(() => scheduleStore.totalScheduledUnitsLabel)
</script>

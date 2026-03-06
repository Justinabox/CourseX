<template>
  <button
    class="text-sm w-fit py-2 px-3 rounded-md bg-cx-surface-800 hover:bg-cx-surface-700"
    :class="{ 'text-cx-status-danger-icon/80 border-cx-status-danger-border border-1': isInSchedule, 'text-cx-text-subtle': !isInSchedule }"
    @click="onAddOrRemoveSchedule"
    @mouseenter="onHoverPreviewEnter"
    @mouseleave="onHoverPreviewLeave"
  >
    {{ isInSchedule ? 'Remove from Schedule' : 'Add to Schedule' }}
  </button>

  <button
    class="text-sm w-fit py-2 px-3 rounded-md bg-cx-surface-800 hover:bg-cx-surface-700"
    :class="{ 'text-cx-status-warning-icon/80 border-cx-status-warning-border border-1': isInWatchlist, 'text-cx-text-subtle': !isInWatchlist }"
    @click="onAddOrRemoveWatchlist"
  >
    {{ isInWatchlist ? 'Remove from Watchlist' : 'Add to Watchlist' }}
  </button>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { CourseDetails } from '@/composables/useAPI'
import { useSchedule } from '@/composables/useSchedule'
import { useScheduleStore } from '@/stores/schedule'
import { useWatchlistStore } from '@/stores/watchlist'
import { useCourseSelection } from '@/composables/useCourseSelection'

const props = defineProps<{
  details: CourseDetails
}>()

const { selectedSectionId } = useCourseSelection()
const { hasCourseSection, removeCourseSection, setHoverPreviewFromSchedules, clearHoverPreview } = useSchedule()
const { upsertScheduledSection } = useScheduleStore()
const watchlistStore = useWatchlistStore()

const isInSchedule = computed(() => {
  return hasCourseSection(props.details.code, selectedSectionId.value || null)
})

const isInWatchlist = computed(() => {
  return watchlistStore.hasInWatchlist(props.details.code, props.details.title)
})

function onAddOrRemoveSchedule() {
  clearHoverPreview()
  const sectionId = selectedSectionId.value || undefined
  if (isInSchedule.value) {
    removeCourseSection(props.details.code, sectionId)
    return
  }
  const section = {
    sectionId: (sectionId || 'COURSE') as string,
    instructors: Array.from(new Set(props.details.instructors || [])),
    enrolled: Number(props.details.enrolled || 0),
    capacity: Number(props.details.capacity || 0),
    waitlisted: props.details.waitlisted ?? 0,
    schedules: props.details.schedules,
    hasDClearance: !!props.details.dClearance,
    hasPrerequisites: props.details.prerequisites.length > 0,
    hasDuplicatedCredit: !!props.details.dupeCreditComment,
    units: props.details.units,
    type: props.details.type ?? null,
    isCancelled: props.details.isCancelled ?? false,
  }
  upsertScheduledSection({ code: props.details.code, title: props.details.title, description: props.details.description }, section)
}

function onHoverPreviewEnter() {
  try {
    if (props.details.schedules.length === 0) return
    setHoverPreviewFromSchedules(props.details.schedules, props.details.title, props.details.code)
  } catch {}
}

function onHoverPreviewLeave() { clearHoverPreview() }

function onAddOrRemoveWatchlist() {
  const { code, title, description, ges } = props.details
  if (isInWatchlist.value) {
    watchlistStore.removeFromWatchlist(code, title)
  } else {
    watchlistStore.upsertWatchlistItem(code, title, {
      title, code, description, sections: [], ges,
    })
  }
}
</script>

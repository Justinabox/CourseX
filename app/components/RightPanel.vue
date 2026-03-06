<template>
  <div class="w-full h-full flex flex-col justify-between">

    <div class="gap-2 flex flex-col p-4 h-full overflow-y-hidden">
      <div v-if="details">
        <div class="flex items-center gap-2">
          <span class="text-sm text-cx-text-subtle">Course Details</span>
          <span class="text-sm text-cx-text-subtle">{{ details.code }}</span>
        </div>
        <h1 class="text-2xl font-semibold">{{ details.title }}</h1>
      </div>

      <div class="flex flex-col gap-2 overflow-y-scroll h-full">
        <span v-if="details" class="text-sm text-cx-text-muted">
          {{ details.description || 'No description available.' }}
        </span>

        <button v-if="details" class="text-sm w-fit p-2 rounded-md mt-1 mb-2 bg-cx-surface-800 hover:bg-cx-surface-700" :class="{ 'text-rose-500/80 border-rose-700/50 border-1': isInSchedule, 'text-cx-text-subtle': !isInSchedule }" @click="onAddOrRemove" @mouseenter="onHoverPreviewEnter" @mouseleave="onHoverPreviewLeave">
          {{ isInSchedule ? 'Remove from Schedule' : 'Add to Schedule' }}
        </button>

        <button v-if="details" class="text-sm w-fit p-2 rounded-md mb-2 bg-cx-surface-800 hover:bg-cx-surface-700" :class="{ 'text-yellow-500/80 border-yellow-700/50 border-1': isInWatchlist, 'text-cx-text-subtle': !isInWatchlist }" @click="onAddOrRemoveWatchlist">
          {{ isInWatchlist ? 'Remove from Watchlist' : 'Add to Watchlist' }}
        </button>

        <div v-if="details" class="flex flex-col gap-1.5 border-y border-cx-border py-4">
          <div class="flex items-center gap-2">
            <Icon name="uil:graduation-cap" class="h-5 w-5 text-cx-text-muted"/>
            <div class="flex flex-wrap items-center">
              <template v-if="instructorViews.length > 0">
                <template v-for="item in instructorViews" :key="item.name">
                  <a
                    :href="item.link"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="text-sm underline decoration-1 decoration-dashed hover:text-cx-text-muted"
                    :class="{ 'text-rose-600': item.isLow, 'text-cx-text-subtle': !item.isLow }"
                  >
                    <span v-if="!Number.isNaN(item.rating)" class="text-sm text-cx-text-subtle border-cx-text-secondary" :class="{ 'text-rose-600': item.isLow, 'text-cx-text-subtle': !item.isLow }">{{ item.rating.toFixed(1) }}</span>
                    {{ item.name }}
                  </a>
                  <span class="text-cx-text-muted" v-if="item !== instructorViews[instructorViews.length - 1]">,&nbsp;</span>
                </template>
              </template>
              <span v-else class="text-sm text-cx-text-subtle">TBA</span>
            </div>
          </div>
          <div class="flex items-center gap-2">
            <Icon name="uil:user" class="h-5 w-5" :class="{ 'text-rose-800': details.enrolled === details.capacity, 'text-cx-text-muted': details.enrolled !== details.capacity }" />
            <span class="text-sm" :class="{ 'text-rose-700': details.enrolled === details.capacity, 'text-cx-text-subtle': details.enrolled !== details.capacity }">{{ details.enrolled }} / {{ details.capacity }} Students</span>
          </div>
          <div v-if="detailScheduleLines.length > 0" class="flex items-start gap-2">
            <Icon name="uil:clock" class="h-5 w-5 text-cx-text-muted shrink-0 mt-0.5" />
            <div class="flex flex-col">
              <span v-for="(line, idx) in detailScheduleLines" :key="idx" class="text-sm text-cx-text-subtle">{{ line }}</span>
            </div>
          </div>
          <div v-if="showDetailLocation" class="flex items-center gap-2">
            <Icon name="uil:location-point" class="h-5 w-5 text-cx-text-muted" />
            <span class="text-sm text-cx-text-subtle">{{ detailLocation }}</span>
          </div>
        </div>

        <div v-if="details" class="flex flex-col gap-2">
          <div v-if="details.units.length > 0" class="flex items-center gap-2">
            <Icon name="uil:bill" class="h-5 w-5 text-cx-text-muted" />
            <span class="text-sm text-cx-text-subtle">{{ renderDetailUnits }} units</span>
          </div>

          <div v-if="details.dupeCreditComment" class="flex items-center gap-2">
            <Icon name="uil:pathfinder" class="h-5 w-5 text-green-500" />
            <span class="text-sm text-green-400">Dupe credit: </span>
            <span class="text-xs bg-green-800 text-green-200 px-1 py-0.5 rounded-md">{{ details.dupeCreditComment }}</span>
          </div>

          <div v-if="details.dClearance" class="flex items-center gap-2">
            <Icon name="uil:bell" class="h-5 w-5 text-rose-500" />
            <span class="text-sm text-rose-400">D-Clearance</span>
          </div>

          <div v-if="details.prerequisites.length > 0" class="flex items-center gap-2">
            <Icon name="uil:link" class="h-5 w-5 text-yellow-500" />
            <span class="text-sm text-yellow-400">Pre-requisite: </span>
            <div class="flex items-center gap-1">
              <span v-for="(group, idx) in details.prerequisites" :key="idx" class="text-xs bg-yellow-800 text-yellow-200 px-1 py-0.5 rounded-md">{{ renderPrereqGroup(group) }}</span>
            </div>
          </div>

          <div v-if="typeMeta" class="flex items-center gap-2">
            <Icon :name="typeMeta.detailIconName" class="h-5 w-5" :class="typeMeta.detailIconClass" />
            <span class="text-sm" :class="typeMeta.detailTextClass">{{ typeMeta.detailLabel }}</span>
          </div>
        </div>

        <div v-else class="h-full flex-1 flex flex-col items-center justify-center text-center border border-dashed border-cx-border rounded-md">
          <Icon name="uil:apps" class="h-14 w-14 text-cx-surface-800 mb-2" />
          <h2 class="text-lg text-cx-text-subtle">No course selected</h2>
          <p class="text-sm text-cx-text-muted max-w-xs">Choose a program from the left or search all courses in the middle panel to view details here.</p>
        </div>
      </div>

    </div>

    <div class="w-full h-full max-h-2/5 min-h-56 border-t border-cx-border pt-2">
      <ScheduleGrid
        :blocks="blocks"
        :preview-blocks="previewBlocks"
        :on-block-click="onBlockClick"
        :on-day-mouse-down="onDayMouseDown"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { getCourseDetails, getSectionDetails, type CourseDetails } from '@/composables/useAPI'
import { useSchedule } from '@/composables/useSchedule'
import { useCalendarDrag } from '@/composables/useCalendarDrag'
import { getCourseTypeMeta } from '@/composables/useCourseTypeMeta'
import { useUiStore } from '@/stores/ui'
import { useScheduleStore } from '@/stores/schedule'
import { useCourseListSource } from '@/composables/useCourseListSource'
import { useRouteMode } from '@/composables/useRouteMode'
import ScheduleGrid from '@/components/ScheduleGrid.vue'
import { useRMPRatings } from '@/composables/useRMPRatings'
import { resolveInstructorViews, type InstructorView } from '@/composables/useInstructorViews'
import { useWatchlistStore } from '@/stores/watchlist'
import { formatDetailScheduleLines, formatCardLocation, formatUnitsOptions, formatPrerequisiteGroup } from '@/composables/api/transforms'
import type { CourseGroup } from '@/types/db'

const ui = useUiStore()
const selectedCourseCode = computed(() => ui.selectedCourseCode)
const selectedSectionId = computed(() => ui.selectedSectionId)
const selectCourse = (code: string, sectionId: string | null = null) => ui.setSelection(code, sectionId)
const router = useRouter()
const { mode } = useCourseListSource()
const { makeSelectionPath } = useRouteMode()

const details = ref<CourseDetails | null>(null)

const loadDetails = async () => {
  const code = selectedCourseCode.value
  const section = selectedSectionId.value
  if (!code) {
    details.value = null
    return
  }
  if (section) {
    const bySection = await getSectionDetails(code, section)
    if (bySection) {
      details.value = bySection
      return
    }
  }
  details.value = await getCourseDetails(code)
}

watch(
  () => [selectedCourseCode.value, selectedSectionId.value],
  () => { loadDetails() },
  { immediate: true }
)

// RateMyProfessors integration
const { getProfessor } = useRMPRatings()
const instructorViews = ref<InstructorView[]>([])

async function updateInstructorViews() {
  const names = details.value?.instructors || []
  instructorViews.value = await resolveInstructorViews(names, getProfessor)
}
watch(
  () => details.value?.instructors,
  () => { void updateInstructorViews() },
  { immediate: true }
)

const typeMeta = computed(() => getCourseTypeMeta(details.value?.type))

const detailScheduleLines = computed(() => formatDetailScheduleLines(details.value?.schedules || []))
// Show location separately only when there's a single schedule (multi-schedule includes location inline)
const hasMultipleSchedules = computed(() => (details.value?.schedules || []).length > 1)
const detailLocation = computed(() => formatCardLocation(details.value?.schedules || []))
const showDetailLocation = computed(() => !hasMultipleSchedules.value && !!detailLocation.value)
const renderDetailUnits = computed(() => formatUnitsOptions(details.value?.units || []))

function renderPrereqGroup(group: CourseGroup) {
  return formatPrerequisiteGroup(group)
}

// Calendar state/hooks
const {
  blocks,
  previewBlocks,
  addBlock,
  updateBlock,
  hasCourseSection,
  removeCourseSection,
  setHoverPreviewFromSchedules,
  clearHoverPreview,
} = useSchedule()

const { onDayMouseDown } = useCalendarDrag({ addBlock, updateBlock, blocks })

const { upsertScheduledSection } = useScheduleStore()
  const watchlistStore = useWatchlistStore()

function onBlockClick(id: string) {
  const target = blocks.value.find((b) => b.id === id)
  if (!target) return
  const courseCode = target.courseCode
  if (!courseCode) return
  const sectionId = target.sectionId || 'section'
  selectCourse(courseCode, target.sectionId || null)
  const parsed = mode.value
  router.push(makeSelectionPath(parsed, courseCode, sectionId))
}

const isInSchedule = computed(() => {
  if (!details.value) return false
  const courseCode = details.value.code
  const sectionId = selectedSectionId.value || null
  return hasCourseSection(courseCode, sectionId)
})

const isInWatchlist = computed(() => {
  if (!details.value) return false
  const courseCode = details.value.code
  const title = details.value.title
  return watchlistStore.hasInWatchlist(courseCode, title)
})

function onAddOrRemove() {
  clearHoverPreview()
  if (!details.value) return
  const courseCode = details.value.code
  const sectionId = selectedSectionId.value || undefined
  if (isInSchedule.value) {
    removeCourseSection(courseCode, sectionId)
    return
  }
  const section = {
    sectionId: (sectionId || 'COURSE') as string,
    instructors: Array.from(new Set(details.value.instructors || [])),
    enrolled: Number(details.value.enrolled || 0),
    capacity: Number(details.value.capacity || 0),
    waitlisted: details.value.waitlisted ?? 0,
    schedules: details.value.schedules,
    hasDClearance: !!details.value.dClearance,
    hasPrerequisites: details.value.prerequisites.length > 0,
    hasDuplicatedCredit: !!details.value.dupeCreditComment,
    units: details.value.units,
    type: details.value.type ?? null,
    isCancelled: details.value.isCancelled ?? false,
  }
  upsertScheduledSection({ code: details.value.code, title: details.value.title, description: details.value.description }, section)
}

function onHoverPreviewEnter() {
  try {
    if (!details.value || details.value.schedules.length === 0) return
    setHoverPreviewFromSchedules(details.value.schedules, details.value.title, details.value.code)
  } catch {}
}

function onHoverPreviewLeave() { clearHoverPreview() }

function onAddOrRemoveWatchlist() {
  if (!details.value) return
  const code = details.value.code
  const title = details.value.title

  if (isInWatchlist.value) {
    watchlistStore.removeFromWatchlist(code, title)
    return
  }

  watchlistStore.upsertWatchlistItem(code, title)
}
</script>

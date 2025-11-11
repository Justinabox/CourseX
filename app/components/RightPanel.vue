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
          <div v-if="details.times.length > 0" class="flex items-center gap-2">
            <Icon name="uil:clock" class="h-5 w-5 text-cx-text-muted" />
            <span class="text-sm text-cx-text-subtle">{{ details.times[0] }}</span>
          </div>
          <div v-if="details.locations.length > 0" class="flex items-center gap-2">
            <Icon name="uil:location-point" class="h-5 w-5 text-cx-text-muted" />
            <span class="text-sm text-cx-text-subtle">{{ details.locations[0] }}</span>
          </div>
        </div>

        <div v-if="details" class="flex flex-col gap-2">
          <div v-if="details.units != null" class="flex items-center gap-2">
            <Icon name="uil:bill" class="h-5 w-5 text-cx-text-muted" />
            <span class="text-sm text-cx-text-subtle">{{ Number(details.units).toFixed(1) }} units</span>
          </div>
          
          <div v-if="details.duplicatedCredits.length > 0" class="flex items-center gap-2">
            <Icon name="uil:pathfinder" class="h-5 w-5 text-green-500" />
            <span class="text-sm text-green-400">Dupe credit: </span>
            <div class="flex items-center gap-1">
              <span v-for="dc in details.duplicatedCredits" :key="dc" class="text-xs bg-green-800 text-green-200 px-1 py-0.5 rounded-md">{{ dc }}</span>
            </div>
          </div>

          <div v-if="details.dClearance" class="flex items-center gap-2">
            <Icon name="uil:bell" class="h-5 w-5 text-rose-500" />
            <span class="text-sm text-rose-400">D-Clearance</span>
          </div>

          <div v-if="details.prerequisites.length > 0" class="flex items-center gap-2">
            <Icon name="uil:link" class="h-5 w-5 text-yellow-500" />
            <span class="text-sm text-yellow-400">Pre-requisite: </span>
            <div class="flex items-center gap-1">
              <span v-for="pr in details.prerequisites" :key="pr" class="text-xs bg-yellow-800 text-yellow-200 px-1 py-0.5 rounded-md">{{ pr }}</span>
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
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { getCourseDetails, getSectionDetails, type CourseDetails } from '@/composables/useAPI'
import { useSchedule } from '@/composables/useSchedule'
import type { ScheduleBlock } from '@/composables/scheduleUtils'
import { getCourseTypeMeta } from '@/composables/useCourseTypeMeta'
import { useStore } from '@/composables/useStore'
import { useCourseListSource } from '@/composables/useCourseListSource'
import { useCourseSelection } from '@/composables/useCourseSelection'
import { useRouteMode } from '@/composables/useRouteMode'
import ScheduleGrid from '@/components/ScheduleGrid.vue'
import { useRMPRatings } from '@/composables/useRMPRatings'

const { selectedCourseCode, selectedSectionId, selectCourse } = useCourseSelection()
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
type InstructorView = { name: string; rating: number; link: string; isLow: boolean }
const instructorViews = ref<InstructorView[]>([])

async function updateInstructorViews() {
  const names = Array.from(new Set((details.value?.instructors || []).filter(Boolean)))
  if (names.length === 0) {
    instructorViews.value = []
    return
  }
  const results = await Promise.all(
    names.map(async (name) => {
      const prof = await getProfessor(name)
      const rating = prof && typeof prof.rating === 'number' && !Number.isNaN(prof.rating) ? prof.rating : NaN
      const link = prof?.link || `https://www.ratemyprofessors.com/search/professors?q=${encodeURIComponent(name)}`
      const isLow = typeof rating === 'number' && !Number.isNaN(rating) ? rating < 3.0 : false
      return { name, rating, link, isLow } as InstructorView
    })
  )
  instructorViews.value = results
}
watch(
  () => details.value?.instructors,
  () => { void updateInstructorViews() },
  { immediate: true }
)

const typeMeta = computed(() => getCourseTypeMeta(details.value?.type))

// Calendar state/hooks
const {
  blocks,
  previewBlocks,
  DAY_LABELS,
  START_MINUTES,
  END_MINUTES,
  SLOT_MINUTES,
  addBlock,
  updateBlock,
  removeBlock,
  geometryFor,
  minutesToTime,
    parseBlocksFromApiSpec,
    hasCourseSection,
    removeCourseSection,
    setHoverPreviewFromString,
    clearHoverPreview,
} = useSchedule()

const { upsertScheduledSection } = useStore()

const dayLabels = DAY_LABELS

const totalRange = END_MINUTES - START_MINUTES
const hourTicks = computed(() => {
  const ticks: { topPct: number; label: string }[] = []
  for (let m = START_MINUTES; m <= END_MINUTES; m += 60) {
    const topPct = ((m - START_MINUTES) / totalRange) * 100
    const h = Math.floor(m / 60)
    const label = `${h.toString().padStart(2, '0')}:00`
    ticks.push({ topPct, label })
  }
  return ticks
})

function onDayMouseDown(e: MouseEvent, dayIndex: number) {
  if (!enableManualCalendarSlotCreation.value) return
  const el = e.currentTarget as HTMLElement
  const start = eventMinutesInColumn(e, el)
  isDragging.value = true
  dragDayIndex.value = dayIndex
  dragStart.value = start
  dragCurrent.value = start
  dragColumnEl.value = el
  const id = addBlock({ dayIndex, startMinutes: start, endMinutes: start + 5, label: 'New', color: 'rgb(var(--color-green-500-rgb) / 0.25)' })
  draftBlockId.value = id || null
  window.addEventListener('mousemove', onWindowMouseMove)
  window.addEventListener('mouseup', onWindowMouseUp)
}

function eventMinutesInColumn(e: MouseEvent, columnEl: HTMLElement): number {
  const rect = columnEl.getBoundingClientRect()
  const y = e.clientY - rect.top
  const frac = Math.max(0, Math.min(1, y / Math.max(rect.height, 1)))
  const minutes = START_MINUTES + Math.round((frac * totalRange) / SLOT_MINUTES) * SLOT_MINUTES
  return Math.max(START_MINUTES, Math.min(END_MINUTES, minutes))
}

function onWindowMouseMove(e: MouseEvent) {
  if (!isDragging.value) return
  const dayIndex = dragDayIndex.value
  if (dayIndex == null) return
  const columnEl = dragColumnEl.value
  if (!columnEl) return
  const minutes = eventMinutesInColumn(e, columnEl)
  dragCurrent.value = minutes
  const id = draftBlockId.value
  if (id) updateBlock(id, { startMinutes: Math.min(dragStart.value, dragCurrent.value), endMinutes: Math.max(dragStart.value, dragCurrent.value) })
}

function onWindowMouseUp() {
  if (!isDragging.value) return
  isDragging.value = false
  window.removeEventListener('mousemove', onWindowMouseMove)
  window.removeEventListener('mouseup', onWindowMouseUp)
  const id = draftBlockId.value
  if (id) {
    const b = blocks.value.find((x) => x.id === id)
    if (b && b.endMinutes - b.startMinutes < SLOT_MINUTES) updateBlock(id, { endMinutes: b.startMinutes + SLOT_MINUTES })
  }
  draftBlockId.value = null
  dragColumnEl.value = null
}

const enableManualCalendarSlotCreation = ref(false)
const isDragging = ref(false)
const dragDayIndex = ref<number | null>(null)
const dragStart = ref(START_MINUTES)
const dragCurrent = ref(START_MINUTES)
const draftBlockId = ref<string | null>(null)
const dragColumnEl = ref<HTMLElement | null>(null)

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
    schedule: (details.value.times || []).join('; '),
    location: (details.value.locations || [])[0] || '',
    hasDClearance: !!details.value.dClearance,
    hasPrerequisites: (details.value.prerequisites || []).length > 0,
    hasDuplicatedCredit: (details.value.duplicatedCredits || []).length > 0,
    units: details.value.units ?? null,
    type: details.value.type ?? null,
  }
  upsertScheduledSection({ code: details.value.code, title: details.value.title, description: details.value.description }, section)
}

function onHoverPreviewEnter() {
  try {
    if (!details.value) return
    const spec = (details.value.times || []).join('; ').trim()
    if (!spec) return
    setHoverPreviewFromString(spec, details.value.title, details.value.code)
  } catch {}
}

function onHoverPreviewLeave() { clearHoverPreview() }
</script>
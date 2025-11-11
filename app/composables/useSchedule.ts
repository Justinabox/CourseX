import { computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useStore } from '@/composables/useStore'
import {
  parseBlocksFromString,
  parseBlocksFromApiSpec,
  timeToMinutes,
  type ScheduleBlock,
  type DayOfWeek,
} from '@/composables/scheduleUtils'
import { useScheduleManualStore } from '@/stores/scheduleManual'
import { minutesToTime } from '@/composables/useTimeParsing'

export const DAY_LABELS: DayOfWeek[] = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

export const START_MINUTES = 8 * 60 // 08:00
export const END_MINUTES = 22 * 60 // 22:00
export const SLOT_MINUTES = 5

const STORAGE_KEY_MANUAL = 'ui:schedule:manualBlocks:v1'

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

function roundToFiveMinutes(minutes: number): number {
  return Math.round(minutes / SLOT_MINUTES) * SLOT_MINUTES
}

export function useSchedule() {
  const { selectedCourseCode, selectedSectionId, scheduledCourses, hasScheduled, removeScheduledSection } = useStore()
  // Manual blocks created by user interactions (dragging). Persisted via Pinia store.
  const manualStore = useScheduleManualStore()
  const { manualBlocks } = storeToRefs(manualStore)
  // Ephemeral preview blocks that should never be persisted
  const previewBlocks = useState<ScheduleBlock[]>('ui:schedule:preview', () => [])

  const totalRangeMinutes = END_MINUTES - START_MINUTES

  // listBlocks is computed from scheduled courses + any manual blocks
  const listBlocks = computed<ScheduleBlock[]>(() => {
    return [...scheduledComputedBlocks.value, ...(manualBlocks.value || [])]
  })

  const normalizeId = (value?: string | null): string | undefined => {
    const s = (value ?? '').toString().trim()
    return s ? s.toUpperCase() : undefined
  }

  const addBlock = (input: Omit<ScheduleBlock, 'id'> & { id?: string }) => {
    const id = input.id || `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`
    const start = roundToFiveMinutes(input.startMinutes)
    const end = roundToFiveMinutes(input.endMinutes)
    const startClamped = clamp(start, START_MINUTES, END_MINUTES)
    const endClamped = clamp(end, START_MINUTES, END_MINUTES)
    const normalized: ScheduleBlock = {
      id,
      dayIndex: clamp(input.dayIndex, 0, 6),
      startMinutes: Math.min(startClamped, endClamped),
      endMinutes: Math.max(startClamped, endClamped),
      label: input.label,
      color: input.color,
      courseCode: normalizeId(input.courseCode),
      sectionId: normalizeId(input.sectionId),
      meta: input.meta,
    }
    manualBlocks.value = [...manualBlocks.value, normalized]
    return normalized.id
  }

  const updateBlock = (id: string, patch: Partial<Omit<ScheduleBlock, 'id'>>) => {
    manualBlocks.value = (manualBlocks.value || []).map((b: ScheduleBlock) => {
      if (b.id !== id) return b
      const next: ScheduleBlock = { ...b, ...patch }
      next.dayIndex = clamp(next.dayIndex, 0, 6)
      next.startMinutes = roundToFiveMinutes(clamp(next.startMinutes, START_MINUTES, END_MINUTES))
      next.endMinutes = roundToFiveMinutes(clamp(next.endMinutes, START_MINUTES, END_MINUTES))
      if (next.startMinutes > next.endMinutes) {
        const t = next.startMinutes
        next.startMinutes = next.endMinutes
        next.endMinutes = t
      }
      return next
    })
  }

  const removeBlock = (id: string) => {
    manualBlocks.value = (manualBlocks.value || []).filter((b: ScheduleBlock) => b.id !== id)
  }

  const clearBlocks = () => {
    manualBlocks.value = []
  }

  const setBlocks = (next: ScheduleBlock[]) => {
    manualBlocks.value = next.map((b) => ({
      ...b,
      dayIndex: clamp(b.dayIndex, 0, 6),
      startMinutes: roundToFiveMinutes(clamp(b.startMinutes, START_MINUTES, END_MINUTES)),
      endMinutes: roundToFiveMinutes(clamp(b.endMinutes, START_MINUTES, END_MINUTES)),
      courseCode: normalizeId((b as any).courseCode),
      sectionId: normalizeId((b as any).sectionId),
    }))
  }

  // Ephemeral preview setters (not persisted)
  const setPreviewBlocks = (next: ScheduleBlock[]) => {
    previewBlocks.value = next.map((b) => ({
      ...b,
      id: b.id || `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
      dayIndex: clamp(b.dayIndex, 0, 6),
      startMinutes: roundToFiveMinutes(clamp(b.startMinutes, START_MINUTES, END_MINUTES)),
      endMinutes: roundToFiveMinutes(clamp(b.endMinutes, START_MINUTES, END_MINUTES)),
      courseCode: normalizeId((b as any).courseCode),
      sectionId: normalizeId((b as any).sectionId),
    }))
  }

  const clearPreviewBlocks = () => {
    previewBlocks.value = []
  }

  // Persistence now handled via useStore.usePersistentState

  const geometryFor = (block: ScheduleBlock) => {
    const topPct = ((block.startMinutes - START_MINUTES) / totalRangeMinutes) * 100
    const heightPct = ((block.endMinutes - block.startMinutes) / totalRangeMinutes) * 100
    return {
      topPct,
      heightPct,
      columnIndex: block.dayIndex, // 0=Sun .. 6=Sat
    }
  }

  // Parsing helpers are imported from scheduleUtils and re-exposed below

  function hashColorFromCourse(code: string): string {
    const s = (code || '').toUpperCase()
    let hash = 0
    for (let i = 0; i < s.length; i++) hash = (hash * 31 + s.charCodeAt(i)) >>> 0
    const hue = hash % 360
    const color = `hsla(${hue}, 85%, 60%, 0.25)`
    return color
  }

  // Compute schedule blocks from scheduled courses in the store
  const scheduledComputedBlocks = computed<ScheduleBlock[]>(() => {
    const out: ScheduleBlock[] = []
    for (const course of scheduledCourses.value || []) {
      const color = hashColorFromCourse(course.code)
      for (const section of course.sections || []) {
        const spec = (section.schedule || '').toString()
        const blocks = parseBlocksFromApiSpec(spec, course.title, color, course.code, section.sectionId)
        out.push(...blocks)
      }
    }
    return out
  })

  const hasCourseSection = (courseCode?: string | null, sectionId?: string | null) => {
    return hasScheduled(courseCode || null, sectionId || null)
  }

  const removeCourseSection = (courseCode?: string | null, sectionId?: string | null) => {
    removeScheduledSection(courseCode || null, sectionId || null)
  }

  // Public helpers to control preview from components
  const setHoverPreviewFromString = (spec: string, label?: string, courseCode?: string) => {
    const color = 'rgba(249, 115, 22, 0.25)' // orange, translucent
    let parsed = parseBlocksFromString(spec, label, color, courseCode)
    if (!parsed || parsed.length === 0) {
      // try API-style
      parsed = parseBlocksFromApiSpec(spec, label, color, courseCode)
    }
    setPreviewBlocks(parsed)
  }

  const clearHoverPreview = () => {
    clearPreviewBlocks()
  }

  return {
    // state
    blocks: listBlocks,
    previewBlocks,
    selectedCourseCode,
    selectedSectionId,
    // constants
    DAY_LABELS,
    START_MINUTES,
    END_MINUTES,
    SLOT_MINUTES,
    // crud
    addBlock,
    updateBlock,
    removeBlock,
    clearBlocks,
    setBlocks,
    setPreviewBlocks,
    clearPreviewBlocks,
    // helpers
    geometryFor,
    timeToMinutes,
    minutesToTime,
    parseBlocksFromString,
    parseBlocksFromApiSpec,
    hasCourseSection,
    removeCourseSection,
    setHoverPreviewFromString,
    clearHoverPreview,
  }
}


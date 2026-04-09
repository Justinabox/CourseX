import { ref, onBeforeUnmount } from 'vue'
import type { Ref } from 'vue'
import type { ScheduleBlock } from '@/utils/scheduleUtils'
import { START_MINUTES, END_MINUTES, SLOT_MINUTES } from '@/composables/useSchedule'

export function useCalendarDrag(opts: {
  addBlock: (input: Omit<ScheduleBlock, 'id'> & { id?: string }) => string | undefined
  updateBlock: (id: string, patch: Partial<Omit<ScheduleBlock, 'id'>>) => void
  blocks: Ref<ScheduleBlock[]>
}) {
  const { addBlock, updateBlock, blocks } = opts
  const totalRange = END_MINUTES - START_MINUTES

  const enableManualCalendarSlotCreation = ref(false)
  const isDragging = ref(false)
  const dragDayIndex = ref<number | null>(null)
  const dragStart = ref(START_MINUTES)
  const dragCurrent = ref(START_MINUTES)
  const draftBlockId = ref<string | null>(null)
  const dragColumnEl = ref<HTMLElement | null>(null)

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

  onBeforeUnmount(() => {
    window.removeEventListener('mousemove', onWindowMouseMove)
    window.removeEventListener('mouseup', onWindowMouseUp)
  })

  return {
    onDayMouseDown,
    enableManualCalendarSlotCreation,
  }
}

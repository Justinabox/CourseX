<template>
  <div class="w-full h-full flex flex-col gap-2 overflow-hidden">
    <div class="w-full grid grid-cols-7 text-xs text-cx-text-muted select-none">
      <div v-for="(d, di) in dayLabels" :key="di" class="text-center">{{ d }}</div>
    </div>
    <div class="w-full grow grid grid-cols-7 overflow-hidden">
      <div
        v-for="(dayIndex, colIdx) in dayIndices"
        :key="'col-' + dayIndex"
        :class="['relative border-r last:border-r-0 border-cx-border bg-cx-surface-950/20 overflow-hidden', { 'pl-8': colIdx === 0 }]"
        :data-day-column="dayIndex"
        @mousedown="(e) => onDayMouseDown?.(e as MouseEvent, dayIndex)"
      >
        <div v-for="(tick, i) in hourTicks" :key="i" class="absolute left-0 right-0 border-t border-cx-border" :style="{ top: tick.topPct + '%' }"></div>
        <div v-if="colIdx === 0" class="absolute inset-0 pointer-events-none">
          <div v-for="(tick, i) in hourTicks" :key="i" class="absolute left-1/2 -translate-x-1/2" :style="{ top: tick.topPct + '%' }">
            <div class="text-[10px] text-cx-text-weak-muted -translate-y-1/2">{{ tick.label }}</div>
          </div>
        </div>

        <div
          v-for="block in blocksByDay(dayIndex)"
          :key="block.id"
          class="absolute left-0 right-0 mx-1 rounded-md text-[10px] leading-tight px-1 py-0.5 cursor-pointer flex flex-col justify-between"
          :style="styleForBlock(block)"
          @click.stop="() => onBlockClick?.(block.id || '')"
        >
          <div class="truncate">{{ block.label || 'Block' }}</div>
          <div v-if="block.courseCode" class="text-[8px] opacity-70">{{ block.courseCode }}</div>
        </div>

        <div
          v-for="p in previewBlocksByDay(dayIndex)"
          :key="'preview-' + p.id"
          class="absolute left-0 right-0 mx-1 rounded-md text-[10px] leading-tight px-1 py-0.5 pointer-events-none"
          :style="styleForPreviewBlock(p)"
        >
          <div class="truncate">{{ p.label || 'Preview' }}</div>
          <div v-if="p.courseCode" class="text-[8px] opacity-70">{{ p.courseCode }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { ScheduleBlock } from '@/composables/scheduleUtils'
import { DAY_LABELS, START_MINUTES, END_MINUTES } from '@/composables/useSchedule'

const props = defineProps<{ blocks: ScheduleBlock[]; previewBlocks: ScheduleBlock[]; onBlockClick?: (id: string) => void; onDayMouseDown?: (e: MouseEvent, dayIndex: number) => void }>()

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

const dayLabels = DAY_LABELS
const dayIndices = computed(() => [0, 1, 2, 3, 4, 5, 6])

const blocksByDay = (dayIndex: number) => props.blocks.filter((b) => b.dayIndex === dayIndex)
const previewBlocksByDay = (dayIndex: number) => props.previewBlocks.filter((b) => b.dayIndex === dayIndex)

function styleForBlock(block: ScheduleBlock) {
  const topPct = ((block.startMinutes - START_MINUTES) / totalRange) * 100
  const heightPct = ((block.endMinutes - block.startMinutes) / totalRange) * 100
  const color = (block.color as string) || 'rgb(var(--color-cx-blue-500-rgb) / 0.25)'
  const border = (block.color as string) || 'rgb(var(--color-cx-blue-500-rgb) / 0.65)'
  return { top: `${topPct}%`, height: `${heightPct}%`, background: color, border: `1px solid ${border}` }
}

function styleForPreviewBlock(block: ScheduleBlock) {
  const topPct = ((block.startMinutes - START_MINUTES) / totalRange) * 100
  const heightPct = ((block.endMinutes - block.startMinutes) / totalRange) * 100
  const color = (block.color as string) || 'rgb(var(--color-cx-orange-500-rgb) / 0.25)'
  const border = 'rgb(var(--color-cx-orange-500-rgb) / 0.7)'
  return { top: `${topPct}%`, height: `${heightPct}%`, background: color, border: `1px dashed ${border}` }
}
</script>

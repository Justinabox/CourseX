<template>
  <div class="w-full h-full max-h-2/5 min-h-56 border-t border-cx-border pt-2">
    <ScheduleGrid
      :blocks="blocks"
      :preview-blocks="previewBlocks"
      :on-block-click="onBlockClick"
      :on-day-mouse-down="onDayMouseDown"
    />
  </div>
</template>

<script setup lang="ts">
import { useSchedule } from '@/composables/useSchedule'
import { useCalendarDrag } from '@/composables/useCalendarDrag'
import { useCourseListSource } from '@/composables/useCourseListSource'
import { useRouteMode } from '@/composables/useRouteMode'
import ScheduleGrid from '@/components/ScheduleGrid.vue'

const router = useRouter()
const { mode } = useCourseListSource()
const { makeSelectionPath } = useRouteMode()

const {
  blocks,
  previewBlocks,
  addBlock,
  updateBlock,
} = useSchedule()

const { onDayMouseDown } = useCalendarDrag({ addBlock, updateBlock, blocks })

function onBlockClick(id: string) {
  const target = blocks.value.find((b) => b.id === id)
  if (!target) return
  const courseCode = target.courseCode
  if (!courseCode) return
  const sectionId = target.sectionId || 'section'
  const parsed = mode.value
  router.push(makeSelectionPath(parsed, courseCode, sectionId))
}
</script>

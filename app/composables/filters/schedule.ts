import type { UICourseSection } from '@/composables/api/types'
import { scheduleToBlocks } from '@/composables/scheduleUtils'

export function sectionMatchesScheduleFilters(
  section: UICourseSection,
  days: number[],
  start: number | null,
  end: number | null,
): boolean {
  if ((!days || days.length === 0) && start == null && end == null) return true
  const blocks = scheduleToBlocks(section.schedules)
  if (blocks.length === 0) return false

  const daySet = new Set(days || [])
  if (daySet.size > 0) {
    if (!blocks.every((b) => daySet.has(b.dayIndex))) return false
  }

  if (start != null) {
    if (!blocks.every((b) => b.startMinutes >= start)) return false
  }
  if (end != null) {
    if (!blocks.every((b) => b.endMinutes <= end)) return false
  }
  return true
}

export function sectionMatchesTriState(flag: boolean, state: 'any' | 'only' | 'exclude'): boolean {
  if (state === 'any') return true
  if (state === 'only') return !!flag
  return !flag
}

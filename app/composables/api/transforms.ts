import type { CourseCode, CourseGroup, DayCode, Schedule } from '@/types/db'

// ─── DayCode Mapping ─────────────────────────────────────────────────────────

const DAYCODE_LABELS: Record<DayCode, string> = {
  M: 'M', T: 'T', W: 'W', Th: 'Th', F: 'F', S: 'S',
}

// ─── Schedule Formatting ─────────────────────────────────────────────────────

function trimTimeSeconds(time: string | null | undefined): string {
  if (!time) return ''
  const m = /^(\d{1,2}:\d{2})(:\d{2})?$/.exec(time.trim())
  return m ? m[1] : time.trim()
}

export function formatScheduleAsString(schedule: Schedule): string {
  const days = schedule.days.map((d) => DAYCODE_LABELS[d] ?? d).join('')
  const start = trimTimeSeconds(schedule.start)
  const end = trimTimeSeconds(schedule.end)
  const time = `${days} ${start} - ${end}`
  return schedule.location ? `${time} (${schedule.location})` : time
}

/** Format just the time portion of a schedule (no location). */
export function formatScheduleTimeOnly(schedule: Schedule): string {
  const days = schedule.days.map((d) => DAYCODE_LABELS[d] ?? d).join('')
  const start = trimTimeSeconds(schedule.start)
  const end = trimTimeSeconds(schedule.end)
  return `${days} ${start} - ${end}`
}

export function formatSchedulesAsString(schedules: Schedule[]): string {
  if (!schedules || schedules.length === 0) return ''
  return schedules.map(formatScheduleAsString).join('; ')
}

/**
 * Card-level schedule display:
 * - 1 schedule: just the time, no location
 * - >1 schedules: first schedule time + "(+N)"
 */
export function formatCardSchedule(schedules: Schedule[]): string {
  if (!schedules || schedules.length === 0) return ''
  const first = formatScheduleTimeOnly(schedules[0])
  if (schedules.length === 1) return first
  return `${first} (+${schedules.length - 1})`
}

/**
 * Card-level location display with priority:
 * 1. Any physical location (not ONLINE, not OFFICE)
 * 2. OFFICE
 * 3. ONLINE (only if nothing else)
 */
export function formatCardLocation(schedules: Schedule[]): string {
  if (!schedules || schedules.length === 0) return ''
  let physical: string | null = null
  let office = false
  let online = false
  for (const s of schedules) {
    const loc = (s.location || '').trim()
    if (!loc) continue
    const upper = loc.toUpperCase()
    if (upper === 'ONLINE') { online = true; continue }
    if (upper === 'OFFICE') { office = true; continue }
    if (!physical) physical = loc
  }
  if (physical) return physical
  if (office) return 'OFFICE'
  if (online) return 'ONLINE'
  return ''
}

/**
 * Detail panel schedule display:
 * - >1 schedules: each line is "Time (Location)"
 * - 1 schedule: just the time (location shown separately)
 */
export function formatDetailScheduleLines(schedules: Schedule[]): string[] {
  if (!schedules || schedules.length === 0) return []
  if (schedules.length === 1) return [formatScheduleTimeOnly(schedules[0])]
  return schedules.map(formatScheduleAsString)
}

// ─── Location Extraction ─────────────────────────────────────────────────────

export function schedulesToLocations(schedules: Schedule[]): string[] {
  const seen = new Set<string>()
  const locations: string[] = []
  for (const s of schedules) {
    const loc = (s.location || '').trim()
    if (loc && !seen.has(loc)) {
      seen.add(loc)
      locations.push(loc)
    }
  }
  return locations
}

// ─── Units Formatting ────────────────────────────────────────────────────────

export function formatUnitsRange(units: number[]): string {
  if (!units || units.length === 0) return ''
  const sorted = [...units].sort((a, b) => a - b)
  const min = sorted[0].toFixed(1)
  const max = sorted[sorted.length - 1].toFixed(1)
  return min === max ? min : `${min}-${max}`
}

export function formatUnitsOptions(units: number[]): string {
  if (!units || units.length === 0) return ''
  return [...units].sort((a, b) => a - b).map(u => u.toFixed(1)).join(', ')
}

// ─── CourseCode Formatting ───────────────────────────────────────────────────

export function formatCourseCode(code: CourseCode): string {
  const base = `${code.prefix}-${code.number}`
  return code.suffix ? `${base}${code.suffix}` : base
}

// ─── Prerequisite Formatting ─────────────────────────────────────────────────

export function formatPrerequisiteGroup(group: CourseGroup): string {
  const codeStrs = group.codes.map(formatCourseCode)
  if (group.total === 1 && group.codes.length === 1) return codeStrs[0]
  if (group.required >= group.codes.length) return codeStrs.join(' and ')
  return `${group.required} of: ${codeStrs.join(', ')}`
}

export function formatPrerequisites(groups: CourseGroup[]): string {
  if (!groups || groups.length === 0) return ''
  return groups.map(formatPrerequisiteGroup).join(' + ')
}


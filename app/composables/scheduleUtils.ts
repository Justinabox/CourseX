import type { DayCode, Schedule } from '@/types/db'

export type DayOfWeek = 'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun'

export type ScheduleBlock = {
  id?: string
  dayIndex: number // 0=Sun .. 6=Sat
  startMinutes: number // minutes from 00:00, e.g. 8:30 => 510
  endMinutes: number
  label?: string
  color?: string
  courseCode?: string
  sectionId?: string
  meta?: Record<string, unknown>
}

// ─── Structured Schedule → ScheduleBlock Conversion ──────────────────────────

const DAYCODE_TO_INDEX: Record<DayCode, number> = {
  M: 1, T: 2, W: 3, Th: 4, F: 5, S: 6,
}

function makeBlockId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`
}

export function scheduleToBlocks(
  schedules: Schedule[],
  label?: string,
  color?: string,
  courseCode?: string,
  sectionId?: string,
): ScheduleBlock[] {
  const out: ScheduleBlock[] = []
  for (const sched of schedules) {
    const start = timeToMinutes(sched.start)
    const end = timeToMinutes(sched.end)
    for (const day of sched.days) {
      const dayIndex = DAYCODE_TO_INDEX[day]
      if (dayIndex == null) continue
      out.push({ id: makeBlockId(), dayIndex, startMinutes: start, endMinutes: end, label, color, courseCode, sectionId })
    }
  }
  return out
}

export function timeToMinutes(time: string): number {
  const m = /^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$/.exec(time)
  if (!m) return 0
  const h = Number(m[1])
  const mm = Number(m[2])
  return h * 60 + mm
}

export function dayIndexFromToken(token: string): number | null {
  const map: Record<string, number> = {
    sun: 0,
    mon: 1,
    tue: 2,
    wed: 3,
    thu: 4,
    fri: 5,
    sat: 6,
    u: 0,
    m: 1,
    t: 2,
    w: 3,
    th: 4,
    f: 5,
    s: 6,
  }
  const key = token.trim().toLowerCase()
  const value = map[key]
  return typeof value === 'number' ? value : null
}

export function parseBlocksFromString(spec: string, label?: string, color?: string, courseCode?: string): ScheduleBlock[] {
  if (!spec) return []
  const match = spec.match(/(.*)\s+(\d{1,2}:\d{2})(?::\d{2})?\s*-\s*(\d{1,2}:\d{2})(?::\d{2})?/i)
  if (!match) return []
  const [, dayPart, startStr, endStr] = match
  if (!dayPart || !startStr || !endStr) return []
  const start = timeToMinutes(startStr)
  const end = timeToMinutes(endStr)
  const dayTokens = dayPart.split(/[\s,/]+/).filter(Boolean)
  const indices: number[] = []
  for (const tok of dayTokens) {
    const idx = dayIndexFromToken(tok)
    if (idx != null) indices.push(idx)
  }
  return indices.map((dayIndex) => ({
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
    dayIndex,
    startMinutes: start,
    endMinutes: end,
    label,
    color,
    courseCode,
  }))
}

export function decodeApiDaysToken(token: string): number[] {
  const s = (token || '').toUpperCase()
  const indices: number[] = []
  let i = 0
  while (i < s.length) {
    if (s.startsWith('TH', i)) {
      indices.push(4)
      i += 2
      continue
    }
    const ch = s[i]
    switch (ch) {
      case 'M': indices.push(1); break
      case 'T': indices.push(2); break
      case 'W': indices.push(3); break
      case 'F': indices.push(5); break
      case 'S': indices.push(6); break
      default: break
    }
    i += 1
  }
  return Array.from(new Set(indices)).sort((a, b) => a - b)
}

export function parseBlocksFromApiSpec(spec: string, label?: string, color?: string, courseCode?: string, sectionId?: string): ScheduleBlock[] {
  if (!spec) return []
  const chunks = spec.split(/;+/).map((s) => s.trim()).filter(Boolean)
  const out: ScheduleBlock[] = []
  for (const chunk of chunks) {
    const m = /^([A-Za-z]+)\s+(\d{1,2}:\d{2})(?::\d{2})?\s*-\s*(\d{1,2}:\d{2})(?::\d{2})?$/.exec(chunk.replace(/\s+/g, ' '))
    if (!m) continue
    const dayToken = m[1] ?? ''
    const startStr = m[2] ?? ''
    const endStr = m[3] ?? ''
    const start = timeToMinutes(startStr)
    const end = timeToMinutes(endStr)
    const indices = decodeApiDaysToken(dayToken)
    for (const dayIndex of indices) {
      out.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
        dayIndex,
        startMinutes: start,
        endMinutes: end,
        label,
        color,
        courseCode,
        sectionId,
      })
    }
  }
  return out
}


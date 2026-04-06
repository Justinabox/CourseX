import type { CourseDetails, UICourse, CourseCode } from '../api/types'
import { normalizeCourseCode } from '@/utils/normalize'
import { formatCourseCode } from './transforms'

// Module-level caches
let programsCache: Record<string, any> | null = null
const coursesByTermCache = new Map<string, { data: UICourse[]; ts: number }>()
const COURSES_CACHE_TTL = 5 * 60 * 1000 // 5 minutes

type ApiUICourse = Omit<UICourse, 'displayCode'> & {
  displayCode?: CourseCode | string | null
}

type ApiCourseDetails = Omit<CourseDetails, 'displayCode'> & {
  displayCode?: CourseCode | string | null
}

function normalizeDisplayCode(displayCode?: CourseCode | string | null): string | null {
  if (!displayCode) return null
  if (typeof displayCode === 'string') return displayCode
  return formatCourseCode(displayCode)
}

function mapUICourse(course: ApiUICourse): UICourse {
  return {
    ...course,
    displayCode: normalizeDisplayCode(course.displayCode),
  }
}

function mapCourseDetails(details: ApiCourseDetails): CourseDetails {
  return {
    ...details,
    displayCode: normalizeDisplayCode(details.displayCode),
  }
}

export async function listSchoolAndPrograms() {
  if (programsCache) return programsCache
  try {
    const data = await $fetch<Record<string, any>>('/api/programs')
    programsCache = data
    return data
  } catch (e: any) {
    if (e?.statusCode === 401) return {}
    throw e
  }
}

export async function listAllCourses(termId: string): Promise<UICourse[]> {
  const cached = coursesByTermCache.get(termId)
  if (cached && Date.now() - cached.ts < COURSES_CACHE_TTL) return cached.data
  try {
    const data = (await $fetch<ApiUICourse[]>(`/api/courses/${termId}`)).map(mapUICourse)
    coursesByTermCache.set(termId, { data, ts: Date.now() })
    return data
  } catch (e: any) {
    if (e?.statusCode === 401) return []
    throw e
  }
}

export async function getSchoolCourses(termId: string, schoolPrefix: string, programPrefix: string): Promise<UICourse[]> {
  if (!schoolPrefix || !programPrefix) return []
  try {
    return (await $fetch<ApiUICourse[]>(`/api/courses/${termId}/by-program/${encodeURIComponent(programPrefix)}`)).map(mapUICourse)
  } catch (e: any) {
    if (e?.statusCode === 401) return []
    throw e
  }
}

export async function getCourseDetails(termId: string, courseCode: string, title?: string): Promise<CourseDetails | null> {
  if (!courseCode) return null
  try {
    const data = await $fetch<ApiCourseDetails>(`/api/courses/${termId}/${encodeURIComponent(courseCode)}`, {
      query: title ? { title } : undefined,
    })
    return mapCourseDetails(data)
  } catch {
    return null
  }
}

export async function getSectionDetails(termId: string, courseCode: string, sectionId: string): Promise<CourseDetails | null> {
  if (!courseCode || !sectionId) return null
  try {
    const data = await $fetch<ApiCourseDetails>(`/api/sections/${termId}/${encodeURIComponent(sectionId)}`, {
      query: { courseId: courseCode },
    })
    return mapCourseDetails(data)
  } catch {
    return null
  }
}

export async function getSectionDetailsBatch(termId: string, sectionIds: string[]): Promise<CourseDetails[]> {
  if (!sectionIds || sectionIds.length === 0) return []
  try {
    return (await $fetch<ApiCourseDetails[]>(`/api/sections/${termId}/batch`, {
      method: 'POST',
      body: { sectionIds },
    })).map(mapCourseDetails)
  } catch (e: any) {
    if (e?.statusCode === 401) return []
    throw e
  }
}

export function createWatchlistKey(code: string, title: string): string {
  const normalizedCode = normalizeCourseCode(code)
  const normalizedTitle = title.trim().toUpperCase()
  return `${normalizedCode}::${normalizedTitle}`
}

export function parseWatchlistKey(key: string): { code: string; title: string } | null {
  const parts = key.split('::')
  if (parts.length < 2) return null
  const code = parts[0]
  const title = parts.slice(1).join('::')
  if (!code || !title) return null
  return { code, title }
}

// ─── Server-synced watchlist/schedule ─────────────────────────────────────

export async function fetchWatchlistData(termId: string): Promise<{ keys: string[]; courses: UICourse[] }> {
  const data = await $fetch<{ keys: string[]; courses: ApiUICourse[] }>(`/api/user/watchlist/${termId}`)
  return { ...data, courses: data.courses.map(mapUICourse) }
}

export async function putWatchlistKeys(termId: string, keys: string[]): Promise<string[]> {
  const res = await $fetch<{ keys: string[] }>(`/api/user/watchlist/${termId}`, {
    method: 'PUT',
    body: { keys },
  })
  return res.keys
}

export async function postWatchlistKey(termId: string, key: string): Promise<string[]> {
  const res = await $fetch<{ keys: string[] }>(`/api/user/watchlist/${termId}`, {
    method: 'POST',
    body: { key },
  })
  return res.keys
}

export async function deleteWatchlistKey(termId: string, key: string): Promise<string[]> {
  const res = await $fetch<{ keys: string[] }>(`/api/user/watchlist/${termId}`, {
    method: 'DELETE',
    query: { key },
  })
  return res.keys
}

type ScheduleEntryApi = { courseId: string; sectionId: number }

export async function fetchScheduleData(termId: string): Promise<{ entries: ScheduleEntryApi[]; courses: UICourse[] }> {
  const data = await $fetch<{ entries: ScheduleEntryApi[]; courses: ApiUICourse[] }>(`/api/user/schedule/${termId}`)
  return { ...data, courses: data.courses.map(mapUICourse) }
}

export async function putScheduleEntries(termId: string, entries: ScheduleEntryApi[]): Promise<void> {
  await $fetch(`/api/user/schedule/${termId}`, {
    method: 'PUT',
    body: { entries },
  })
}

export async function postScheduleEntry(termId: string, courseId: string, sectionId: number): Promise<void> {
  await $fetch(`/api/user/schedule/${termId}`, {
    method: 'POST',
    body: { courseId, sectionId },
  })
}

export async function deleteScheduleEntry(termId: string, courseId: string, sectionId: number): Promise<void> {
  await $fetch(`/api/user/schedule/${termId}`, {
    method: 'DELETE',
    query: { courseId, sectionId },
  })
}

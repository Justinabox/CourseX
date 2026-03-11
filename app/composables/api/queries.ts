import type { CourseDetails, UICourse } from '../api/types'
import { normalizeCourseCode } from '@/utils/normalize'

// Module-level caches
let programsCache: Record<string, any> | null = null
const coursesByTermCache = new Map<string, { data: UICourse[]; ts: number }>()
const COURSES_CACHE_TTL = 5 * 60 * 1000 // 5 minutes

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
    const data = await $fetch<UICourse[]>(`/api/courses/${termId}`)
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
    return await $fetch<UICourse[]>(`/api/courses/${termId}/by-program/${encodeURIComponent(programPrefix)}`)
  } catch (e: any) {
    if (e?.statusCode === 401) return []
    throw e
  }
}

export async function getCourseDetails(termId: string, courseCode: string, title?: string): Promise<CourseDetails | null> {
  if (!courseCode) return null
  try {
    return await $fetch<CourseDetails>(`/api/courses/${termId}/${encodeURIComponent(courseCode)}`, {
      query: title ? { title } : undefined,
    })
  } catch {
    return null
  }
}

export async function getSectionDetails(termId: string, courseCode: string, sectionId: string): Promise<CourseDetails | null> {
  if (!courseCode || !sectionId) return null
  try {
    return await $fetch<CourseDetails>(`/api/sections/${termId}/${encodeURIComponent(sectionId)}`)
  } catch {
    return null
  }
}

export async function getSectionDetailsBatch(termId: string, sectionIds: string[]): Promise<CourseDetails[]> {
  if (!sectionIds || sectionIds.length === 0) return []
  try {
    return await $fetch<CourseDetails[]>(`/api/sections/${termId}/batch`, {
      method: 'POST',
      body: { sectionIds },
    })
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
  return await $fetch<{ keys: string[]; courses: UICourse[] }>(`/api/user/watchlist/${termId}`)
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

export async function fetchScheduleData(termId: string): Promise<{ sectionIds: number[]; courses: UICourse[] }> {
  return await $fetch<{ sectionIds: number[]; courses: UICourse[] }>(`/api/user/schedule/${termId}`)
}

export async function putScheduleSectionIds(termId: string, sectionIds: number[]): Promise<number[]> {
  const res = await $fetch<{ sectionIds: number[] }>(`/api/user/schedule/${termId}`, {
    method: 'PUT',
    body: { sectionIds },
  })
  return res.sectionIds
}

export async function postScheduleSectionId(termId: string, sectionId: number): Promise<number[]> {
  const res = await $fetch<{ sectionIds: number[] }>(`/api/user/schedule/${termId}`, {
    method: 'POST',
    body: { sectionId },
  })
  return res.sectionIds
}

export async function deleteScheduleSectionId(termId: string, sectionId: number): Promise<number[]> {
  const res = await $fetch<{ sectionIds: number[] }>(`/api/user/schedule/${termId}`, {
    method: 'DELETE',
    query: { sectionId },
  })
  return res.sectionIds
}

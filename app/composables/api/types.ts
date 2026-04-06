import type { Schedule, GECode, CourseGroup, CourseCode, DayCode, SectionMode, Restriction } from '@/types/db'

export type { Schedule, GECode, CourseGroup, CourseCode, DayCode, SectionMode, Restriction }

export type UICourseSection = {
  sectionId: string
  instructors: string[]
  enrolled: number
  capacity: number
  waitlisted: number
  schedules: Schedule[]
  hasDClearance: boolean
  hasPrerequisites: boolean
  hasDuplicatedCredit: boolean
  units: number[]
  type: SectionMode | null
  isCancelled: boolean
}

export type UICourse = {
  title: string
  code: string
  description: string
  sections: UICourseSection[]
  ges: GECode[]
  displayCode?: string | null
  isCrosslisted?: boolean
}

export type CourseDetails = {
  sectionId?: string
  title: string
  code: string
  description: string
  instructors: string[]
  units: number[]
  enrolled: number
  capacity: number
  waitlisted: number
  schedules: Schedule[]
  dupeCreditComment: string | null
  prerequisites: CourseGroup[]
  corequisites: CourseGroup[]
  restrictions: Restriction[]
  note: string | null
  recomPrepComment: string | null
  dClearance: boolean
  type: SectionMode | null
  ges: GECode[]
  displayCode?: string | null
  isCrosslisted?: boolean
  isCancelled: boolean
  syllabus: string | null
  previousSyllabus: { termCode: number; filename: string } | null
}

// Shared app-level types (centralized)
export type TriState = 'any' | 'only' | 'exclude'

export type EnrollmentFilter = 'any' | 'only-full' | 'only-open'

export type SchedulePair = {
  code: string
  sectionId: string
}

export type CourseFiltersState = {
  searchText: string
  days: number[]
  timeStartMinutes: number | null
  timeEndMinutes: number | null
  unitsMin: number | null
  unitsMax: number | null
  courseLevelMin: number | null
  courseLevelMax: number | null
  dClearance: TriState
  prerequisites: TriState
  duplicatedCredit: TriState
  conflicts: TriState
  enrollment: EnrollmentFilter
  sectionTypes: string[]
}

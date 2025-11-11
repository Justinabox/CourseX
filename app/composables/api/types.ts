export type UICourseSection = {
  sectionId: string
  instructors: string[]
  enrolled: number
  capacity: number
  schedule: string
  location: string
  hasDClearance: boolean
  hasPrerequisites: boolean
  hasDuplicatedCredit: boolean
  units?: number | null
  type?: string | null
}

export type UICourse = {
  title: string
  code: string
  description: string
  sections: UICourseSection[]
  ge?: string[]
}

export type RawSection = {
  sectionCode?: string | null
  instructors?: string[]
  units?: number | string | null
  total?: number | null
  registered?: number | null
  location?: string | null
  time?: string | null
  duplicatedCredits?: string[]
  prerequisites?: string[]
  dClearance?: boolean
  type?: string | null
}

export type RawGroupedCourse = {
  title?: string
  description?: string
  courseCode?: string
  sections?: RawSection[]
  GE?: string[]
}

export type CourseDetails = {
  title: string
  code: string
  description: string
  instructors: string[]
  units?: number | null
  enrolled: number
  capacity: number
  times: string[]
  locations: string[]
  duplicatedCredits: string[]
  prerequisites: string[]
  dClearance: boolean
  type: string | null
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

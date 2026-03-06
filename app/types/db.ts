// ─── PostgreSQL Enum Types ───────────────────────────────────────────────────

export type GECode =
  | 'A' | 'B' | 'C' | 'D' | 'E' | 'F'
  | 'G' | 'H'
  | 'GESM' | 'DSL'

export type SectionMode =
  | 'Lecture'
  | 'Lecture/Lab'
  | 'Lab'
  | 'Discussion'
  | 'Quiz'
  | 'Lecture/Discussion'

export type DayCode = 'M' | 'T' | 'W' | 'Th' | 'F' | 'S'

// ─── PostgreSQL Composite Types ──────────────────────────────────────────────

export type CourseCode = {
  prefix: string
  number: number
  suffix: string
}

export type CourseGroup = {
  total: number
  required: number
  codes: CourseCode[]
}

export type Restriction = {
  code: string
  description: string
  inclusive: boolean
}

export type Schedule = {
  days: DayCode[]
  start: string
  end: string
  location: string
}

// ─── Table Row Types ─────────────────────────────────────────────────────────

export type DbSchool = {
  prefix: string
  name: string
}

export type DbProgram = {
  prefix: string
  name: string
  schoolPrefix: string
}

export type DbInstructor = {
  name: string
  rating: number | null
  difficulty: number | null
  ratingCount: number | null
  takeAgainPercent: number | null
  rmpId: number | null
}

export type DbCourse = {
  id: string
  title: string
  description: string | null
  note: string | null
  dupeCreditComment: string | null
  recomPrepComment: string | null
  registrarCode: CourseCode
  prerequisites: CourseGroup[]
  corequisites: CourseGroup[]
  displayCode: CourseCode | null
  isCrossListed: boolean
  ges: GECode[]
  restrictions: Restriction[]
}

export type DbSection = {
  id: number
  description: string | null
  note: string | null
  dClearance: boolean
  totalSeat: number
  registeredSeat: number
  waitlistedSeat: number
  units: number[]
  schedules: Schedule[]
  syllabus: string | null
  rnrId: number
  peId: number
  isCancelled: boolean
}

// ─── Join Table Row Types ────────────────────────────────────────────────────

export type DbCourseSection = {
  courseId: string
  sectionId: number
}

export type DbSectionInstructor = {
  sectionId: number
  instructorName: string
}

import { pgTable, varchar, text, boolean, smallint, integer, real, timestamp, primaryKey, index } from 'drizzle-orm/pg-core'

// ─── Static Tables ───────────────────────────────────────────────────────────

export const schools = pgTable('schools', {
  prefix: varchar('prefix', { length: 16 }).primaryKey(),
  name: text('name').notNull(),
})

export const programs = pgTable('programs', {
  prefix: varchar('prefix', { length: 16 }).primaryKey(),
  name: text('name').notNull(),
  schoolPrefix: varchar('school_prefix', { length: 16 }).notNull().references(() => schools.prefix, { onDelete: 'cascade' }),
})

export const instructors = pgTable('instructors', {
  name: text('name').primaryKey(),
  rating: smallint('rating'),
  difficulty: smallint('difficulty'),
  ratingCount: smallint('rating_count'),
  takeAgainPercent: real('take_again_percent'),
  rmpId: integer('rmp_id'),
})

export const pipelineMeta = pgTable('pipeline_meta', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
})

// ─── Term-Partitioned Table Factories ────────────────────────────────────────
//
// The scrapper creates per-term tables: courses_{term}, sections_{term}, etc.
// Drizzle doesn't natively support dynamic table names, so we use factory
// functions that return table definitions for a given term code.
//
// IMPORTANT: PostgreSQL composite types (coursecode, schedule, coursegroup,
// restriction) are stored as composite/array columns. Drizzle has limited
// support for these — queries that need composite data should use raw SQL
// with to_jsonb() casts via drizzle-orm's `sql` template, then parse
// results into the TypeScript types from @/types/db.ts.

export function coursesTable(term: string) {
  return pgTable(`courses_${term}`, {
    id: varchar('id', { length: 16 }).primaryKey(),
    title: text('title').notNull(),
    description: text('description'),
    note: text('note'),
    dupeCreditComment: text('dupe_credit_comment'),
    recomPrepComment: text('recom_prep_comment'),
    isCrossListed: boolean('is_cross_listed'),
    // Composite type columns (registrar_code, display_code, prerequisites,
    // corequisites, ges, restrictions) require raw SQL with to_jsonb() casts.
    // They are intentionally omitted from the Drizzle schema and handled via
    // sql`` in query functions.
  })
}

export function sectionsTable(term: string) {
  return pgTable(`sections_${term}`, {
    id: integer('id').primaryKey(),
    // type column uses sectionmode enum — cast to text in raw SQL queries
    description: text('description'),
    note: text('note'),
    dClearance: boolean('d_clearance').notNull(),
    totalSeat: smallint('total_seat').notNull().default(0),
    registeredSeat: smallint('registered_seat').notNull().default(0),
    waitlistedSeat: smallint('waitlisted_seat').notNull().default(0),
    syllabus: text('syllabus'),
    rnrId: integer('rnr_id').notNull(),
    peId: integer('pe_id').notNull(),
    isCancelled: boolean('is_cancelled').notNull().default(false),
    // Composite type columns (units smallint[], schedules schedule[]) require
    // raw SQL. Handled via sql`` in query functions.
  })
}

export function courseSectionsTable(term: string) {
  const courses = coursesTable(term)
  const sections = sectionsTable(term)

  return pgTable(`course_sections_${term}`, {
    courseId: varchar('course_id', { length: 16 }).notNull().references(() => courses.id, { onDelete: 'cascade' }),
    sectionId: integer('section_id').notNull().references(() => sections.id, { onDelete: 'cascade' }),
  }, (t) => [
    primaryKey({ columns: [t.courseId, t.sectionId] }),
    index(`course_sections_${term}_section_id_idx`).on(t.sectionId),
  ])
}

export function sectionInstructorsTable(term: string) {
  const sections = sectionsTable(term)

  return pgTable(`section_instructors_${term}`, {
    sectionId: integer('section_id').notNull().references(() => sections.id, { onDelete: 'cascade' }),
    instructorName: text('instructor_name').notNull().references(() => instructors.name, { onDelete: 'restrict' }),
  }, (t) => [
    primaryKey({ columns: [t.sectionId, t.instructorName] }),
    index(`section_instructors_${term}_instructor_name_idx`).on(t.instructorName),
  ])
}

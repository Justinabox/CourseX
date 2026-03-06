import { useSql } from '~~/server/db'
import { validateTermCode } from '~~/server/utils/termValidator'

// ─── Programs (school → program tree) ──────────────────────────────────────

export async function queryPrograms() {
  const sql = useSql()
  const schools = await sql`SELECT prefix, name FROM schools ORDER BY name`
  const programs = await sql`SELECT prefix, name, school_prefix FROM programs ORDER BY name`

  const tree: Record<string, { name: string; programs: { prefix: string; name: string }[] }> = {}
  for (const s of schools) {
    tree[s.prefix] = { name: s.name, programs: [] }
  }
  for (const p of programs) {
    tree[p.school_prefix]?.programs.push({ prefix: p.prefix, name: p.name })
  }
  return tree
}

// ─── Courses (all courses for a term, grouped with sections) ───────────────

export async function queryCoursesByTerm(termId: string) {
  validateTermCode(termId)
  const sql = useSql()

  const rows = await sql`
    SELECT
      c.id,
      c.title,
      c.description,
      c.dupe_credit_comment,
      c.ges::text[] as ges,
      to_jsonb(c.prerequisites) as prerequisites,
      to_jsonb(c.corequisites) as corequisites,
      to_jsonb(c.registrar_code) as registrar_code,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId',       s.id::text,
        'type',            s.type::text,
        'enrolled',        s.registered_seat,
        'capacity',        s.total_seat,
        'waitlisted',      s.waitlisted_seat,
        'units',           s.units,
        'schedules',       to_jsonb(s.schedules),
        'hasDClearance',   s.d_clearance,
        'isCancelled',     s.is_cancelled
      )) as sections,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId', si.section_id::text,
        'name',      si.instructor_name
      )) FILTER (WHERE si.instructor_name IS NOT NULL) as section_instructors
    FROM ${sql.unsafe(`courses_${termId}`)} c
    JOIN ${sql.unsafe(`course_sections_${termId}`)} cs ON cs.course_id = c.id
    JOIN ${sql.unsafe(`sections_${termId}`)} s ON s.id = cs.section_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termId}`)} si ON si.section_id = s.id
    WHERE s.is_cancelled = false
    GROUP BY c.id
    ORDER BY c.id
  `

  return rows.map(mapCourseRow)
}

// ─── Courses by school/program ─────────────────────────────────────────────

export async function queryCoursesByProgram(termId: string, programPrefix: string) {
  validateTermCode(termId)
  const sql = useSql()

  const rows = await sql`
    SELECT
      c.id,
      c.title,
      c.description,
      c.dupe_credit_comment,
      c.ges::text[] as ges,
      to_jsonb(c.prerequisites) as prerequisites,
      to_jsonb(c.corequisites) as corequisites,
      to_jsonb(c.registrar_code) as registrar_code,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId',       s.id::text,
        'type',            s.type::text,
        'enrolled',        s.registered_seat,
        'capacity',        s.total_seat,
        'waitlisted',      s.waitlisted_seat,
        'units',           s.units,
        'schedules',       to_jsonb(s.schedules),
        'hasDClearance',   s.d_clearance,
        'isCancelled',     s.is_cancelled
      )) as sections,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId', si.section_id::text,
        'name',      si.instructor_name
      )) FILTER (WHERE si.instructor_name IS NOT NULL) as section_instructors
    FROM ${sql.unsafe(`courses_${termId}`)} c
    JOIN ${sql.unsafe(`course_sections_${termId}`)} cs ON cs.course_id = c.id
    JOIN ${sql.unsafe(`sections_${termId}`)} s ON s.id = cs.section_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termId}`)} si ON si.section_id = s.id
    WHERE s.is_cancelled = false
      AND c.id LIKE ${programPrefix + '-%'}
    GROUP BY c.id
    ORDER BY c.id
  `

  return rows.map(mapCourseRow)
}

// ─── Single course detail ──────────────────────────────────────────────────

export async function queryCourseDetail(termId: string, courseId: string) {
  validateTermCode(termId)
  const sql = useSql()

  const rows = await sql`
    SELECT
      c.id,
      c.title,
      c.description,
      c.note,
      c.dupe_credit_comment,
      c.recom_prep_comment,
      c.ges::text[] as ges,
      c.is_cross_listed,
      to_jsonb(c.prerequisites) as prerequisites,
      to_jsonb(c.corequisites) as corequisites,
      to_jsonb(c.registrar_code) as registrar_code,
      to_jsonb(c.display_code) as display_code,
      to_jsonb(c.restrictions) as restrictions,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId',       s.id::text,
        'type',            s.type::text,
        'description',     s.description,
        'note',            s.note,
        'enrolled',        s.registered_seat,
        'capacity',        s.total_seat,
        'waitlisted',      s.waitlisted_seat,
        'units',           s.units,
        'schedules',       to_jsonb(s.schedules),
        'hasDClearance',   s.d_clearance,
        'isCancelled',     s.is_cancelled,
        'syllabus',        s.syllabus
      )) as sections,
      json_agg(DISTINCT jsonb_build_object(
        'sectionId', si.section_id::text,
        'name',      si.instructor_name
      )) FILTER (WHERE si.instructor_name IS NOT NULL) as section_instructors
    FROM ${sql.unsafe(`courses_${termId}`)} c
    JOIN ${sql.unsafe(`course_sections_${termId}`)} cs ON cs.course_id = c.id
    JOIN ${sql.unsafe(`sections_${termId}`)} s ON s.id = cs.section_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termId}`)} si ON si.section_id = s.id
    WHERE c.id = ${courseId}
    GROUP BY c.id
  `

  if (rows.length === 0) return null
  return mapCourseDetailRow(rows[0])
}

// ─── Single section detail ─────────────────────────────────────────────────

export async function querySectionDetail(termId: string, sectionId: string) {
  validateTermCode(termId)
  const sql = useSql()
  const numericId = parseInt(sectionId, 10)
  if (Number.isNaN(numericId)) {
    throw createError({ statusCode: 400, statusMessage: 'Invalid section ID' })
  }

  const rows = await sql`
    SELECT
      c.id as course_id,
      c.title,
      c.description as course_description,
      c.dupe_credit_comment,
      c.ges::text[] as ges,
      to_jsonb(c.prerequisites) as prerequisites,
      to_jsonb(c.corequisites) as corequisites,
      s.id as section_id,
      s.type::text as section_type,
      s.registered_seat as enrolled,
      s.total_seat as capacity,
      s.waitlisted_seat as waitlisted,
      s.units,
      to_jsonb(s.schedules) as schedules,
      s.d_clearance,
      s.is_cancelled,
      s.syllabus,
      array_agg(si.instructor_name) FILTER (WHERE si.instructor_name IS NOT NULL) as instructors
    FROM ${sql.unsafe(`sections_${termId}`)} s
    JOIN ${sql.unsafe(`course_sections_${termId}`)} cs ON cs.section_id = s.id
    JOIN ${sql.unsafe(`courses_${termId}`)} c ON c.id = cs.course_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termId}`)} si ON si.section_id = s.id
    WHERE s.id = ${numericId}
    GROUP BY c.id, s.id
  `

  if (rows.length === 0) return null
  return mapSectionDetailRow(rows[0])
}

// ─── Batch section details ────────────────────────────────────────────────

export async function querySectionsBatch(termId: string, sectionIds: number[]) {
  validateTermCode(termId)
  const sql = useSql()

  const rows = await sql`
    SELECT
      c.id as course_id,
      c.title,
      c.description as course_description,
      c.dupe_credit_comment,
      c.note,
      c.recom_prep_comment,
      c.ges::text[] as ges,
      to_jsonb(c.prerequisites) as prerequisites,
      to_jsonb(c.corequisites) as corequisites,
      to_jsonb(c.restrictions) as restrictions,
      s.id as section_id,
      s.type::text as section_type,
      s.registered_seat as enrolled,
      s.total_seat as capacity,
      s.waitlisted_seat as waitlisted,
      s.units,
      to_jsonb(s.schedules) as schedules,
      s.d_clearance,
      s.is_cancelled,
      s.syllabus,
      array_agg(si.instructor_name) FILTER (WHERE si.instructor_name IS NOT NULL) as instructors
    FROM ${sql.unsafe(`sections_${termId}`)} s
    JOIN ${sql.unsafe(`course_sections_${termId}`)} cs ON cs.section_id = s.id
    JOIN ${sql.unsafe(`courses_${termId}`)} c ON c.id = cs.course_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termId}`)} si ON si.section_id = s.id
    WHERE s.id = ANY(${sectionIds})
    GROUP BY c.id, s.id
  `

  return rows.map(mapSectionDetailRow)
}

// ─── Instructor lookup ─────────────────────────────────────────────────────

export async function queryInstructor(name: string) {
  const sql = useSql()
  const rows = await sql`SELECT * FROM instructors WHERE name = ${name} LIMIT 1`
  if (rows.length === 0) return null
  const r = rows[0]
  return {
    name: r.name,
    rating: r.rating != null ? r.rating / 10 : null,
    difficulty: r.difficulty != null ? r.difficulty / 10 : null,
    ratingCount: r.rating_count,
    takeAgainPercent: r.take_again_percent === -1 ? null : r.take_again_percent,
    rmpId: r.rmp_id,
  }
}

// ─── Row mappers ───────────────────────────────────────────────────────────

function attachInstructorsToSections(
  sections: any[],
  sectionInstructors: any[] | null,
): any[] {
  const bySection: Record<string, string[]> = {}
  if (sectionInstructors) {
    for (const si of sectionInstructors) {
      if (!si.sectionId || !si.name) continue
      const key = String(si.sectionId)
      if (!bySection[key]) bySection[key] = []
      if (!bySection[key].includes(si.name)) bySection[key].push(si.name)
    }
  }
  return sections.map((s: any) => ({
    ...s,
    instructors: bySection[String(s.sectionId)] || [],
  }))
}

function mapCourseRow(row: any) {
  const sections = attachInstructorsToSections(
    row.sections || [],
    row.section_instructors,
  )

  return {
    title: row.title,
    code: row.id,
    description: row.description || '',
    ges: row.ges || [],
    sections: sections.map((s: any) => ({
      sectionId: String(s.sectionId),
      instructors: s.instructors,
      enrolled: s.enrolled ?? 0,
      capacity: s.capacity ?? 0,
      waitlisted: s.waitlisted ?? 0,
      schedules: s.schedules || [],
      hasDClearance: s.hasDClearance ?? false,
      hasPrerequisites: (row.prerequisites?.length ?? 0) > 0,
      hasDuplicatedCredit: !!row.dupe_credit_comment,
      units: s.units || [],
      type: s.type || null,
      isCancelled: s.isCancelled ?? false,
    })),
  }
}

function mapCourseDetailRow(row: any) {
  const sections = attachInstructorsToSections(
    row.sections || [],
    row.section_instructors,
  )

  const allInstructors = [...new Set(sections.flatMap((s: any) => s.instructors))]
  const allSchedules = deduplicateSchedules(sections.flatMap((s: any) => s.schedules || []))
  const allUnits = [...new Set(sections.flatMap((s: any) => s.units || []))].sort((a, b) => a - b)
  const totalEnrolled = sections.reduce((sum: number, s: any) => sum + (s.enrolled ?? 0), 0)
  const totalCapacity = sections.reduce((sum: number, s: any) => sum + (s.capacity ?? 0), 0)
  const totalWaitlisted = sections.reduce((sum: number, s: any) => sum + (s.waitlisted ?? 0), 0)

  return {
    title: row.title,
    code: row.id,
    description: row.description || '',
    instructors: allInstructors,
    units: allUnits,
    enrolled: totalEnrolled,
    capacity: totalCapacity,
    waitlisted: totalWaitlisted,
    schedules: allSchedules,
    dupeCreditComment: row.dupe_credit_comment || null,
    prerequisites: row.prerequisites || [],
    corequisites: row.corequisites || [],
    restrictions: row.restrictions || [],
    note: row.note || null,
    recomPrepComment: row.recom_prep_comment || null,
    dClearance: sections.some((s: any) => s.hasDClearance),
    type: sections[0]?.type || null,
    ges: row.ges || [],
    isCancelled: sections.every((s: any) => s.isCancelled),
  }
}

function mapSectionDetailRow(row: any) {
  return {
    sectionId: String(row.section_id),
    title: row.title,
    code: row.course_id,
    description: row.course_description || '',
    instructors: row.instructors || [],
    units: row.units || [],
    enrolled: row.enrolled ?? 0,
    capacity: row.capacity ?? 0,
    waitlisted: row.waitlisted ?? 0,
    schedules: row.schedules || [],
    dupeCreditComment: row.dupe_credit_comment || null,
    prerequisites: row.prerequisites || [],
    corequisites: row.corequisites || [],
    restrictions: row.restrictions || [],
    note: row.note || null,
    recomPrepComment: row.recom_prep_comment || null,
    dClearance: row.d_clearance ?? false,
    type: row.section_type || null,
    ges: row.ges || [],
    isCancelled: row.is_cancelled ?? false,
  }
}

function deduplicateSchedules(schedules: any[]): any[] {
  const seen = new Set<string>()
  const result: any[] = []
  for (const s of schedules) {
    const key = JSON.stringify(s)
    if (!seen.has(key)) {
      seen.add(key)
      result.push(s)
    }
  }
  return result
}

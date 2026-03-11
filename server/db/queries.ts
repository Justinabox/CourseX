import { useSql } from '~~/server/db'
import { validateTermCode } from '~~/server/utils/termValidator'

// ─── Terms ────────────────────────────────────────────────────────────────

export async function queryTerms() {
  const sql = useSql()
  const rows = await sql`SELECT term_code, season, year, status FROM terms ORDER BY term_code DESC`
  return rows.map((r: any) => ({
    termCode: r.term_code as number,
    season: r.season as string,
    year: r.year as number,
    status: r.status as string,
  }))
}

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
        'sectionTitle',    s.title,
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

  return rows.flatMap(splitCourseRowByTitle)
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
        'sectionTitle',    s.title,
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

  return rows.flatMap(splitCourseRowByTitle)
}

// ─── Single course detail ──────────────────────────────────────────────────

export async function queryCourseDetail(termId: string, courseId: string, title?: string) {
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
        'sectionTitle',    s.title,
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

  const row = rows[0]!
  if (title) {
    const titleUpper = title.trim().toUpperCase()
    const sections = row.sections || []
    row.sections = sections.filter((s: any) => {
      const effectiveTitle = ((s.sectionTitle || '').trim() || row.title).toUpperCase()
      return effectiveTitle === titleUpper
    })
    if (row.sections.length > 0) {
      return mapCourseDetailRow(row, title.trim())
    }
  }
  return mapCourseDetailRow(row)
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
      s.title as section_title,
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
      s.title as section_title,
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

function splitCourseRowByTitle(row: any): any[] {
  const sections = attachInstructorsToSections(row.sections || [], row.section_instructors)

  const groups: Record<string, { title: string; sections: any[] }> = {}
  for (const s of sections) {
    const effectiveTitle = (s.sectionTitle || '').trim() || row.title
    const groupKey = effectiveTitle.toUpperCase()
    if (!groups[groupKey]) groups[groupKey] = { title: effectiveTitle, sections: [] }
    groups[groupKey].sections.push(s)
  }

  return Object.values(groups).map(group => ({
    title: group.title,
    code: row.id,
    description: row.description || '',
    ges: row.ges || [],
    sections: group.sections.map((s: any) => ({
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

function mapCourseDetailRow(row: any, titleOverride?: string) {
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
    title: titleOverride || row.title,
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
    syllabus: sections.find((s: any) => s.syllabus)?.syllabus || null,
  }
}

function mapSectionDetailRow(row: any) {
  return {
    sectionId: String(row.section_id),
    title: (row.section_title || '').trim() || row.title,
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
    syllabus: row.syllabus || null,
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

// ─── Hydrated user data queries ───────────────────────────────────────────

export async function queryWatchlistCourses(termCode: string, courseKeys: string[]) {
  if (courseKeys.length === 0) return []
  validateTermCode(termCode)
  const sql = useSql()

  // Extract course codes from keys (format: "CODE::TITLE")
  const courseCodes = courseKeys
    .map((k) => k.split('::')[0])
    .filter(Boolean)

  if (courseCodes.length === 0) return []

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
        'sectionTitle',    s.title,
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
    FROM ${sql.unsafe(`courses_${termCode}`)} c
    JOIN ${sql.unsafe(`course_sections_${termCode}`)} cs ON cs.course_id = c.id
    JOIN ${sql.unsafe(`sections_${termCode}`)} s ON s.id = cs.section_id
    LEFT JOIN ${sql.unsafe(`section_instructors_${termCode}`)} si ON si.section_id = s.id
    WHERE s.is_cancelled = false
      AND c.id = ANY(${courseCodes})
    GROUP BY c.id
    ORDER BY c.id
  `

  return rows.flatMap(splitCourseRowByTitle)
}

export function hydrateScheduleCourses(sectionDetails: any[]) {
  const byKey: Record<string, any> = {}
  for (const d of sectionDetails) {
    const code = d.code
    const title = (d.title || '').toString().trim()
    const titleUpper = title.toUpperCase()
    const key = `${code}::${titleUpper}`

    if (!byKey[key]) {
      byKey[key] = {
        title,
        code: d.code,
        description: d.description || '',
        ges: d.ges || [],
        sections: [],
      }
    }

    byKey[key].sections.push({
      sectionId: d.sectionId,
      instructors: d.instructors || [],
      enrolled: d.enrolled ?? 0,
      capacity: d.capacity ?? 0,
      waitlisted: d.waitlisted ?? 0,
      schedules: d.schedules || [],
      hasDClearance: d.dClearance ?? false,
      hasPrerequisites: (d.prerequisites?.length ?? 0) > 0,
      hasDuplicatedCredit: !!d.dupeCreditComment,
      units: d.units || [],
      type: d.type ?? null,
      isCancelled: d.isCancelled ?? false,
    })
  }
  return Object.values(byKey)
}

// ─── User management ──────────────────────────────────────────────────────

export async function upsertUser(googleId: string, email: string, name: string, picture: string | null): Promise<number> {
  const sql = useSql()
  const rows = await sql`
    INSERT INTO users (google_id, email, name, picture)
    VALUES (${googleId}, ${email}, ${name}, ${picture})
    ON CONFLICT (google_id) DO UPDATE SET
      name = EXCLUDED.name,
      picture = EXCLUDED.picture,
      last_login_at = now()
    RETURNING id
  `
  return rows[0].id
}

export async function getUserIdByGoogleId(googleId: string): Promise<number | null> {
  const sql = useSql()
  const rows = await sql`SELECT id FROM users WHERE google_id = ${googleId} LIMIT 1`
  return rows.length > 0 ? rows[0].id : null
}

// ─── Watchlist CRUD ───────────────────────────────────────────────────────

export async function getWatchlistKeys(userId: number, termCode: string): Promise<string[]> {
  const sql = useSql()
  const rows = await sql`
    SELECT course_keys FROM user_watchlist
    WHERE user_id = ${userId} AND term_code = ${termCode}
  `
  return rows.length > 0 ? (rows[0].course_keys || []) : []
}

export async function replaceWatchlist(userId: number, termCode: string, keys: string[]): Promise<string[]> {
  const sql = useSql()
  const rows = await sql`
    INSERT INTO user_watchlist (user_id, term_code, course_keys, updated_at)
    VALUES (${userId}, ${termCode}, ${keys}, now())
    ON CONFLICT (user_id, term_code) DO UPDATE SET
      course_keys = EXCLUDED.course_keys,
      updated_at = now()
    RETURNING course_keys
  `
  return rows[0].course_keys || []
}

export async function addWatchlistKey(userId: number, termCode: string, key: string): Promise<string[]> {
  const sql = useSql()
  const rows = await sql`
    INSERT INTO user_watchlist (user_id, term_code, course_keys, updated_at)
    VALUES (${userId}, ${termCode}, ARRAY[${key}]::text[], now())
    ON CONFLICT (user_id, term_code) DO UPDATE SET
      course_keys = array_append(array_remove(user_watchlist.course_keys, ${key}), ${key}),
      updated_at = now()
    RETURNING course_keys
  `
  return rows[0].course_keys || []
}

export async function removeWatchlistKey(userId: number, termCode: string, key: string): Promise<string[]> {
  const sql = useSql()
  const rows = await sql`
    UPDATE user_watchlist SET
      course_keys = array_remove(course_keys, ${key}),
      updated_at = now()
    WHERE user_id = ${userId} AND term_code = ${termCode}
    RETURNING course_keys
  `
  return rows.length > 0 ? (rows[0].course_keys || []) : []
}

// ─── Schedule CRUD ────────────────────────────────────────────────────────

export async function getScheduleSectionIds(userId: number, termCode: string): Promise<number[]> {
  const sql = useSql()
  const rows = await sql`
    SELECT section_ids FROM user_schedule
    WHERE user_id = ${userId} AND term_code = ${termCode}
  `
  return rows.length > 0 ? (rows[0].section_ids || []) : []
}

export async function replaceSchedule(userId: number, termCode: string, sectionIds: number[]): Promise<number[]> {
  const sql = useSql()
  const rows = await sql`
    INSERT INTO user_schedule (user_id, term_code, section_ids, updated_at)
    VALUES (${userId}, ${termCode}, ${sectionIds}, now())
    ON CONFLICT (user_id, term_code) DO UPDATE SET
      section_ids = EXCLUDED.section_ids,
      updated_at = now()
    RETURNING section_ids
  `
  return rows[0].section_ids || []
}

export async function addScheduleSectionId(userId: number, termCode: string, sectionId: number): Promise<number[]> {
  const sql = useSql()
  const rows = await sql`
    INSERT INTO user_schedule (user_id, term_code, section_ids, updated_at)
    VALUES (${userId}, ${termCode}, ARRAY[${sectionId}]::integer[], now())
    ON CONFLICT (user_id, term_code) DO UPDATE SET
      section_ids = array_append(array_remove(user_schedule.section_ids, ${sectionId}), ${sectionId}),
      updated_at = now()
    RETURNING section_ids
  `
  return rows[0].section_ids || []
}

export async function removeScheduleSectionId(userId: number, termCode: string, sectionId: number): Promise<number[]> {
  const sql = useSql()
  const rows = await sql`
    UPDATE user_schedule SET
      section_ids = array_remove(section_ids, ${sectionId}),
      updated_at = now()
    WHERE user_id = ${userId} AND term_code = ${termCode}
    RETURNING section_ids
  `
  return rows.length > 0 ? (rows[0].section_ids || []) : []
}

// ─── Pipeline Metadata ──────────────────────────────────────────────────────

export async function queryPipelineMetaTimestamp(key: string): Promise<string | null> {
  const sql = useSql()
  const rows = await sql`SELECT updated_at FROM pipeline_meta WHERE key = ${key}`
  return rows.length > 0 ? (rows[0].updated_at as string) : null
}

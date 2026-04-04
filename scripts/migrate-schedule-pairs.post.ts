import { neon } from '@neondatabase/serverless'

/**
 * One-time migration: converts user_schedule.section_ids → schedule_pairs.
 * Run with: bun server/api/admin/migrate-schedule-pairs.post.ts
 * DELETE THIS FILE after running.
 */

const url = process.env.DATABASE_URL
if (!url) {
  console.error('DATABASE_URL not set. Run with: DATABASE_URL=... bun server/api/admin/migrate-schedule-pairs.post.ts')
  process.exit(1)
}

const sql = neon(url)

const rows = await sql`
  SELECT user_id, term_code, section_ids
  FROM user_schedule
  WHERE array_length(section_ids, 1) > 0
    AND (schedule_pairs IS NULL OR array_length(schedule_pairs, 1) IS NULL)
`

if (rows.length === 0) {
  console.log('No rows to migrate.')
  process.exit(0)
}

console.log(`Found ${rows.length} row(s) to migrate.\n`)

let migrated = 0
let skipped = 0

for (const row of rows) {
  const userId = row.user_id as number
  const termCode = String(row.term_code)
  const sectionIds: number[] = (row.section_ids || []) as number[]

  if (!/^\d{5}$/.test(termCode)) {
    console.log(`  SKIP user=${userId} term=${termCode} — invalid term code`)
    skipped++
    continue
  }

  try {
    // Look up course_id for each section_id (DISTINCT ON picks first for crosslisted)
    const mappings = await sql`
      SELECT DISTINCT ON (section_id) section_id, course_id
      FROM ${sql.unsafe(`course_sections_${termCode}`)}
      WHERE section_id = ANY(${sectionIds})
      ORDER BY section_id, course_id
    `

    const courseMap = new Map<number, string>()
    for (const m of mappings) {
      courseMap.set(m.section_id as number, m.course_id as string)
    }

    const pairs: string[] = []
    const unmapped: number[] = []
    for (const sid of sectionIds) {
      const courseId = courseMap.get(sid)
      if (courseId) {
        pairs.push(`${courseId}:${sid}`)
      } else {
        unmapped.push(sid)
      }
    }

    await sql`
      UPDATE user_schedule
      SET schedule_pairs = ${pairs}::text[],
          section_ids = '{}',
          updated_at = now()
      WHERE user_id = ${userId} AND term_code = ${termCode}
    `

    console.log(`  OK   user=${userId} term=${termCode} — ${pairs.length} pair(s): [${pairs.join(', ')}]`)
    if (unmapped.length > 0) {
      console.log(`       ⚠ ${unmapped.length} section(s) not found in course_sections: [${unmapped.join(', ')}]`)
    }
    migrated++
  } catch (e: any) {
    console.log(`  ERR  user=${userId} term=${termCode} — ${e?.message || e}`)
    skipped++
  }
}

console.log(`\nDone. Migrated: ${migrated}, Skipped: ${skipped}`)

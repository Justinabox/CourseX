import { neon, type NeonQueryFunction } from '@neondatabase/serverless'

let _sql: NeonQueryFunction<false, false> | null = null

export function useSql() {
  if (!_sql) {
    const url = useRuntimeConfig().databaseUrl || process.env.DATABASE_URL || ''
    if (!url) {
      throw createError({
        statusCode: 500,
        statusMessage: 'DATABASE_URL is not configured',
      })
    }
    _sql = neon(url)
  }
  return _sql
}

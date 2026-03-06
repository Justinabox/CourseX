import { neon, type NeonQueryFunction } from '@neondatabase/serverless'

let _sql: NeonQueryFunction<false, false> | null = null

export function useSql() {
  if (!_sql) {
    const config = useRuntimeConfig()
    _sql = neon(config.databaseUrl)
  }
  return _sql
}

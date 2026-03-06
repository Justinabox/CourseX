import { computed } from 'vue'

export type ModeAll = { mode: 'all'; courseCode: string | null; sectionId: string | null }
export type ModeScheduled = { mode: 'scheduled'; courseCode: string | null; sectionId: string | null }
export type ModeWatchlist = { mode: 'watchlist'; courseCode: string | null; sectionId: string | null }
export type ModeProgram = { mode: 'program'; school: string; program: string; courseCode: string | null; sectionId: string | null }
export type ModeUnknown = { mode: 'unknown' }
export type RouteMode = ModeAll | ModeScheduled | ModeWatchlist | ModeProgram | ModeUnknown

export function useRouteMode() {
  const route = useRoute()
  const term = computed(() => (route.params as any)?.termId as string | undefined)

  const mode = computed<RouteMode>(() => {
    const parts = (route.params.slug as string[] | undefined) || []
    if (parts.length === 0) return { mode: 'unknown' as const }
    if (parts[0] === 'all') {
      return { mode: 'all' as const, courseCode: parts[1] || null, sectionId: parts[2] || null }
    }
    if (parts[0] === 'scheduled') {
      return { mode: 'scheduled' as const, courseCode: parts[1] || null, sectionId: parts[2] || null }
    }
    if (parts[0] === 'watchlist') {
      return { mode: 'watchlist' as const, courseCode: parts[1] || null, sectionId: parts[2] || null }
    }
    if (parts.length >= 2) {
      const school = parts[0] as string
      const program = parts[1] as string
      return { mode: 'program' as const, school, program, courseCode: parts[2] || null, sectionId: parts[3] || null }
    }
    return { mode: 'unknown' as const }
  })

  function makeSelectionPath(target: RouteMode, code: string, sectionId: string | null): string {
    const t = term.value || '20261'
    if (target.mode === 'all' || target.mode === 'unknown') {
      return `/course/${t}/all/${encodeURIComponent(code)}/${encodeURIComponent(sectionId || 'section')}`
    }
    if (target.mode === 'scheduled') {
      return `/course/${t}/scheduled/${encodeURIComponent(code)}/${encodeURIComponent(sectionId || 'section')}`
    }
    if (target.mode === 'watchlist') {
      return `/course/${t}/watchlist/${encodeURIComponent(code)}/${encodeURIComponent(sectionId || 'section')}`
    }
    return `/course/${t}/${encodeURIComponent(target.school)}/${encodeURIComponent(target.program)}/${encodeURIComponent(code)}/${encodeURIComponent(sectionId || 'section')}`
  }

  const scopeKey = computed<string>(() => {
    const parts = (route.params.slug as string[] | undefined) || []
    if (parts[0] === 'all') return 'all'
    if (parts[0] === 'scheduled') return 'scheduled'
    if (parts[0] === 'watchlist') return 'watchlist'
    if ((parts?.length || 0) >= 2) return `${parts[0]}/${parts[1]}`
    return 'unknown'
  })

  return { mode, makeSelectionPath, scopeKey }
}

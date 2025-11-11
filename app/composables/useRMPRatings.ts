import { onMounted } from 'vue'

type RawProfessor = {
  id?: number
  duplicated?: boolean
  difficulty?: number
  rating?: number
  rating_count?: number
  take_again?: number
}

export type Professor = {
  link: string
  duplicated: boolean
  difficulty: number
  rating: number
  rating_count: number
  take_again: number
}

type ProfessorsByName = Record<string, RawProfessor>

function normalizeWhitespace(input: string): string {
  return input.trim().replace(/\s+/g, ' ')
}

function normalizeKey(input: string): string {
  return normalizeWhitespace(input).toLowerCase()
}

function buildSearchNameVariants(name: string): string[] {
  const normalized = normalizeWhitespace(name)
  const parts = normalized.split(' ')
  if (parts.length === 1) return [normalizeKey(normalized)]
  // Build a reversed variant where last token is last name and the rest are given names
  const last = parts[parts.length - 1]
  const first = parts.slice(0, parts.length - 1).join(' ')
  const reversed = `${last} ${first}`
  return [normalizeKey(normalized), normalizeKey(reversed)]
}

function toProfessor(name: string, raw: RawProfessor): Professor {
  const isDuplicated = raw.duplicated === true
  const id = raw.id
  const trimmedName = normalizeWhitespace(name)
  const link = !isDuplicated && typeof id === 'number'
    ? `https://www.ratemyprofessors.com/professor/${id}`
    : `https://www.ratemyprofessors.com/search/professors?q=${encodeURIComponent(trimmedName)}`

  return {
    link,
    duplicated: isDuplicated,
    difficulty: typeof raw.difficulty === 'number' ? raw.difficulty : NaN,
    rating: typeof raw.rating === 'number' ? raw.rating : NaN,
    rating_count: typeof raw.rating_count === 'number' ? Math.trunc(raw.rating_count) : 0,
    take_again: typeof raw.take_again === 'number' ? Math.round(raw.take_again) : 0
  }
}

async function loadProfessorsJSON(): Promise<ProfessorsByName> {
  if (!process.client) return {}
  const res = await fetch('/data/professors.json', { cache: 'force-cache' })
  if (!res.ok) return {}
  const data = (await res.json()) as ProfessorsByName
  return data || {}
}

export function useRMPRatings() {
  const rawData = useState<ProfessorsByName>('rmp-professors-raw', () => ({}))
  const index = useState<Record<string, RawProfessor>>('rmp-professors-index', () => ({}))
  const loaded = useState<boolean>('rmp-professors-loaded', () => false)
  const loading = useState<boolean>('rmp-professors-loading', () => false)

  async function ensureLoaded(): Promise<void> {
    if (loaded.value || loading.value) return
    loading.value = true
    try {
      const data = await loadProfessorsJSON()
      rawData.value = data

      // Build a fast lookup index supporting both "Last First" and "First Last" queries
      const nextIndex: Record<string, RawProfessor> = {}
      for (const [key, value] of Object.entries(data)) {
        const normalizedKey = normalizeKey(key)
        nextIndex[normalizedKey] = value

        // Also add reversed tokenization to support FirstName LastName inputs
        const parts = normalizeWhitespace(key).split(' ')
        if (parts.length > 1) {
          const last = parts[0]
          const first = parts.slice(1).join(' ')
          const reversed = `${first} ${last}`
          nextIndex[normalizeKey(reversed)] = value
        }
      }
      index.value = nextIndex
      loaded.value = true
    } finally {
      loading.value = false
    }
  }

  onMounted(() => {
    // Load on page load (client mount)
    if (process.client) void ensureLoaded()
  })

  async function getProfessor(name: string): Promise<Professor | null> {
    if (!name) return null
    await ensureLoaded()
    const variants = buildSearchNameVariants(name)
    for (const variant of variants) {
      const raw = index.value[variant]
      if (raw) return toProfessor(name, raw)
    }
    return null
  }

  return {
    getProfessor
  }
}











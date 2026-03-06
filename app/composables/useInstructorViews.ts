export type InstructorView = {
  name: string
  rating: number
  link: string
  isLow: boolean
}

type GetProfessorFn = (name: string) => Promise<{ rating?: number | null; link?: string | null } | null | undefined>

export async function resolveInstructorViews(names: string[], getProfessor: GetProfessorFn): Promise<InstructorView[]> {
  const unique = Array.from(new Set(names.filter(Boolean)))
  if (unique.length === 0) return []
  return Promise.all(
    unique.map(async (name) => {
      const prof = await getProfessor(name)
      const rating = prof && typeof prof.rating === 'number' && !Number.isNaN(prof.rating) ? prof.rating : NaN
      const link = prof?.link || `https://www.ratemyprofessors.com/search/professors?q=${encodeURIComponent(name)}`
      const isLow = typeof rating === 'number' && !Number.isNaN(rating) ? rating < 3.0 : false
      return { name, rating, link, isLow }
    })
  )
}

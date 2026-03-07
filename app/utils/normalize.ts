export function normalizeString(value: string | null | undefined): string {
  return (value || '').toString().trim().toLowerCase()
}


export function normalizeCourseCode(value: string | null | undefined): string {
  const s = (value || '').toString().trim().toUpperCase()
  return s
}

export function normalizeSectionId(value: string | null | undefined): string {
  const s = (value || '').toString().trim().toUpperCase()
  return s
}

export function normalizeSectionType(raw: string | null | undefined): string {
  const t = normalizeString(raw)
  if (!t) return ''
  // Handle common shorthands
  if (t === 'disc' || t === 'dis' || t === 'discussion') return 'discussion'
  if (t === 'lec' || t === 'lecture') return 'lecture'
  if (t === 'lab') return 'lab'
  // Handle composite types like "lecture/lab", "lec/lab", "lecture & lab"
  const composite = t.replace(/\s+/g, '')
  if (/(^|[^a-z])(lec|lecture)\/(lab)([^a-z]|$)/.test(composite) || /(^|[^a-z])(lecture)(and|&|\/)\s*(lab)([^a-z]|$)/.test(t)) return 'lecture'
  if (/(^|[^a-z])(lab)\/(lec|lecture)([^a-z]|$)/.test(composite) || /(^|[^a-z])(lab)(and|&|\/)\s*(lecture)([^a-z]|$)/.test(t)) return 'lecture'
  return t
}

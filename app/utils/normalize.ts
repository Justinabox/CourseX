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
  if (t === 'disc' || t === 'dis' || t === 'discussion') return 'discussion'
  if (t === 'lec' || t === 'lecture') return 'lecture'
  if (t === 'lab') return 'lab'
  if (t === 'qz' || t === 'quiz') return 'quiz'
  const composite = t.replace(/\s+/g, '')
  if (/\b(lec(ture)?)\b/.test(composite)) return 'lecture'
  if (/\b(lab)\b/.test(composite)) return 'lab'
  if (/\b(dis(c(ussion)?)?)\b/.test(composite)) return 'discussion'
  return t
}

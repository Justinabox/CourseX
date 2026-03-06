const TERM_CODE_RE = /^\d{5}$/

export function validateTermCode(termCode: string): string {
  if (!TERM_CODE_RE.test(termCode)) {
    throw createError({ statusCode: 400, statusMessage: 'Invalid term code' })
  }
  return termCode
}

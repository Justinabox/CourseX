/** @deprecated Legacy helper for single-value units. Use parseUnitsArrayToRange instead. */
export function parseUnitsToNumber(units: number | string | null | undefined): number {
  if (units == null) return 0
  if (typeof units === 'number') return Number.isFinite(units) ? units : 0
  const m = (units || '').toString().match(/-?\d+(?:\.\d+)?/)
  return m ? parseFloat(m[0]) : 0
}

export function parseUnitsArrayToRange(units: number[]): { min: number; max: number } {
  if (!units || units.length === 0) return { min: 0, max: 0 }
  let min = units[0]
  let max = units[0]
  for (let i = 1; i < units.length; i++) {
    if (units[i] < min) min = units[i]
    if (units[i] > max) max = units[i]
  }
  return { min, max }
}

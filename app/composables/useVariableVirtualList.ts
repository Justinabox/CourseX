import { computed, ref, shallowRef, watch, onScopeDispose } from 'vue'
import type { Ref } from 'vue'

type KeyGetter<T> = (item: T, index: number) => string

export function useVariableVirtualList<T>(params: {
  items: Ref<T[]>
  estimateItemHeight?: number
  getKey: KeyGetter<T>
}) {
  const estimate = Math.max(params.estimateItemHeight || 120, 1)
  const items = params.items
  const getKey = params.getKey

  const containerRef = ref<HTMLElement | null>(null)
  const window = shallowRef({ start: 0, end: 0 })

  // Non-reactive height cache. Mutations here do NOT trigger Vue re-renders.
  // Heights are consumed when window/items change (both reactive), so padding
  // stays accurate on every scroll frame without creating render cascades.
  const keyToHeight: Record<string, number> = {}

  const indexToKey = computed<string[]>(() => {
    const list = items.value || []
    return list.map((it, i) => getKey(it, i))
  })
  const bufferPx = 3 * estimate

  const totalContentHeight = computed(() => {
    void window.value
    let total = 0
    for (const k of indexToKey.value) total += keyToHeight[k] || estimate
    return total
  })

  const sumByIndex = (from: number, toExclusive: number) => {
    const keys = indexToKey.value
    let total = 0
    for (let i = from; i < toExclusive; i++) {
      const k = keys[i]
      total += (k && keyToHeight[k]) || estimate
    }
    return total
  }

  const topPadding = computed(() => {
    const w = window.value
    return sumByIndex(0, w.start)
  })

  const bottomPadding = computed(() => {
    const w = window.value
    const visibleHeight = sumByIndex(w.start, w.end)
    const remaining = Math.max(totalContentHeight.value - topPadding.value - visibleHeight, 0)
    return remaining
  })

  const visibleItems = computed(() => {
    const list = items.value || []
    const w = window.value
    const start = Math.min(w.start, list.length)
    const end = Math.min(w.end, list.length)
    return list.slice(start, end)
  })

  function findStartIndexForScroll(scrollTop: number): number {
    const keys = indexToKey.value
    const n = keys.length
    let acc = 0
    const target = Math.max(scrollTop - bufferPx, 0)
    for (let i = 0; i < n; i++) {
      const k = keys[i]
      const h = (k && keyToHeight[k]) || estimate
      if (acc + h > target) return i
      acc += h
    }
    return n
  }

  function findEndIndexForViewport(start: number, viewportHeight: number, initialOffsetPx: number): number {
    const keys = indexToKey.value
    const n = keys.length
    let acc = 0
    const target = viewportHeight + bufferPx + Math.max(initialOffsetPx, 0)
    let i = start
    while (i < n && acc < target) {
      const k = keys[i]
      const h = (k && keyToHeight[k]) || estimate
      acc += h
      i++
    }
    return i
  }

  function computeWindow(): { start: number; end: number } | null {
    const container = containerRef.value
    if (!container) return null
    const height = container.clientHeight || 0
    const scrollTop = container.scrollTop || 0
    const len = (items.value || []).length
    const first = Math.min(Math.max(findStartIndexForScroll(scrollTop), 0), len)
    const prefixBeforeFirst = sumByIndex(0, first)
    const intraFirstOffset = Math.max(scrollTop - prefixBeforeFirst, 0)
    const last = Math.min(findEndIndexForViewport(first, height, intraFirstOffset), len)
    return { start: first, end: last }
  }

  let updating = false
  let heightsDirty = false

  const updateViewport = () => {
    if (updating) return
    updating = true
    try {
      const next = computeWindow()
      if (!next) return
      const cur = window.value
      const forceUpdate = heightsDirty
      heightsDirty = false
      if (forceUpdate || cur.start !== next.start || cur.end !== next.end) {
        window.value = next
      }
    } finally {
      updating = false
    }
  }

  // Single RAF-based scheduler for all viewport updates (client-only)
  let rafId: number | null = null
  const scheduleUpdateViewport = () => {
    if (typeof requestAnimationFrame === 'undefined') return
    if (rafId != null) return
    rafId = requestAnimationFrame(() => {
      rafId = null
      updateViewport()
    })
  }

  onScopeDispose(() => {
    if (rafId != null) {
      cancelAnimationFrame(rafId)
      rafId = null
    }
  })

  function onRowMeasure(el: Element | any | null, key: string) {
    if (!el || !key) return
    const rootEl = (el as any)?.$el ? ((el as any).$el as HTMLElement) : (el as HTMLElement)
    if (!rootEl || !(rootEl instanceof HTMLElement)) return
    const rect = rootEl.getBoundingClientRect()
    const h = Math.max(Math.ceil(rect.height), 1)
    if (!Number.isFinite(h) || h <= 0) return
    const prev = keyToHeight[key]
    keyToHeight[key] = h
    if (prev !== h) {
      heightsDirty = true
      scheduleUpdateViewport()
    }
  }

  watch(items, () => {
    const len = (items.value || []).length
    const cur = window.value
    if (cur.start > len || cur.end > len) {
      window.value = { start: 0, end: Math.min(cur.end, len) }
    }
    scheduleUpdateViewport()
  }, { immediate: true })

  return {
    containerRef,
    topPadding,
    bottomPadding,
    visibleItems,
    onRowMeasure,
    updateViewport,
    scheduleUpdateViewport,
  }
}

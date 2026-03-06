import { onActivated, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from 'vue'

export function useScrollMemory(scopeKey: (() => string) | string) {
  const containerRef = ref<HTMLElement | null>(null)
  const mapState = useState<Record<string, number>>('ui:scroll:generic', () => ({}))

  const getKey = () => {
    const k = typeof scopeKey === 'function' ? scopeKey() : scopeKey
    return Array.isArray(k) ? k.join('/') : k
  }

  const restore = () => {
    const el = containerRef.value
    if (!el) return
    const key = getKey()
    const saved = mapState.value[key] || 0
    if (saved > 0) el.scrollTop = saved
  }

  const persist = () => {
    const el = containerRef.value
    if (!el) return
    const key = getKey()
    const top = el.scrollTop || 0
    mapState.value = { ...mapState.value, [key]: top }
  }

  const onScroll = () => {
    // direct persist; higher layers can throttle if needed
    persist()
  }

  onMounted(() => {
    const el = containerRef.value
    if (!el) return
    // Restore first to avoid emitting a scroll during hydration/mount
    restore()
    el.addEventListener('scroll', onScroll, { passive: true })
  })

  onBeforeUnmount(() => {
    const el = containerRef.value
    if (!el) return
    persist()
    el.removeEventListener('scroll', onScroll)
  })

  // Support keepalive: restore on reactivation, persist on deactivation
  onActivated(() => {
    const el = containerRef.value
    if (!el) return
    restore()
    el.addEventListener('scroll', onScroll, { passive: true })
  })

  onDeactivated(() => {
    const el = containerRef.value
    if (!el) return
    persist()
    el.removeEventListener('scroll', onScroll)
  })

  if (typeof scopeKey === 'function') {
    watch(scopeKey, () => {
      restore()
    })
  }

  return {
    containerRef,
    restore,
    persist,
  }
}



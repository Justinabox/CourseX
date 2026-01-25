<template>
  <div class="w-full h-full flex flex-col lg:flex-row">
    <div class="lg:hidden border-b border-cx-border bg-cx-background/80 backdrop-blur">
      <div class="flex items-center justify-between px-3 pt-3">
        <div class="font-serif text-lg text-transparent bg-clip-text bg-gradient-to-r from-cx-brand-start to-cx-brand-end">
          CourseX
        </div>
        <span class="text-[10px] text-cx-text-muted">Swipe to switch</span>
      </div>
      <div class="grid grid-cols-3 gap-2 px-3 pb-3 pt-2">
        <button
          v-for="(tab, idx) in tabs"
          :key="tab.key"
          class="min-h-11 rounded-md text-sm font-medium border border-cx-border/60 transition-colors"
          :class="idx === activePanel ? 'bg-cx-surface-800/70 text-cx-text-subtle' : 'bg-transparent text-cx-text-muted hover:bg-cx-surface-800/30'"
          :aria-pressed="idx === activePanel"
          @click="setActive(idx)"
        >
          {{ tab.label }}
        </button>
      </div>
    </div>

    <div
      class="flex-1 overflow-hidden lg:overflow-visible touch-pan-y"
      @touchstart.passive="onTouchStart"
      @touchmove.passive="onTouchMove"
      @touchend="onTouchEnd"
    >
      <div
        class="flex h-full transition-transform duration-300 ease-out lg:transition-none"
        :style="panelStyle"
      >
        <section class="w-full shrink-0 h-full min-w-0 lg:w-auto lg:flex-none">
          <LeftPanel />
        </section>
        <section class="w-full shrink-0 h-full min-w-0 lg:w-auto lg:flex-none">
          <MiddlePanel />
        </section>
        <section class="w-full shrink-0 h-full min-w-0 lg:flex-1">
          <RightPanel :is-mobile="isMobile" />
        </section>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import LeftPanel from '@/components/LeftPanel.vue'
import MiddlePanel from '@/components/MiddlePanel.vue'
import RightPanel from '@/components/RightPanel.vue'

const tabs = [
  { key: 'programs', label: 'Programs' },
  { key: 'courses', label: 'Courses' },
  { key: 'schedule', label: 'Schedule' },
]

const activePanel = ref(1)
const isMobile = ref(false)
let mediaQuery: MediaQueryList | null = null

const updateIsMobile = (event?: MediaQueryListEvent | MediaQueryList) => {
  if (event && 'matches' in event) {
    isMobile.value = event.matches
    return
  }
  if (mediaQuery) isMobile.value = mediaQuery.matches
}

onMounted(() => {
  if (!window.matchMedia) return
  mediaQuery = window.matchMedia('(max-width: 1023px)')
  updateIsMobile(mediaQuery)
  mediaQuery.addEventListener('change', updateIsMobile)
})

onBeforeUnmount(() => {
  if (mediaQuery) mediaQuery.removeEventListener('change', updateIsMobile)
})

const setActive = (idx: number) => {
  activePanel.value = Math.min(Math.max(idx, 0), tabs.length - 1)
}

const panelStyle = computed(() => {
  if (!isMobile.value) return { transform: 'translateX(0%)' }
  return { transform: `translateX(-${activePanel.value * 100}%)` }
})

const touchStartX = ref(0)
const touchStartY = ref(0)
const isSwiping = ref(false)

const onTouchStart = (e: TouchEvent) => {
  if (!isMobile.value || e.touches.length === 0) return
  const touch = e.touches[0]
  touchStartX.value = touch.clientX
  touchStartY.value = touch.clientY
  isSwiping.value = false
}

const onTouchMove = (e: TouchEvent) => {
  if (!isMobile.value || e.touches.length === 0) return
  const touch = e.touches[0]
  const dx = touch.clientX - touchStartX.value
  const dy = touch.clientY - touchStartY.value
  if (!isSwiping.value && Math.abs(dx) > 20 && Math.abs(dx) > Math.abs(dy)) {
    isSwiping.value = true
  }
}

const onTouchEnd = (e: TouchEvent) => {
  if (!isMobile.value || !isSwiping.value || e.changedTouches.length === 0) return
  const touch = e.changedTouches[0]
  const dx = touch.clientX - touchStartX.value
  if (Math.abs(dx) < 50) return
  if (dx < 0) setActive(activePanel.value + 1)
  if (dx > 0) setActive(activePanel.value - 1)
}
</script>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Audiowide&display=swap');

.font-serif {
  font-family: 'Audiowide', serif;
}
</style>

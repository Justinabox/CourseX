<template>
  <div class="h-full w-4/5 min-w-64 max-w-112 flex flex-col overflow-hidden">
    <!-- Header -->
    <div class="font-serif w-full items-center pb-4 p-4">
      <h1 class="text-4xl text-transparent bg-clip-text bg-gradient-to-r from-cx-brand-start to-cx-brand-end global-shadow">CourseX</h1>
      <span class="text-sm text-cx-text-muted">By students, for students.</span>
    </div>

    <div class="p-4 pb-3 flex flex-col gap-2 border-t border-cx-border">
      <input v-model="query" type="text" placeholder="Search Schools & Programs" class="w-full p-2 text-sm rounded-md border border-cx-border focus:outline-none focus:ring-1 focus:ring-cx-text-muted" />
      <div class="w-full flex flex-col">
        <NuxtLink :to="`/course/${termId}/all`" class="w-full rounded-md flex items-center gap-2 hover:bg-cx-surface-800/40 px-1 py-1 rounded">
          <Icon name="uil:list-ul" class="h-5 w-5"/>
          <span class="text-md">All Courses</span>
        </NuxtLink>

        <NuxtLink :to="`/course/${termId}/scheduled`" class="w-full rounded-md flex justify-between items-center gap-2 hover:bg-cx-surface-800/40 px-1 py-1 rounded">
          <div class="flex gap-2 items-center shrink-0">
            <Icon name="uil:calendar" class="h-5 w-5"/>
            <span class="text-md">Scheduled Courses</span>
          </div>
          <div class="text-xs font-semibold text-cx-text-weak-muted">
            <span class="lg:hidden">{{ totalScheduledUnits.toFixed(1) }}</span>
            <span class="hidden lg:inline truncate max-w-24">{{ totalScheduledUnitsLabel }}</span>
          </div>
        </NuxtLink>
      </div>
    </div>

    <div ref="leftScrollRef" class="w-full px-4 gap-3 flex flex-col h-full grow overflow-y-scroll overscroll-auto border-b border-cx-border hide-scrollbar-bg">
      <!-- <h1>Schools & Programs</h1> -->
      <div class="w-full flex flex-col gap-2 h-full border-0 border-cx-border">
        <!-- Schools & Programs List -->
        <ProgramTree :schools="schools" :query="query" />
      </div>
    </div>
    <div class="w-full h-16 flex justify-between items-center">
      <div class="flex items-center gap-2 p-1 border-r border-cx-border" ref="settingsRef">
        <!-- Settings menu -->
        <div class="relative">
          <button @click="toggleSettings" title="Settings" class="flex justify-center items-center p-2 rounded-md hover:bg-cx-surface-700/20">
            <Icon name="uil:cog" class="h-6 w-6 text-cx-text-subtle" />
          </button>
          <div v-if="settingsOpen" class="absolute bottom-full left-0 mb-2 w-48 rounded-md border border-cx-border bg-cx-surface-800/30 backdrop-blur shadow-lg p-2 z-50">
            <button @click="cycleTheme" :title="`Theme: ${preferenceLabel}`" class="w-full flex items-center gap-2 p-2 rounded-md hover:bg-cx-surface-800/80">
              <Icon :name="themeIcon" class="h-5 w-5" />
              <span class="text-sm">Cycle Theme</span>
            </button>
            <div class="w-full h-fit rounded-md text-sm flex items-center p-2 gap-2 hover:bg-cx-surface-800/90">
              <Icon name="uil:calendar" class="h-5 w-5" />
              <!-- selector, remove downwards arrow -->
              <select :value="termId" @change="onTermChange" aria-label="Select term" class="rounded-md appearance-none h-full">
                <option value="20253">Fall 2025</option>
                <option value="20261">Spring 2026</option>
              </select>
            </div>
          </div>
        </div>
      </div>
      <div class="w-full h-full flex flex-col items-center justify-center">
        <span class="text-sm text-cx-text-muted">Built with ❤️ by Korgo</span>
        <span class="text-[8px] text-cx-text-weak-muted">ver: <a href="https://github.com/MeloticZ/CourseX" class="underline hover:text-cx-text-secondary">{{ commitSha.slice(0, 7) }}</a> - data: 20251111 14:10 PST</span>
      </div>
    </div>

  </div>
</template>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Audiowide&display=swap');

.font-serif {
  font-family: 'Audiowide', serif;
}
</style>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount, nextTick, onActivated, onDeactivated, watch } from 'vue'
import { useRoute, useRouter, onBeforeRouteLeave } from 'vue-router'
import { listSchoolAndPrograms } from '@/composables/useAPI'
import { useStore } from '@/composables/useStore'
import { useTermId } from '@/composables/useTermId'
 

type Program = {
  name: string
  prefix: string
}

type School = {
  name: string
  prefix: string
  programs: Program[]
}

const query = ref('')

const runtimeConfig = useRuntimeConfig()
const commitSha = computed(() => runtimeConfig.public.WORKERS_CI_COMMIT_SHA || 'dev')

const { totalScheduledUnits, totalScheduledUnitsLabel } = useStore()
const { termId } = useTermId()

// Persist left panel scroll position
const leftScrollTop = useState<number>('ui:scroll:left', () => 0)
const leftScrollRef = ref<HTMLElement | null>(null)
const onLeftScroll = () => {
  const el = leftScrollRef.value
  if (!el) return
  leftScrollTop.value = el.scrollTop || 0
}

const restoreLeftScroll = async () => {
  await nextTick()
  const el = leftScrollRef.value
  const target = leftScrollTop.value || 0
  if (!el) return
  if (target <= 0) return
  el.scrollTop = target
  requestAnimationFrame(() => {
    if (!el) return
    if (el.scrollTop !== target) el.scrollTop = target
    setTimeout(() => {
      if (!el) return
      if (el.scrollTop !== target) el.scrollTop = target
    }, 0)
  })
}

// Temporary seed data; replace with real data source later
const schools = ref<School[]>([])
onMounted(async () => {
  const data = await listSchoolAndPrograms()
  schools.value = data.schools
  await restoreLeftScroll()
  const el = leftScrollRef.value
  if (el) el.addEventListener('scroll', onLeftScroll, { passive: true })
  // Global listeners for settings popup
  document.addEventListener('click', onDocumentClick, true)
  document.addEventListener('keydown', onDocumentKeydown)
})

onActivated(async () => {
  await restoreLeftScroll()
})

onDeactivated(() => {
  const el = leftScrollRef.value
  if (el) leftScrollTop.value = el.scrollTop || 0
})

// Ensure state is saved before navigating away and restored after navigation
const route = useRoute()
const router = useRouter()

onBeforeRouteLeave(() => {
  const el = leftScrollRef.value
  if (el) leftScrollTop.value = el.scrollTop || 0
})

watch(
  () => route.fullPath,
  async () => {
    await restoreLeftScroll()
  }
)

// As an extra safety, restore after any navigation completes
router.afterEach(async () => {
  await restoreLeftScroll()
})

onBeforeUnmount(() => {
  const el = leftScrollRef.value
  if (el) {
    leftScrollTop.value = el.scrollTop || 0
    el.removeEventListener('scroll', onLeftScroll)
  }
  document.removeEventListener('click', onDocumentClick, true)
  document.removeEventListener('keydown', onDocumentKeydown)
})

// Theme: auto -> dark -> light cycle using Nuxt color mode
const colorMode = useColorMode()
type ModePref = 'system' | 'dark' | 'light'

function getPreference(): ModePref {
  const pref = colorMode.preference
  return (pref === 'system' || pref === 'dark' || pref === 'light') ? pref : 'system'
}

const themeIcon = computed(() => {
  const pref = getPreference()
  if (pref === 'system') return 'uil:adjust-half' // auto
  if (pref === 'dark') return 'uil:moon'
  return 'uil:sun'
})

function cycleTheme() {
  const order: ModePref[] = ['system', 'dark', 'light']
  const current = getPreference()
  const idx = (order.indexOf(current) + 1) % order.length
  const next = order[idx] ?? 'system'
  colorMode.preference = next
}

const preferenceLabel = computed(() => getPreference())

// Settings popup state and handlers
const settingsOpen = ref(false)
const settingsRef = ref<HTMLElement | null>(null)
function toggleSettings() {
  settingsOpen.value = !settingsOpen.value
}

function onDocumentClick(e: MouseEvent) {
  if (!settingsOpen.value) return
  const root = settingsRef.value
  const target = e.target as Node | null
  if (root && target && !root.contains(target)) settingsOpen.value = false
}

function onDocumentKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') settingsOpen.value = false
}

function onTermChange(e: Event) {
  const target = e.target as HTMLSelectElement | null
  const selected = (target?.value || '').toString()
  if (!/^\d{5}$/.test(selected)) return
  const slug = (route.params.slug as string[] | undefined) || []
  const nextPath = ['/course', selected, ...slug].join('/')
  router.push(nextPath)
}

</script>
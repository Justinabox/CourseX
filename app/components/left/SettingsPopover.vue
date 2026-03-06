<template>
  <div class="relative" ref="settingsRef">
    <button @click="settingsOpen = !settingsOpen" title="Settings" class="flex justify-center items-center p-2 rounded-md hover:bg-cx-surface-700/20">
      <Icon name="uil:cog" class="h-6 w-6 text-cx-text-subtle" />
    </button>
    <div v-if="settingsOpen" class="absolute bottom-full left-0 mb-2 w-48 rounded-md border border-cx-border bg-cx-surface-800/30 backdrop-blur shadow-lg p-2 z-50">
      <button @click="cycleTheme" :title="`Theme: ${preferenceLabel}`" class="w-full flex items-center gap-2 p-2 rounded-md hover:bg-cx-surface-800/80">
        <Icon :name="themeIcon" class="h-5 w-5" />
        <span class="text-sm">Cycle Theme</span>
      </button>
      <div class="w-full h-fit rounded-md text-sm flex items-center p-2 gap-2 hover:bg-cx-surface-800/90">
        <Icon name="uil:calendar" class="h-5 w-5" />
        <select :value="termId" @change="onTermChange" aria-label="Select term" class="rounded-md appearance-none h-full">
          <option v-for="t in terms" :key="t.termCode" :value="String(t.termCode)">{{ t.season }} {{ t.year }}</option>
        </select>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useTermId } from '@/composables/useTermId'
import { useTerms } from '@/composables/useTerms'

const route = useRoute()
const router = useRouter()
const { termId } = useTermId()
const { terms } = useTerms()

const settingsOpen = ref(false)
const settingsRef = ref<HTMLElement | null>(null)

// Close on click outside or Escape
function onDocumentClick(e: MouseEvent) {
  if (!settingsOpen.value) return
  const root = settingsRef.value
  const target = e.target as Node | null
  if (root && target && !root.contains(target)) settingsOpen.value = false
}

function onDocumentKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') settingsOpen.value = false
}

onMounted(() => {
  document.addEventListener('click', onDocumentClick, true)
  document.addEventListener('keydown', onDocumentKeydown)
})

onBeforeUnmount(() => {
  document.removeEventListener('click', onDocumentClick, true)
  document.removeEventListener('keydown', onDocumentKeydown)
})

// Theme cycling
const colorMode = useColorMode()
type ModePref = 'system' | 'dark' | 'light'

function getPreference(): ModePref {
  const pref = colorMode.preference
  return (pref === 'system' || pref === 'dark' || pref === 'light') ? pref : 'system'
}

const themeIcon = computed(() => {
  const pref = getPreference()
  if (pref === 'system') return 'uil:adjust-half'
  if (pref === 'dark') return 'uil:moon'
  return 'uil:sun'
})

const preferenceLabel = computed(() => getPreference())

function cycleTheme() {
  const order: ModePref[] = ['system', 'dark', 'light']
  const current = getPreference()
  const idx = (order.indexOf(current) + 1) % order.length
  colorMode.preference = order[idx] ?? 'system'
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

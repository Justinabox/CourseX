<template>
  <div class="w-full h-16 flex justify-between">
    <div class="flex items-center gap-2 p-1 shrink-0 border-r border-cx-border">
      <SettingsPopover />
    </div>
    <div class="flex flex-1 items-center justify-center px-2 text-md text-cx-text-subtle w-full">
      <select v-model="selectedTermId" aria-label="Select term" class="w-full select-plain bg-transparent outline-none rounded-md px-2 py-1 appearance-none cursor-pointer transition-colors hover:text-cx-text hover:bg-cx-surface-700/20">
        <option v-for="t in terms" :key="t.termCode" :value="String(t.termCode)">{{ t.year }} {{ t.season }}</option>
      </select>
    </div>
    <div class="flex items-center gap-2 p-1 shrink-0 border-l border-cx-border">
      <button @click="cycleTheme" :title="`Theme: ${preferenceLabel}`" class="flex justify-center items-center p-2 rounded-md hover:bg-cx-surface-700/20">
        <Icon :name="themeIcon" class="h-6 w-6 text-cx-text-subtle" />
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useTermId } from '@/composables/useTermId'
import { useTerms } from '@/composables/useTerms'

const route = useRoute()
const router = useRouter()
const { termId } = useTermId()
const { terms } = useTerms()

const colorMode = useColorMode()
type ModePref = 'system' | 'dark' | 'light'

function getPreference(): ModePref {
  const pref = colorMode.preference
  return (pref === 'system' || pref === 'dark' || pref === 'light') ? pref : 'system'
}

const themeIcon = computed(() => {
  const pref = getPreference()
  if (pref === 'system') return 'lucide:sun-moon'
  if (pref === 'dark') return 'lucide:moon'
  return 'lucide:sun'
})

const preferenceLabel = computed(() => getPreference())

const selectedTermId = computed({
  get: () => termId.value,
  set: (selected: string) => {
    if (!/^\d{5}$/.test(selected)) return
    const slug = (route.params.slug as string[] | undefined) || []
    const nextPath = ['/course', selected, ...slug].join('/')
    router.push(nextPath)
  },
})

function cycleTheme() {
  const order: ModePref[] = ['system', 'dark', 'light']
  const current = getPreference()
  const idx = (order.indexOf(current) + 1) % order.length
  colorMode.preference = order[idx] ?? 'system'
}

</script>

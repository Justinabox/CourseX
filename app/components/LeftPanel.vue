<template>
  <div class="h-full flex flex-col overflow-hidden">
    <LeftPanelHeader />

    <div class="p-4 pb-3 flex flex-col gap-2 border-t border-cx-border">
      <!-- <input v-model="query" type="text" placeholder="Search Schools & Programs" class="w-full p-2 text-sm rounded-md border border-cx-border focus:outline-none focus:ring-1 focus:ring-cx-text-muted" /> -->
      <LeftPanelNav />
    </div>

    <div ref="containerRef" class="w-full px-4 gap-3 flex flex-col h-full grow overflow-y-scroll overscroll-auto border-b border-cx-border hide-scrollbar-bg">
      <div class="w-full flex flex-col gap-2 h-full border-0 border-cx-border">
        <ProgramTree :schools="schools" :query="query" />
      </div>
    </div>

    <LeftPanelFooter />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { listSchoolAndPrograms } from '@/composables/useAPI'
import { useScrollMemory } from '@/composables/useScrollMemory'

type Program = { name: string; prefix: string }
type School = { name: string; prefix: string; programs: Program[] }

const query = ref('')
const schools = ref<School[]>([])

// Scroll persistence via useScrollMemory (supports keepalive)
const { containerRef } = useScrollMemory('left-panel')

onMounted(async () => {
  const tree = await listSchoolAndPrograms()
  schools.value = Object.entries(tree).map(([prefix, val]: [string, any]) => ({
    prefix,
    name: val.name,
    programs: (val.programs || []).map((p: any) => ({ prefix: p.prefix, name: p.name })),
  }))
})
</script>
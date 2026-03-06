<template>
  <div class="w-full flex flex-col gap-2 pb-8">
    <div
      v-for="school in filteredSchools"
      :key="school.name"
      class="w-full flex flex-col gap-1"
    >
      <button
        type="button"
        class="w-full text-[15px] rounded-md flex items-center gap-2 text-cx-text-muted text-left min-w-0 hover:bg-cx-surface-800/40 rounded"
        @click="toggleSchool(school)"
      >
        <Icon
          name="uil:angle-right-b"
          class="h-5 w-5 transition-transform duration-150 shrink-0"
          :class="{ 'rotate-90': isOpen(school) }"
        />
        <span class="text-md truncate block flex-1 min-w-0">{{ school.name }}</span>
      </button>

      <div v-if="isOpen(school)" class="w-full pl-6 flex flex-col gap-1">
        <NuxtLink
          v-for="program in school.filteredPrograms"
          :key="program.prefix"
          class="w-full rounded-md flex items-center gap-2 justify-between hover:bg-cx-surface-800/40 px-1 py-1 rounded"
          :to="`/course/${termId}/${school.prefix}/${program.prefix}`"
          active-class="bg-cx-surface-800/60"
        >
          <div class="flex items-center gap-2 flex-1 min-w-0">
            <Icon name="uil:book-open" class="h-4 w-4 shrink-0"/>
            <span class="text-sm truncate block">{{ program.name }}</span>
          </div>
          <span class="text-xs font-semibold text-cx-text-weak-muted shrink-0">{{ program.prefix }}</span>
        </NuxtLink>
      </div>
    </div>
  </div>
  
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
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

const props = defineProps<{
  schools: School[]
  query?: string
}>()

// Persist open state globally across route changes
const openSchoolKeySet = useState<Set<string>>('openSchoolKeySet', () => new Set<string>())

// Initialize all schools as open by default if empty
const schoolKey = (school: School) => school.name

const initializeOpen = () => {
  if (openSchoolKeySet.value.size === 0) {
    openSchoolKeySet.value = new Set(props.schools.map((s) => schoolKey(s)))
  }
}

initializeOpen()

const normalize = (value: string) => value.toLowerCase().trim()

const filteredSchools = computed(() => {
  const search = normalize(props.query ?? '')
  const hasQuery = search.length > 0

  return props.schools
    .map((school) => {
      if (!hasQuery) {
        return {
          ...school,
          filteredPrograms: school.programs,
        }
      }

      const filteredPrograms = school.programs.filter((p) => {
        const haystack = `${p.name} ${p.prefix}`.toLowerCase()
        return haystack.includes(search)
      })

      return {
        ...school,
        filteredPrograms,
      }
    })
    .filter((school) => school.filteredPrograms.length > 0)
})

const isOpen = (school: School) => openSchoolKeySet.value.has(schoolKey(school))

const toggleSchool = (school: School) => {
  const key = schoolKey(school)
  const set = new Set(openSchoolKeySet.value)
  if (set.has(key)) set.delete(key)
  else set.add(key)
  openSchoolKeySet.value = set
}

const { termId } = useTermId()

</script>

<style scoped>
</style>


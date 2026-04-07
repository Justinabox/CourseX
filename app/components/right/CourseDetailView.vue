<template>
  <div class="gap-2 flex flex-col p-4 h-full overflow-y-hidden">
    <div v-if="details">
      <div class="flex items-center gap-2">
        <span class="text-sm text-cx-text-subtle">Course Details</span>
        <span class="text-sm text-cx-text-subtle">{{ details.code }} {{ details.isCrosslisted && details.code !== details.displayCode ? `(${details.displayCode})` : '' }}</span>
      </div>
      <div class="flex gap-2 items-baseline">
        <div class="flex items-center" v-if="isGESM || geLetters.length > 0">
          <span
            class="w-fit text-xl font-semibold line-clamp-1 leading-none grid place-items-center text-cx-text-weak-muted"
          >
            {{ isGESM ? 'GESM-' : 'GE-' }}
          </span>
          <div class="flex items-center" v-if="geLetters.length > 0">
            <span
              v-for="g in geLetters"
              :key="g"
              class="w-fit text-xl font-semibold line-clamp-1 leading-none grid place-items-center text-cx-text-weak-muted"
            >
              {{ g }}
            </span>
          </div>
        </div>
        <h1 class="text-2xl font-semibold">{{ details.title }}</h1>
      </div>
    </div>

    <div class="flex flex-col gap-2 overflow-y-scroll h-full">
      <template v-if="details">
        <span class="text-sm text-cx-text-muted">
          {{ details.description || 'No description available.' }}
        </span>

        <div class="flex items-center gap-4 py-1">
          <CourseActions :details="details" />
        </div>

        <div class="flex flex-col gap-1.5 border-y border-cx-border py-4">
          <div v-if="syllabusUrl" class="flex justify-start gap-2">
            <Icon name="uil:file-alt" class="h-5 w-5 text-cx-text-muted shrink-0" />
            <a :href="syllabusUrl" target="_blank" rel="noopener noreferrer" class="text-sm text-cx-text-subtle hover:underline decoration-1 hover:text-cx-text-muted flex items-center gap-1">
              Syllabus available
              <Icon name="lucide:external-link" class="h-3.5 w-3.5 shrink-0" />
            </a>
          </div>
          <div v-else-if="previousSyllabusUrl" class="flex justify-start gap-2">
            <Icon name="uil:file-alt" class="h-5 w-5 text-cx-text-muted shrink-0 opacity-60" />
            <a :href="previousSyllabusUrl" target="_blank" rel="noopener noreferrer" class="text-sm text-cx-text-subtle hover:underline decoration-1 hover:text-cx-text-muted flex items-center gap-1 opacity-80">
              Syllabus for other semester available
              <Icon name="lucide:external-link" class="h-3.5 w-3.5 shrink-0" />
            </a>
          </div>

          <InstructorList :instructors="details.instructors || []" />
          <div class="flex justify-start gap-2">
            <Icon name="uil:user" class="h-5 w-5 shrink-0" :class="{ 'text-cx-status-danger-emphasis': details.enrolled === details.capacity, 'text-cx-text-muted': details.enrolled !== details.capacity }" />
            <span class="text-sm" :class="{ 'text-cx-status-danger-emphasis': details.enrolled === details.capacity, 'text-cx-text-subtle': details.enrolled !== details.capacity }">{{ details.enrolled }} / {{ details.capacity }} Students</span>
          </div>
          <div v-if="detailScheduleLines.length > 0" class="flex justify-start gap-2">
            <Icon name="uil:clock" class="h-5 w-5 text-cx-text-muted shrink-0 mt-0.5" />
            <div class="flex flex-col">
              <span v-for="(line, idx) in detailScheduleLines" :key="idx" class="text-sm text-cx-text-subtle">{{ line }}</span>
            </div>
          </div>
          <div v-if="showDetailLocation" class="flex justify-start gap-2">
            <Icon name="uil:location-point" class="h-5 w-5 text-cx-text-muted shrink-0" />
            <span class="text-sm text-cx-text-subtle">{{ detailLocation }}</span>
          </div>
        </div>

        <CourseMetadata :details="details" />
      </template>

      <EmptyState v-else />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { getCourseDetails, getSectionDetails, type CourseDetails } from '@/composables/useAPI'
import { useCourseSelection } from '@/composables/useCourseSelection'
import { useTermId } from '@/composables/useTermId'
import { formatDetailScheduleLines, formatCardLocation } from '@/composables/api/transforms'

const { selectedCourseCode, selectedSectionId } = useCourseSelection()
const { termId } = useTermId()

const details = ref<CourseDetails | null>(null)
const isLoading = ref(false)
const loadError = ref<string | null>(null)

async function loadDetails() {
  const code = selectedCourseCode.value
  const section = selectedSectionId.value
  const tid = termId.value
  if (!code || !tid) {
    details.value = null
    return
  }
  isLoading.value = true
  loadError.value = null
  try {
    if (section) {
      const bySection = await getSectionDetails(tid, code, section)
      if (bySection) {
        details.value = bySection
        return
      }
    }
    details.value = await getCourseDetails(tid, code)
  } catch (e: any) {
    loadError.value = e?.message || 'Failed to load course details'
    details.value = null
  } finally {
    isLoading.value = false
  }
}

watch(
  () => [selectedCourseCode.value, selectedSectionId.value],
  () => { loadDetails() },
  { immediate: true }
)

const runtimeConfig = useRuntimeConfig()
const syllabusUrl = computed(() => {
  const d = details.value
  if (!d?.syllabus || !runtimeConfig.public.syllabusDomain) return null
  return `${runtimeConfig.public.syllabusDomain}/syllabi/${termId.value}/${d.syllabus}`
})
const previousSyllabusUrl = computed(() => {
  const d = details.value
  if (!d?.previousSyllabus || !runtimeConfig.public.syllabusDomain) return null
  return `${runtimeConfig.public.syllabusDomain}/syllabi/${d.previousSyllabus.termCode}/${d.previousSyllabus.filename}`
})

const detailScheduleLines = computed(() => formatDetailScheduleLines(details.value?.schedules || []))
const hasMultipleSchedules = computed(() => (details.value?.schedules || []).length > 1)
const detailLocation = computed(() => formatCardLocation(details.value?.schedules || []))
const showDetailLocation = computed(() => !hasMultipleSchedules.value && !!detailLocation.value)
const isGESM = computed(() => (details.value?.code || '').toUpperCase().startsWith('GESM'))
const geLetters = computed(() => Array.from(new Set(details.value?.ges || [])).filter(g => g && g !== 'GESM'))
</script>

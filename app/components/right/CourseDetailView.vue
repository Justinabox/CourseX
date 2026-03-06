<template>
  <div class="gap-2 flex flex-col p-4 h-full overflow-y-hidden">
    <div v-if="details">
      <div class="flex items-center gap-2">
        <span class="text-sm text-cx-text-subtle">Course Details</span>
        <span class="text-sm text-cx-text-subtle">{{ details.code }}</span>
      </div>
      <h1 class="text-2xl font-semibold">{{ details.title }}</h1>
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
  if (!code) {
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

const detailScheduleLines = computed(() => formatDetailScheduleLines(details.value?.schedules || []))
const hasMultipleSchedules = computed(() => (details.value?.schedules || []).length > 1)
const detailLocation = computed(() => formatCardLocation(details.value?.schedules || []))
const showDetailLocation = computed(() => !hasMultipleSchedules.value && !!detailLocation.value)
</script>

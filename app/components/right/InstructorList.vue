<template>
  <div class="flex justify-start gap-2">
    <Icon name="uil:graduation-cap" class="h-5 w-5 text-cx-text-muted shrink-0"/>
    <div class="flex flex-wrap items-center">
      <template v-if="instructorViews.length > 0">
        <template v-for="item in instructorViews" :key="item.name">
          <a
            v-if="item.link"
            :href="item.link"
            target="_blank"
            rel="noopener noreferrer"
            class="text-sm hover:underline decoration-1 hover:text-cx-text-muted inline-flex items-center gap-1"
            :class="{ 'text-cx-status-danger-icon': item.isLow, 'text-cx-text-subtle': !item.isLow }"
          >
            <span v-if="!Number.isNaN(item.rating)" class="text-sm border-cx-text-secondary" :class="{ 'text-cx-status-danger-icon': item.isLow, 'text-cx-text-subtle': !item.isLow }">{{ item.rating.toFixed(1) }}</span>
            {{ item.name }}
            <Icon name="lucide:external-link" class="h-3.5 w-3.5 shrink-0" />
          </a>
          <span
            v-else
            class="text-sm"
            :class="{ 'text-cx-status-danger-icon': item.isLow, 'text-cx-text-subtle': !item.isLow }"
          >
            <span v-if="!Number.isNaN(item.rating)" class="text-sm border-cx-text-secondary" :class="{ 'text-cx-status-danger-icon': item.isLow, 'text-cx-text-subtle': !item.isLow }">{{ item.rating.toFixed(1) }}</span>
            {{ item.name }}
          </span>
          <span class="text-cx-text-muted" v-if="item !== instructorViews[instructorViews.length - 1]">,&nbsp;</span>
        </template>
      </template>
      <span v-else class="text-sm text-cx-text-subtle">TBA</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useRMPRatings } from '@/composables/useRMPRatings'
import { resolveInstructorViews, type InstructorView } from '@/composables/instructorViews'

const props = defineProps<{
  instructors: string[]
}>()

const { getProfessor } = useRMPRatings()
const instructorViews = ref<InstructorView[]>([])

async function updateInstructorViews() {
  instructorViews.value = await resolveInstructorViews(props.instructors, getProfessor)
}

watch(
  () => props.instructors,
  () => { void updateInstructorViews() },
  { immediate: true }
)
</script>

<template>
  <div class="flex flex-col gap-2">
    <div v-if="details.units.length > 0" class="flex justify-start gap-2">
      <Icon name="uil:bill" class="h-5 w-5 shrink-0 text-cx-text-muted" />
      <span class="text-sm text-cx-text-subtle">{{ renderDetailUnits }} units</span>
    </div>

    <div v-if="details.dupeCreditComment" class="flex justify-start gap-2">
      <Icon name="uil:pathfinder" class="h-5 w-5 shrink-0 text-cx-status-success-icon" />
      <span class="text-sm text-cx-status-success-text">Dupe credit: </span>
      <span class="text-xs bg-cx-status-success-badge-bg text-cx-status-success-badge-text px-1 py-0.5 rounded-md">{{ details.dupeCreditComment }}</span>
    </div>

    <div v-if="details.dClearance" class="flex justify-start gap-2">
      <Icon name="uil:bell" class="h-5 w-5 shrink-0 text-cx-status-danger-icon" />
      <span class="text-sm text-cx-status-danger-text">D-Clearance</span>
    </div>

    <div v-if="details.prerequisites.length > 0" class="flex justify-start gap-2">
      <Icon name="uil:link" class="h-5 w-5 shrink-0 text-cx-status-warning-icon" />
      <span class="text-sm text-cx-status-warning-text">Pre-requisite: </span>
      <div class="flex items-center gap-1">
        <span v-for="(group, idx) in details.prerequisites" :key="idx" class="text-xs bg-cx-status-warning-badge-bg text-cx-status-warning-badge-text px-1 py-0.5 rounded-md">{{ formatPrerequisiteGroup(group) }}</span>
      </div>
    </div>

    <div v-if="typeMeta" class="flex items-center gap-2">
      <Icon :name="typeMeta.detailIconName" class="h-5 w-5" :class="typeMeta.detailIconClass" />
      <span class="text-sm" :class="typeMeta.detailTextClass">{{ typeMeta.detailLabel }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { CourseDetails } from '@/composables/useAPI'
import { getCourseTypeMeta } from '@/composables/courseTypeMeta'
import { formatUnitsOptions, formatPrerequisiteGroup } from '@/composables/api/transforms'

const props = defineProps<{
  details: CourseDetails
}>()

const typeMeta = computed(() => getCourseTypeMeta(props.details.type))
const renderDetailUnits = computed(() => formatUnitsOptions(props.details.units || []))
</script>

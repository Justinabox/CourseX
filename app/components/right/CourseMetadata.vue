<template>
  <div class="flex flex-col gap-2">
    <div v-if="details.units.length > 0" class="flex items-center gap-2">
      <Icon name="uil:bill" class="h-5 w-5 text-cx-text-muted" />
      <span class="text-sm text-cx-text-subtle">{{ renderDetailUnits }} units</span>
    </div>

    <div v-if="details.dupeCreditComment" class="flex items-center gap-2">
      <Icon name="uil:pathfinder" class="h-5 w-5 text-green-500" />
      <span class="text-sm text-green-400">Dupe credit: </span>
      <span class="text-xs bg-green-800 text-green-200 px-1 py-0.5 rounded-md">{{ details.dupeCreditComment }}</span>
    </div>

    <div v-if="details.dClearance" class="flex items-center gap-2">
      <Icon name="uil:bell" class="h-5 w-5 text-rose-500" />
      <span class="text-sm text-rose-400">D-Clearance</span>
    </div>

    <div v-if="details.prerequisites.length > 0" class="flex items-center gap-2">
      <Icon name="uil:link" class="h-5 w-5 text-yellow-500" />
      <span class="text-sm text-yellow-400">Pre-requisite: </span>
      <div class="flex items-center gap-1">
        <span v-for="(group, idx) in details.prerequisites" :key="idx" class="text-xs bg-yellow-800 text-yellow-200 px-1 py-0.5 rounded-md">{{ formatPrerequisiteGroup(group) }}</span>
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
import { getCourseTypeMeta } from '@/composables/useCourseTypeMeta'
import { formatUnitsOptions, formatPrerequisiteGroup } from '@/composables/api/transforms'

const props = defineProps<{
  details: CourseDetails
}>()

const typeMeta = computed(() => getCourseTypeMeta(props.details.type))
const renderDetailUnits = computed(() => formatUnitsOptions(props.details.units || []))
</script>

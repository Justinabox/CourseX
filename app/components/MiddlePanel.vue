<template>
  <div class="w-full h-full border-x border-cx-border flex flex-col p-4">
    <div class="w-full flex flex-col gap-2 pb-2 border-cx-border">
      <span class="text-sm text-cx-text-subtle">Available Courses</span>
      <input v-model="filters.searchText" type="text" placeholder="Search Courses" class="w-full p-2 text-sm rounded-md border border-cx-border focus:outline-none focus:ring-1 focus:ring-cx-text-muted" />

      <!-- Filters Div -->
      <FilterBar
        :show="showFilters"
        :filters="filters"
        @reset="reset"
      />

      <div class="w-full flex justify-between items-center px-2">
        <div class="flex gap-1 cursor-pointer select-none" @click="showFilters = !showFilters">
          <Icon name="uil:filter" class="h-4 w-4 text-cx-text-weak-muted duration-1000" :class="showFilters ? 'rotate-180' : ''" />
          <span class="text-xs text-cx-text-secondary">Filters</span>
        </div>
        <span class="text-xs text-cx-text-secondary">{{ filteredCourses.length }} courses found</span>
      </div>
    </div>

    <div ref="scrollContainerEl" class="w-full grow overflow-y-auto overflow-x-hidden overscroll-auto min-w-0 hide-scrollbar-bg">
      <div class="w-full min-w-0 relative" :style="{ height: `${rowVirtualizer.getTotalSize()}px` }">
        <div
          v-for="virtualRow in rowVirtualizer.getVirtualItems()"
          :key="String(virtualRow.key)"
          :data-index="virtualRow.index"
          :ref="(el) => { if (el) rowVirtualizer.measureElement(el as any) }"
          :style="{
            position: 'absolute',
            top: 0,
            left: 0,
            width: '100%',
            transform: `translateY(${virtualRow.start}px)`
          }"
          class="pb-3"
        >
          <CourseCard
            v-if="filteredCourses[virtualRow.index]"
            data-card
            :title="filteredCourses[virtualRow.index]!.title"
            :code="filteredCourses[virtualRow.index]!.code"
            :description="filteredCourses[virtualRow.index]!.description"
            :sections="filteredCourses[virtualRow.index]!.sections"
            :ges="filteredCourses[virtualRow.index]!.ges"
            :display-code="filteredCourses[virtualRow.index]!.displayCode"
            :is-crosslisted="filteredCourses[virtualRow.index]!.isCrosslisted"
            @section-click="(sid) => onSectionClick(filteredCourses[virtualRow.index]!.code, sid)"
          />
        </div>
      </div>
    </div>

  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, onBeforeUnmount, watch } from 'vue'
import { type UICourse } from '@/composables/useAPI'
import { useCourseFilters } from '@/composables/useCourseFilters'
import { useCourseListSource } from '@/composables/useCourseListSource'
import { useScrollMemory } from '@/composables/useScrollMemory'
import { useRouteMode } from '@/composables/useRouteMode'
import FilterBar from '@/components/FilterBar.vue'
import { useVirtualizer } from '@tanstack/vue-virtual'

const router = useRouter()

// UI state
const showFilters = ref(false)

// Data source and filters
const { courses, mode, scopeKey } = useCourseListSource()
const { filters, filteredCourses, reset } = useCourseFilters(courses)

// Reset filters when the user navigates to a different category
watch(scopeKey, () => { reset() })

// Scroll persistence
const { containerRef: sContainerRef } = useScrollMemory(() => scopeKey.value)
const scrollContainerEl = ref<HTMLElement | null>(null)

// Variable-height virtualization with key-based heights
const rowVirtualizerOptions = computed(() => ({
  count: filteredCourses.value.length,
  getScrollElement: () => scrollContainerEl.value,
  estimateSize: () => 120,
  getItemKey: (index: number) => {
    const course = filteredCourses.value[index]
    return course ? `${course.code}::${course.title}` : index
  },
  overscan: 5,
}))
const rowVirtualizer = useVirtualizer(rowVirtualizerOptions)

onMounted(() => {
  const container = scrollContainerEl.value
  sContainerRef.value = container as any
})

// Click to navigate
const { makeSelectionPath } = useRouteMode()

const onSectionClick = (courseCode: string, sectionId: string) => {
  const parsed = mode.value
  router.push(makeSelectionPath(parsed, courseCode, sectionId || null))
}
</script>

<style scoped>
</style>

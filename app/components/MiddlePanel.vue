<template>
  <div class="w-full h-full border-x border-cx-border flex flex-col min-w-80 p-4">
    <div class="w-full flex flex-col gap-2 pb-2 border-cx-border">
      <span class="text-sm text-cx-text-subtle">Available Courses</span>
      <input v-model="filters.searchText" type="text" placeholder="Search Courses" class="w-full p-2 text-sm rounded-md border border-cx-border focus:outline-none focus:ring-1 focus:ring-cx-text-muted" />

      <!-- Filters Div -->
      <FilterBar
        :show="showFilters"
        :filters="filters"
        @cycle-tri="cycleTriState"
        @set-tri="setTriState"
        @toggle-conflicts="toggleConflicts"
        @set-conflicts-any="setConflictsAny"
        @toggle-enrollment-open-only="toggleEnrollmentOpenOnly"
        @set-enrollment-any="setEnrollmentAny"
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
      <div class="w-full flex flex-col min-w-0">
        <div :style="{ height: topPadding + 'px' }"></div>
        <template v-for="(course, i) in visibleCourses" :key="visibleKey(course, i)">
          <div class="pb-3" :ref="(el) => onRowRef(el, startIndex + i)" :data-row-index="startIndex + i">
            <CourseCard
              data-card
              :title="course.title"
              :code="course.code"
              :description="course.description"
              :sections="course.sections"
              :ge="(course as any).ge || []"
              @section-click="(sid) => onSectionClick(course.code, sid)"
            />
          </div>
        </template>
        <div :style="{ height: bottomPadding + 'px' }"></div>
      </div>
    </div>
    
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch, nextTick, onBeforeUnmount } from 'vue'
import { type UICourse } from '@/composables/useAPI'
import { useCourseFilters } from '@/composables/useCourseFilters'
import { useCourseSelection } from '@/composables/useCourseSelection'
import { useCourseListSource } from '@/composables/useCourseListSource'
import { useVariableVirtualList } from '@/composables/useVariableVirtualList'
import { useScrollMemory } from '@/composables/useScrollMemory'
import { useRouteMode } from '@/composables/useRouteMode'
import FilterBar from '@/components/FilterBar.vue'

const router = useRouter()

// UI state
const showFilters = ref(false)

// Data source and filters
const { courses, mode, scopeKey } = useCourseListSource()
const { filters, filteredCourses, reset } = useCourseFilters(courses)

// Tri-state toggles for D and R (handlers for FilterBar)
type TriKey = 'dClearance' | 'prerequisites'
const cycleTriState = (key: TriKey) => {
  const val = filters[key]
  const next = val === 'any' ? 'only' : val === 'only' ? 'exclude' : 'any'
  filters[key] = next as any
}
const setTriState = (key: TriKey, v: 'any' | 'only' | 'exclude') => { filters[key] = v as any }

// Conflicts toggle (any <-> exclude)
const toggleConflicts = () => { filters.conflicts = filters.conflicts === 'exclude' ? 'any' : 'exclude' }
const setConflictsAny = () => { filters.conflicts = 'any' }

// Enrollment toggle (any <-> only-open)
const toggleEnrollmentOpenOnly = () => { filters.enrollment = filters.enrollment === 'only-open' ? 'any' : 'only-open' }
const setEnrollmentAny = () => { filters.enrollment = 'any' }
// Variable-height virtualization with key-based heights
const {
  containerRef: vContainerRef,
  startIndex,
  endIndex,
  topPadding,
  bottomPadding,
  visibleItems,
  onRowRef,
  updateViewport,
  scheduleUpdateViewport,
} = useVariableVirtualList<UICourse>({
  items: filteredCourses,
  estimateItemHeight: 120,
  getKey: (c) => `${c.code}::${c.title}`,
})

// Scroll persistence
const { containerRef: sContainerRef } = useScrollMemory(() => scopeKey.value)
const scrollContainerEl = ref<HTMLElement | null>(null)

const visibleCourses = computed(() => visibleItems.value)

onMounted(() => {
  const container = scrollContainerEl.value
  vContainerRef.value = container as any
  sContainerRef.value = container as any
  if (container) {
    container.addEventListener('scroll', scheduleUpdateViewport, { passive: true })
  }
  scheduleUpdateViewport()
})

onBeforeUnmount(() => {
  const container = scrollContainerEl.value
  if (container) {
    container.removeEventListener('scroll', scheduleUpdateViewport)
  }
})

watch(filteredCourses, () => {
  nextTick(scheduleUpdateViewport)
})

// Click to navigate
const { selectCourse } = useCourseSelection()
const { makeSelectionPath } = useRouteMode()

const onSectionClick = (courseCode: string, sectionId: string) => {
  const parsed = mode.value
  selectCourse(courseCode, sectionId || null)
  router.push(makeSelectionPath(parsed, courseCode, sectionId || 'section'))
}

const visibleKey = (course: UICourse, i: number) => `${course.code}::${course.title}`
</script>

<style scoped>
</style>
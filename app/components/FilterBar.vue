<template>
  <div class="w-full flex justify-center py-1 overflow-x-clip" v-show="show">
    <div class="flex gap-2 xl:gap-4">
      <div class="grid grid-cols-7 grid-rows-2 gap-1 w-41 h-11 lg:w-48 lg:h-13 xl:w-55 xl:h-15 2xl:w-62 2xl:h-17 shrink-0">
        <button
          class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm rounded-tl-sm"
          :class="dayActiveClass(0)"
          title="Filter by Sunday"
          @click="toggleDay(0)"
          @contextmenu.prevent="clearDays()"
          :aria-pressed="isDaySelected(0)"
        >S</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(1)" title="Filter by Monday" @click="toggleDay(1)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(1)">M</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(2)" title="Filter by Tuesday" @click="toggleDay(2)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(2)">T</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(3)" title="Filter by Wednesday" @click="toggleDay(3)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(3)">W</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(4)" title="Filter by Thursday" @click="toggleDay(4)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(4)">H</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(5)" title="Filter by Friday" @click="toggleDay(5)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(5)">F</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center bg-zebra-sm" :class="dayActiveClass(6)" title="Filter by Saturday" @click="toggleDay(6)" @contextmenu.prevent="clearDays()" :aria-pressed="isDaySelected(6)">S</button>
        <input :value="String(timeStart.text.value || '')" type="text" placeholder="00:00" maxlength="6" class="text-center text-sm lg:text-md xl:text-lg line-clamp-1 font-semibold leading-none bg-dotted-sm placeholder:text-cx-text-weak-shimmer text-cx-text-weak focus:outline-none col-span-3 rounded-bl-sm" :class="timeStart.valid.value ? validRingClass : ''" title="Filter by Start Time" @focus="timeStart.onFocus" @input="timeStart.onInput" @blur="timeStart.onBlur" @contextmenu.prevent="clearStartTime()" />
        <div class="aspect-square text-sm lg:text-md xl:text-lg font-semibold leading-none grid place-items-center text-cx-text-weak-shimmer" title="Schedule Filter">—</div>
        <input :value="String(timeEnd.text.value || '')" type="text" placeholder="23:59" maxlength="6" class="text-center text-sm lg:text-md xl:text-lg line-clamp-1 font-semibold leading-none bg-dotted-sm placeholder:text-cx-text-weak-shimmer text-cx-text-weak focus:outline-none col-span-3" :class="timeEnd.valid.value ? validRingClass : ''" title="Filter by End Time" @focus="timeEnd.onFocus" @input="timeEnd.onInput" @blur="timeEnd.onBlur" @contextmenu.prevent="clearEndTime()" />
      </div>

      <div class="grid grid-cols-2 grid-rows-2 grid-flow-col gap-1 shrink-0 w-11 h-11 lg:w-13 lg:h-13 xl:w-15 xl:h-15 2xl:w-17 2xl:h-17">
        <input :value="String(unitsMin.text.value || '')" type="text" placeholder="0" maxlength="3" class="aspect-square text-center text-sm lg:text-md xl:text-lg line-clamp-1 font-semibold leading-none bg-dotted-sm placeholder:text-cx-text-weak-shimmer text-cx-text-weak focus:outline-none" :class="unitsMin.valid.value ? validRingClass : ''" title="Filter by Minimum Units" @focus="unitsMin.onFocus" @input="unitsMin.onInput" @blur="unitsMin.onBlur" @contextmenu.prevent="clearUnitsMin()" />
        <input :value="String(unitsMax.text.value || '')" type="text" placeholder="16" maxlength="3" class="aspect-square text-center text-sm lg:text-md xl:text-lg line-clamp-1 font-semibold leading-none bg-dotted-sm placeholder:text-cx-text-weak-shimmer text-cx-text-weak focus:outline-none" :class="unitsMax.valid.value ? validRingClass : ''" title="Filter by Maximum Units" @focus="unitsMax.onFocus" @input="unitsMax.onInput" @blur="unitsMax.onBlur" @contextmenu.prevent="clearUnitsMax()" />
        <input :value="String(courseLevel.text.value || '')" type="text" placeholder="5XX" maxlength="4" class="text-center text-sm lg:text-md xl:text-lg text-sideways line-clamp-1 font-semibold leading-none bg-dotted-sm placeholder:text-cx-text-weak-shimmer text-cx-text-weak focus:outline-none row-span-2" :class="courseLevel.valid.value ? validRingClass : ''" title="Filter by Course Level" @focus="courseLevel.onFocus" @input="courseLevel.onInput" @blur="courseLevel.onBlur" @contextmenu.prevent="clearCourseLevel()" />
      </div>

      <div class="grid grid-cols-3 grid-rows-2 gap-1 shrink-0 w-17 h-11 lg:w-20 lg:h-13 xl:w-23 xl:h-15 2xl:w-26 2xl:h-17">
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold line-clamp-1 leading-none grid place-items-center" :class="dClearanceClass" title="Filter by D-Clearance" @click="$emit('cycle-tri', 'dClearance')" @contextmenu.prevent="$emit('set-tri', 'dClearance', 'any')" :aria-pressed="filters.dClearance !== 'any'">D</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold line-clamp-1 leading-none grid place-items-center" :class="prereqClass" title="Filter by Prerequisites" @click="$emit('cycle-tri', 'prerequisites')" @contextmenu.prevent="$emit('set-tri', 'prerequisites', 'any')" :aria-pressed="filters.prerequisites !== 'any'">R</button>
        <button class="text-sm xl:text-md 2xl:text-lg text-sideways font-semibold line-clamp-1 leading-none grid place-items-center bg-zebra-sm text-cx-text-weak-shimmer row-span-2 rounded-r-sm" title="Reset Filters" @click="$emit('reset')">Reset</button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold line-clamp-1 leading-none grid place-items-center bg-zebra-sm" :class="conflictsClass" title="Exclude Courses with Schedule Conflicts" @click="$emit('toggle-conflicts')" @contextmenu.prevent="$emit('set-conflicts-any')" :aria-pressed="filters.conflicts === 'exclude'"><Icon name="uil:clock" class="aspect-square w-full" :class="conflictsIconClass" /></button>
        <button class="aspect-square text-sm lg:text-md xl:text-lg font-semibold line-clamp-1 leading-none grid place-items-center bg-zebra-sm" :class="enrollmentClass" title="Exclude Full Enrollment Courses" @click="$emit('toggle-enrollment-open-only')" @contextmenu.prevent="$emit('set-enrollment-any')" :aria-pressed="filters.enrollment !== 'any'"><Icon name="uil:user" class="aspect-square w-full" :class="enrollmentIconClass" /></button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { CourseFiltersState } from '@/composables/api/types'
import { useDraftInput } from '@/composables/useDraftInput'
import { minutesToTime, parseTimeLooseToMinutes } from '@/composables/useTimeParsing'
import UiInput from '@/components/ui/UiInput.vue'
import UiToggle from '@/components/ui/UiToggle.vue'
import UiButton from '@/components/ui/UiButton.vue'

const props = defineProps<{ show: boolean; filters: CourseFiltersState }>()

defineEmits([
  'cycle-tri',
  'set-tri',
  'toggle-conflicts',
  'set-conflicts-any',
  'toggle-enrollment-open-only',
  'set-enrollment-any',
  'reset',
])

const filters = props.filters

const validRingClass = 'shadow-[0_0_2px_1px] shadow-cx-text-secondary text-cx-text-weak'

const isDaySelected = (idx: number) => (filters.days || []).includes(idx)
const toggleDay = (idx: number) => {
  const set = new Set(filters.days || [])
  if (set.has(idx)) set.delete(idx)
  else set.add(idx)
  filters.days = Array.from(set).sort((a, b) => a - b)
}
const clearDays = () => { filters.days = [] }
const dayActiveClass = (idx: number) => isDaySelected(idx) ? 'shadow-[0_0_2px_1px] shadow-cx-text-secondary text-cx-text-weak' : 'text-cx-text-weak-shimmer'

const timeStart = useDraftInput<number>({
  getValue: () => filters.timeStartMinutes,
  setValue: (v) => { filters.timeStartMinutes = v },
  format: (v) => minutesToTime(v),
  parse: (s) => {
    const minutes = parseTimeLooseToMinutes(s)
    return { value: minutes, text: minutes != null ? minutesToTime(minutes) : '', valid: minutes != null }
  },
})
const timeStartText = computed(() => timeStart.text.value)
const timeEnd = useDraftInput<number>({
  getValue: () => filters.timeEndMinutes,
  setValue: (v) => { filters.timeEndMinutes = v },
  format: (v) => minutesToTime(v),
  parse: (s) => {
    const minutes = parseTimeLooseToMinutes(s)
    return { value: minutes, text: minutes != null ? minutesToTime(minutes) : '', valid: minutes != null }
  },
})
const timeEndText = computed(() => timeEnd.text.value)

const clearStartTime = () => {
  filters.timeStartMinutes = null
  timeStart.isEditing.value = false
  timeStart.draft.value = ''
  timeStart.valid.value = false
}
const clearEndTime = () => {
  filters.timeEndMinutes = null
  timeEnd.isEditing.value = false
  timeEnd.draft.value = ''
  timeEnd.valid.value = false
}

const unitsMin = useDraftInput<number>({
  getValue: () => filters.unitsMin,
  setValue: (v) => { filters.unitsMin = v },
  format: (v) => String(v),
  parse: (s) => {
    const m = (s || '').match(/\d{1,2}/)
    if (!m) return { value: null, text: '', valid: false }
    const val = Number(m[0])
    if (!Number.isFinite(val)) return { value: null, text: '', valid: false }
    return { value: val, text: String(val), valid: true }
  },
})
const unitsMinText = computed(() => unitsMin.text.value)
const unitsMax = useDraftInput<number>({
  getValue: () => filters.unitsMax,
  setValue: (v) => { filters.unitsMax = v },
  format: (v) => String(v),
  parse: (s) => {
    const m = (s || '').match(/\d{1,2}/)
    if (!m) return { value: null, text: '', valid: false }
    const val = Number(m[0])
    if (!Number.isFinite(val)) return { value: null, text: '', valid: false }
    return { value: val, text: String(val), valid: true }
  },
})
const unitsMaxText = computed(() => unitsMax.text.value)
const clearUnitsMin = () => {
  filters.unitsMin = null
  unitsMin.isEditing.value = false
  unitsMin.draft.value = ''
  unitsMin.valid.value = false
}
const clearUnitsMax = () => {
  filters.unitsMax = null
  unitsMax.isEditing.value = false
  unitsMax.draft.value = ''
  unitsMax.valid.value = false
}

const courseLevel = useDraftInput<number>({
  getValue: () => filters.courseLevelMin,
  setValue: (v) => {
    if (v == null) {
      filters.courseLevelMin = null
      filters.courseLevelMax = null
    } else {
      const base = Math.floor(v / 100) * 100
      filters.courseLevelMin = base
      filters.courseLevelMax = base + 99
    }
  },
  format: (v) => `${Math.floor(v / 100)}XX`,
  parse: (s) => {
    const m = (s || '').match(/(\d)/)
    if (!m) return { value: null, text: '', valid: false }
    const digit = Number(m[1])
    const base = digit * 100
    return { value: base, text: `${digit}XX`, valid: true }
  },
})
const courseLevelText = computed(() => courseLevel.text.value)
const clearCourseLevel = () => {
  filters.courseLevelMin = null
  filters.courseLevelMax = null
  courseLevel.isEditing.value = false
  courseLevel.draft.value = ''
  courseLevel.valid.value = false
}

const dClearanceClass = computed(() => {
  const v = filters.dClearance
  if (v === 'only') return 'bg-zebra-sm-rose text-rose-400'
  if (v === 'exclude') return 'bg-zebra-sm outline outline-1 outline-rose-400 text-rose-400 shadow-[0_0_2px_1px] shadow-rose-500'
  return 'bg-zebra-sm text-cx-text-weak-shimmer'
})
const prereqClass = computed(() => {
  const v = filters.prerequisites
  if (v === 'only') return 'bg-zebra-sm-yellow text-yellow-400'
  if (v === 'exclude') return 'bg-zebra-sm outline outline-1 outline-yellow-400 text-yellow-400 shadow-[0_0_2px_1px] shadow-yellow-500'
  return 'bg-zebra-sm text-cx-text-weak-shimmer'
})
const conflictsClass = computed(() => filters.conflicts === 'exclude' ? 'outline outline-1 outline-cx-text-muted text-cx-text-weak' : 'text-cx-text-weak-shimmer')
const conflictsIconClass = computed(() => filters.conflicts === 'exclude' ? 'text-cx-text-weak' : 'text-cx-text-weak-shimmer')
const enrollmentClass = computed(() => filters.enrollment !== 'any' ? 'outline outline-1 outline-cx-text-muted text-cx-text-weak' : 'text-cx-text-weak-shimmer')
const enrollmentIconClass = computed(() => filters.enrollment !== 'any' ? 'text-cx-text-weak' : 'text-cx-text-weak-shimmer')
</script>

<style scoped>
</style>



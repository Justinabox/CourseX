<template>
  <div
    class="w-full rounded-md border border-cx-border p-2.5 flex flex-col gap-2 overflow-hidden"
  >
    <!-- Title, Class Code and Description -->
    <div class="flex flex-col gap-1">
      <div class="flex items-center gap-2">
        <div class="flex items-center" v-if="isGESM || geLetters.length > 0">
          <span
            class="w-fit h-5 text-sm font-semibold line-clamp-1 leading-none grid place-items-center text-cx-text-weak-muted"
          >
            {{ isGESM ? 'GESM-' : 'GE-' }}
          </span>
          <div class="flex items-center" v-if="geLetters.length > 0">
              <span
              v-for="g in geLetters"
              :key="g"
              class="w-fit h-5 text-sm font-semibold line-clamp-1 leading-none grid place-items-center text-cx-text-weak-muted"
            >
              {{ g }}
            </span>
          </div>
        </div>
        <span class="text-sm truncate block flex-1 min-w-0 max-w-full">{{ title }}</span>
        <span class="text-xs text-cx-text-secondary font-semibold shrink-0 ml-auto">{{ code }}</span>
      </div>
      <span class="text-xs text-cx-text-secondary line-clamp-2">{{ description }}</span>
    </div>

    <div class="flex flex-col w-full">
      <div
        v-for="section in sections"
        :key="section.sectionId"
        class="flex gap-2 w-full border-t border-cx-border border-dashed cursor-pointer hover:bg-cx-surface-800/30 py-0.5"
        @mouseenter="onSectionHoverEnter(section)"
        @mouseleave="onSectionHoverLeave"
        @click="$emit('section-click', section.sectionId)"
      >
        <!-- Tags Display -->
        <div class="flex flex-col text-center">
          <span
            class="w-3.5 h-3.5 text-xs font-semibold line-clamp-1 leading-none grid place-items-center"
            :class="section.hasDClearance ? 'bg-zebra-sm-rose text-rose-500' : 'bg-zebra-sm text-cx-text-weak-shimmer'"
          >
            D
          </span>
          <span
            class="w-3.5 h-3.5 text-xs font-semibold line-clamp-1 leading-none grid place-items-center"
            :class="section.hasPrerequisites ? 'bg-zebra-sm-yellow text-yellow-500' : 'bg-zebra-sm text-cx-text-weak-shimmer'"
          >
            R
          </span>
          <span
            class="w-3.5 h-3.5 text-xs font-semibold line-clamp-1 leading-none grid place-items-center"
            :class="section.hasDuplicatedCredit ? 'bg-zebra-sm-green text-green-500' : 'bg-zebra-sm text-cx-text-weak-shimmer'"
          >
            C
          </span> 
        </div>
        
        <!-- Section Attributes -->
        <div class="flex flex-col gap-1 w-full justify-center" :class="sectionClassFor(section)">
          <div class="flex items-center gap-2 justify-between">
            <div class="flex items-center gap-1">
              <Icon name="uil:graduation-cap" class="h-4 w-4 text-cx-text-muted" />
              <!-- Instructors with RMP-based coloring and links -->
              <span class="text-xs font-semibold line-clamp-1">
                <template v-if="instructorViewsFor(section.sectionId).length > 0">
                  <template v-for="(item, idx) in instructorViewsFor(section.sectionId)" :key="item.name">
                    <a
                      :class="{ 'text-rose-600': item.isLow, 'text-cx-text-secondary': !item.isLow }"
                    >
                      {{ item.name }}
                    </a>
                    <span class="text-cx-text-muted" v-if="idx < instructorViewsFor(section.sectionId).length - 1">,&nbsp;</span>
                  </template>
                </template>
                <template v-else>
                  <span class="text-cx-text-secondary">{{ renderInstructors(section) }}</span>
                </template>
              </span>
            </div>

            <div class="flex items-center gap-1">
              <Icon name="uil:user" class="h-4 w-4" :class="occupancyClassFor(section, 'icon')" />
              <span class="text-xs line-clamp-1" :class="occupancyClassFor(section, 'text')">{{ section.enrolled }} / {{ section.capacity }}</span>
            </div>
          </div>

          <div class="flex items-center gap-2 justify-between">
            <div class="flex items-center gap-1">
              <Icon name="uil:clock" class="h-4 w-4 text-cx-text-muted" :class="scheduleCollisionClassFor(section, 'icon')" />
              <span class="text-xs text-cx-text-secondary line-clamp-1" :class="scheduleCollisionClassFor(section, 'text')">{{ section.schedule }}</span>
            </div>

            <div class="flex items-center gap-1">
              <Icon name="uil:location-point" class="h-4 w-4 text-cx-text-muted" />
              <span class="text-xs text-cx-text-secondary font-semibold line-clamp-1">{{ section.location }}</span>
            </div>
          </div>
        </div>

        <!-- Unit / Type Display -->
        <div class="flex flex-col text-center justify-center">
          <span
            class="text-sideways w-3.5 h-10.5 text-xs font-semibold line-clamp-1 leading-none grid place-items-center"
            :class="sectionTypeMetaClass(section)"
          >
            {{ sectionTypeLabel(section) }}
          </span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { getCourseTypeMeta } from '@/composables/useCourseTypeMeta'
import type { UICourse, UICourseSection } from '@/composables/useAPI'
import { useSchedule } from '@/composables/useSchedule'
import { useStore } from '@/composables/useStore'
import { useRMPRatings } from '@/composables/useRMPRatings'

defineEmits(['section-click'])

const props = defineProps<{
  title: string
  code: string
  description: string
  sections: UICourseSection[]
  ge?: string[]
}>()

const { checkScheduleCollision } = useStore()

const isGESM = computed(() => (props.code || '').toUpperCase().startsWith('GESM'))
const geLetters = computed(() => Array.from(new Set(props.ge || [])).filter(Boolean))

function occupancyClassFor(section: UICourseSection, type: 'icon' | 'text') {
  const isFull = (section.enrolled || 0) >= (section.capacity || 0)
  if (type === 'icon') return isFull ? 'text-rose-700' : 'text-cx-text-muted'
  else if (type === 'text') return isFull ? 'decoration-rose-700 text-rose-700 underline decoration-1' : 'text-cx-text-secondary font-semibold'
}

function scheduleCollisionClassFor(section: UICourseSection, type: 'icon' | 'text') {
  const isColliding = checkScheduleCollision(section.schedule).length > 0
  if (type === 'icon') return isColliding ? 'text-yellow-700' : 'text-cx-text-muted'
  else if (type === 'text') return isColliding ? 'decoration-yellow-700 text-yellow-700 underline decoration-1' : 'text-cx-text-secondary font-semibold'
}

function sectionClassFor(section: UICourseSection) {
  const isFull = (section.enrolled || 0) >= (section.capacity || 0)
  const isColliding = checkScheduleCollision(section.schedule).length > 0

  if (isFull || isColliding) return 'bg-dotted'
  else return ''
}

function sectionUnitsToRender(section: UICourseSection) {
  const value = section.units
  if (value == null) return ''
  return Number(value).toFixed(1)
}

function sectionMeta(section: UICourseSection) {
  return getCourseTypeMeta(section.type)
}

function sectionTypeMetaClass(section: UICourseSection) {
  const meta = sectionMeta(section)
  return meta ? `${meta.cardZebraClass} ${meta.cardTextClass}` : 'bg-zebra-sm text-cx-text-secondary'
}

function sectionTypeLabel(section: UICourseSection) {
  const meta = sectionMeta(section)
  return meta ? meta.cardLabel : sectionUnitsToRender(section)
}

function renderInstructors(section: UICourseSection) {
  const list = (section as any).instructors as string[] | undefined
  if (list && list.length > 0) return list.join(', ')
  return 'TBA'
}

// RMP integration per section
const { getProfessor } = useRMPRatings()
type InstructorView = { name: string; rating: number; link: string; isLow: boolean }
const instructorViewsBySection = reactive<Record<string, InstructorView[]>>({})

function instructorViewsFor(sectionId: string | undefined): InstructorView[] {
  if (!sectionId) return []
  return instructorViewsBySection[sectionId] || []
}

async function ensureSectionInstructorViews(section: UICourseSection) {
  const key = section.sectionId
  if (!key) return
  if (instructorViewsBySection[key]) return
  const rawList = (((section as any).instructors || []) as unknown[]).filter((x): x is string => typeof x === 'string')
  const names: string[] = Array.from(new Set<string>(rawList))
  if (names.length === 0) {
    instructorViewsBySection[key] = []
    return
  }
  const results = await Promise.all(
    names.map(async (name: string) => {
      const prof = await getProfessor(name)
      const rating = prof && typeof prof.rating === 'number' && !Number.isNaN(prof.rating) ? prof.rating : NaN
      const link = prof?.link || `https://www.ratemyprofessors.com/search/professors?q=${encodeURIComponent(name)}`
      const isLow = typeof rating === 'number' && !Number.isNaN(rating) ? rating < 3.0 : false
      return { name, rating, link, isLow } as InstructorView
    })
  )
  instructorViewsBySection[key] = results
}

watch(
  () => props.sections.map((s) => s.sectionId).join('|'),
  async () => {
    for (const section of props.sections) {
      void ensureSectionInstructorViews(section)
    }
  },
  { immediate: true }
)

// Hover preview per section
const { setHoverPreviewFromString, clearHoverPreview } = useSchedule()
function onSectionHoverEnter(section: UICourseSection) {
  if (!section.schedule) return
  setHoverPreviewFromString(section.schedule, props.title, props.code)
}
function onSectionHoverLeave() {
  clearHoverPreview()
}
</script>

<style scoped>
</style>


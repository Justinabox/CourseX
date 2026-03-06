<template>
  <div class="w-full h-full flex flex-col justify-between">
    <CourseDetailView />
    <ScheduleCalendar />
  </div>
</template>

<script setup lang="ts">
import CourseDetailView from '@/components/right/CourseDetailView.vue'
import ScheduleCalendar from '@/components/right/ScheduleCalendar.vue'
</script>

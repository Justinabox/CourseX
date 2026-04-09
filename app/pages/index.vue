<template>
  <div />
</template>

<script setup lang="ts">
const { activeTermCode } = useTerms()

const termCode = activeTermCode.value
if (termCode) {
  await navigateTo(`/course/${termCode}/all`, { redirectCode: 302 })
} else {
  await navigateTo('/course/all', { redirectCode: 302 })
}
</script>


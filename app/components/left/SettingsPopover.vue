<template>
  <div class="relative" ref="settingsRef">
    <button @click="settingsOpen = !settingsOpen" title="Settings" class="flex justify-center items-center p-2 rounded-md hover:bg-cx-surface-700/20">
      <Icon name="uil:cog" class="h-6 w-6 text-cx-text-subtle" />
    </button>
    <div v-if="settingsOpen" class="absolute bottom-full left-0 mb-2 w-48 rounded-md border border-cx-border bg-cx-surface-800/30 backdrop-blur shadow-lg p-2 z-50">
      <button @click="handleLogout" class="w-full flex items-center gap-2 p-2 rounded-md hover:bg-cx-surface-800/80 text-sm">
        <Icon name="uil:sign-out-alt" class="h-5 w-5" />
        <span>Logout</span>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue'

const { clear } = useUserSession()
const settingsOpen = ref(false)
const settingsRef = ref<HTMLElement | null>(null)

// Close on click outside or Escape
function onDocumentClick(e: MouseEvent) {
  if (!settingsOpen.value) return
  const root = settingsRef.value
  const target = e.target as Node | null
  if (root && target && !root.contains(target)) settingsOpen.value = false
}

function onDocumentKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') settingsOpen.value = false
}

onMounted(() => {
  document.addEventListener('click', onDocumentClick, true)
  document.addEventListener('keydown', onDocumentKeydown)
})

onBeforeUnmount(() => {
  document.removeEventListener('click', onDocumentClick, true)
  document.removeEventListener('keydown', onDocumentKeydown)
})

async function handleLogout() {
  await clear()
  navigateTo('/login')
}

</script>

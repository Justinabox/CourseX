<template>
  <div class="flex w-full h-full relative rounded-lg overflow-hidden">
    <div class="z-1 flex flex-col items-center w-92 bg-cx-surface-950/75 backdrop-blur-sm">

      <div class="flex flex-col gap-8 px-12 w-full h-full justify-center items-start">
        <div class="font-serif flex flex-col gap-1">
          <h1 class="text-4xl text-transparent bg-clip-text bg-gradient-to-r from-cx-brand-start to-cx-brand-end">
            CourseX
          </h1>
          <span class="text-sm text-cx-text-muted">Less searching, more learning.</span>
        </div>

        <a
          href="/auth/google"
          class="inline-flex items-center gap-3 px-5 py-2.5 rounded-md bg-white text-gray-700 font-medium text-sm hover:bg-gray-100 transition-colors w-full justify-center"
        >
          <svg class="w-5 h-5" viewBox="0 0 24 24">
            <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" />
            <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" />
            <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" />
            <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" />
          </svg>
          Sign in with Google
        </a>

        <p v-if="error === 'unauthorized'" class="text-sm text-cx-status-error-text text-center">
          Only USC email addresses (@usc.edu) are allowed.
        </p>
        <p v-else-if="error === 'db'" class="text-sm text-cx-status-error-text text-center">
          Something went wrong. Please try again later.
        </p>
        <p v-else-if="error === 'oauth'" class="text-sm text-cx-status-error-text text-center">
          Sign-in failed: {{ route.query.message || 'unknown error' }}
        </p>
      </div>


      <div class="flex flex-col items-center justify-center w-full py-2 border-t border-cx-border">
        <span class="text-sm text-cx-text-muted">Built with ❤️ by Korgo</span>
        <span class="text-[8px] text-cx-text-weak-muted">ver: <a href="https://github.com/MeloticZ/CourseX" class="underline hover:text-cx-text-secondary">{{ commitSha.slice(0, 7) }}</a> - data: {{ dataTimestamp }}</span>
      </div>

    </div>

    <ClientOnly class="absolute inset-0">
      <Shader>
        <Swirl
          color-a="#0b1329"
          color-b="#0c0f17"
          :detail="1.6"/>
        <Aurora
          :balance="73"
          blend-mode="linearDodge"
          :center="{'x':0.33,'y':0.36}"
          color-a="#8d54ff"
          color-b="#29ff8d"
          color-c="#1122d9"
          color-space="oklab"
          :height="84"
          :intensity="75"
          :ray-density="24"
          :seed="14"
          :speed="5.6"
          :waviness="78"/>
        <GridDistortion
          :grid-size="{
            type: 'map',
            source: '',
            channel: 'alpha',
            inputMax: 1,
            inputMin: 0,
            outputMax: 128,
            outputMin: 8
          }"
          :intensity="2"/>
        <FilmGrain
          :strength="0.1"/>
      </Shader>
    </ClientOnly>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import {
  Shader,
  Aurora,
  FilmGrain,
  GridDistortion,
  Swirl
} from 'shaders/vue'

const { loggedIn } = useUserSession()
const route = useRoute()
const error = computed(() => route.query.error as string | undefined)

const runtimeConfig = useRuntimeConfig()
const commitSha = computed(() => runtimeConfig.public.WORKERS_CI_COMMIT_SHA || 'dev')

const { data: meta } = await useFetch('/api/meta')
const dataTimestamp = computed(() => {
  const ts = meta.value?.coursesLastSuccess
  if (!ts) return 'N/A'
  const d = new Date(ts)
  const fmt = new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Los_Angeles',
    timeZoneName: 'short',
  })
  const parts = fmt.formatToParts(d)
  const p = (type: string) => parts.find(p => p.type === type)?.value ?? ''
  return `${p('year')}${p('month')}${p('day')} ${p('hour')}:${p('minute')} ${p('timeZoneName')}`
})

if (loggedIn.value) {
  navigateTo('/')
}
</script>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Audiowide&display=swap');

.font-serif {
  font-family: 'Audiowide', serif;
}
</style>

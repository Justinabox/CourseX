<template>
  <div class="flex w-full h-full relative rounded-lg overflow-hidden">
    <div class="z-1 flex flex-col items-center w-1/3 min-w-96 bg-cx-surface-950">

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
      </div>


      <div class="flex flex-col items-center justify-center w-full py-2 border-t border-cx-border">
        <span class="text-sm text-cx-text-muted">Built with ❤️ by Korgo</span>
        <span class="text-[8px] text-cx-text-weak-muted">ver: <a href="https://github.com/MeloticZ/CourseX" class="underline hover:text-cx-text-secondary">{{ commitSha.slice(0, 7) }}</a> - data: 20260124 00:28 PST</span>
      </div>

    </div>

    <ClientOnly class="absolute inset-0">
      <Shader>
        <Swirl
          :coarse-x="26"
          :coarse-y="24"
          color-a="#b106ba"
          color-b="#eb491c"
          color-space="oklch"
          :detail="0.7"
          :fine-x="50"
          :fine-y="50"
          :medium-x="50"
          :medium-y="50"
          :speed="1.5"/>
        <WaveDistortion
          :angle="223"
          edges="mirror"
          :frequency="1.7"
          :speed="0.1"
          :strength="0.54"
          wave-type="triangle">
          <Grid
            blend-mode="hardLight"
            :cells="29"
            :thickness="3.5"
            :transform="{'scale':0.75}"/>
        </WaveDistortion>
        <GridDistortion
          :decay="1.8"
          edges="wrap"
          :grid-size="8"
          :intensity="1"
          :radius="1.75"
          :swirl="0.1"/>
      </Shader>
    </ClientOnly>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import {
  Shader,
  Grid,
  GridDistortion,
  Swirl,
  WaveDistortion
} from 'shaders/vue'

const { loggedIn } = useUserSession()
const route = useRoute()
const error = computed(() => route.query.error as string | undefined)

const runtimeConfig = useRuntimeConfig()
const commitSha = computed(() => runtimeConfig.public.WORKERS_CI_COMMIT_SHA || 'dev')

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

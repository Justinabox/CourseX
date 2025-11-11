// https://nuxt.com/docs/api/configuration/nuxt-config
import { defineNuxtConfig } from 'nuxt/config'

export default defineNuxtConfig({
  modules: [
    '@nuxt/eslint',
    '@nuxt/ui',
    '@nuxt/content',
    '@pinia/nuxt',
  ],
  srcDir: 'app',
  css: [
    '@/assets/css/main.css',
  ],
  nitro: {
    // no server aliases needed
  },
  vite: {
    plugins: [],
  },
  app: {
    head: {
      meta: [
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      ],
    },
  },
  runtimeConfig: {
    public: {
      WORKERS_CI_COMMIT_SHA: process.env.WORKERS_CI_COMMIT_SHA || '',
    },
  },
})
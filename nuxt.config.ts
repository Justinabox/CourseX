// https://nuxt.com/docs/api/configuration/nuxt-config
import { defineNuxtConfig } from 'nuxt/config'

export default defineNuxtConfig({
  modules: [
    '@nuxt/eslint',
    '@nuxt/ui',
    '@nuxt/content',
    '@pinia/nuxt',
    'nuxt-auth-utils',
  ],
  srcDir: 'app',
  components: [
    { path: '~/components', pathPrefix: false },
  ],
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
      htmlAttrs: {
        lang: 'en',
      },
      title: 'CourseX',
      meta: [
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
        { name: 'description', content: 'Browse and search for courses at USC.' },
        { property: 'og:site_name', content: 'CourseX' },
        { property: 'og:type', content: 'website' },
      ],
    },
  },
  runtimeConfig: {
    oauth: {
      google: {
        clientId: '',
        clientSecret: '',
      },
    },
    session: {
      password: '',
    },
    databaseUrl: process.env.DATABASE_URL || '',
    public: {
      WORKERS_CI_COMMIT_SHA: process.env.WORKERS_CI_COMMIT_SHA || '',
      syllabusDomain: process.env.NUXT_PUBLIC_SYLLABUS_DOMAIN || '',
    },
  },
})
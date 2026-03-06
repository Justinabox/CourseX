import { queryPipelineMetaTimestamp } from '~~/server/db/queries'

export default defineEventHandler(async () => {
  const coursesLastSuccess = await queryPipelineMetaTimestamp('courses_last_success')
  return { coursesLastSuccess }
})

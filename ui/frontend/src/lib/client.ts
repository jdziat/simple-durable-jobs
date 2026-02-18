import { createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { JobsService } from './gen/jobs/v1/jobs_connect'
import { mockJobsClient } from './mock-client'

let jobsClient: typeof mockJobsClient

if (import.meta.env.VITE_DEMO_MODE === 'true') {
  jobsClient = mockJobsClient
} else {
  const transport = createConnectTransport({
    baseUrl: window.location.origin,
  })
  jobsClient = createClient(JobsService, transport) as typeof mockJobsClient
}

export { jobsClient }

// Re-export types for convenience
export type { Job, QueueStats, Checkpoint } from './gen/jobs/v1/jobs_pb'

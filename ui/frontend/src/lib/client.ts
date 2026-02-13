import { createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { JobsService } from './gen/jobs/v1/jobs_connect'

// Create transport for Connect-RPC
const transport = createConnectTransport({
  baseUrl: window.location.origin,
})

// Create typed client
export const jobsClient = createClient(JobsService, transport)

// Re-export types for convenience
export type { Job, QueueStats, Checkpoint } from './gen/jobs/v1/jobs_pb'

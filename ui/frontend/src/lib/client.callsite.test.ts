import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

// Covers the CALL SITE in client.ts, not the helper.
//
// FALSE-GREEN TRAP: rpcBaseUrl.test.ts exercises resolveRpcBaseUrl thoroughly, and
// reverting client.ts's `baseUrl:` back to `window.location.origin` — the original
// bug, which sends every RPC to a path outside the mount — leaves all five of its
// cases green. Testing a helper says nothing about whether anything calls it.
//
// This imports client.ts with a sub-path document and asserts the transport was
// constructed with the mount-relative base.
const createConnectTransport = vi.hoisted(() => vi.fn(() => ({})))
vi.mock('@connectrpc/connect-web', () => ({ createConnectTransport }))
vi.mock('@connectrpc/connect', () => ({ createClient: () => ({}) }))
vi.mock('./gen/jobs/v1/jobs_connect', () => ({ JobsService: {} }))
vi.mock('./mock-client', () => ({ mockJobsClient: {} }))

describe('client.ts RPC base URL', () => {
  beforeEach(() => {
    vi.resetModules()
    createConnectTransport.mockClear()
  })
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('addresses RPCs relative to the document, not the origin', async () => {
    // The dashboard mounted under /jobs/ — origin alone would be http://host.
    vi.stubGlobal('document', { baseURI: 'http://host/jobs/' })
    vi.stubGlobal('window', { location: { origin: 'http://host' } })

    await import('./client')

    expect(createConnectTransport).toHaveBeenCalledTimes(1)
    const [{ baseUrl }] = createConnectTransport.mock.calls[0] as unknown as [{ baseUrl: string }]
    expect(baseUrl).toBe('http://host/jobs/')
    expect(baseUrl).not.toBe('http://host')
  })

  it('is the origin at a root mount, as before', async () => {
    vi.stubGlobal('document', { baseURI: 'http://host/' })
    vi.stubGlobal('window', { location: { origin: 'http://host' } })

    await import('./client')

    const [{ baseUrl }] = createConnectTransport.mock.calls[0] as unknown as [{ baseUrl: string }]
    expect(baseUrl).toBe('http://host/')
  })
})

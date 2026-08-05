import { describe, expect, it } from 'vitest'
import { resolveRpcBaseUrl } from './rpcBaseUrl'

// The dashboard is mounted under a prefix by every shipped example
// (http.StripPrefix("/jobs", ui.Handler(storage))), and window.location.origin
// discards that prefix — so every RPC went to a path the operator's router does
// not serve.
//
// FALSE-GREEN TRAP: asserting only the root case passes with the old
// origin-based code, because at the root mount the origin IS the right answer.
// The sub-path cases are the ones that discriminate.
describe('resolveRpcBaseUrl', () => {
  it('keeps the mount prefix', () => {
    expect(resolveRpcBaseUrl('http://host/jobs/', 'http://host')).toBe('http://host/jobs/')
  })

  it('is unchanged at the root mount', () => {
    expect(resolveRpcBaseUrl('http://host/', 'http://host')).toBe('http://host/')
  })

  it('drops the hash route, which is where every SPA route lives', () => {
    expect(resolveRpcBaseUrl('http://host/jobs/#/queues?q=1', 'http://host')).toBe('http://host/jobs/')
  })

  it('resolves the directory when the shell is served as index.html', () => {
    expect(resolveRpcBaseUrl('http://host/jobs/index.html', 'http://host')).toBe('http://host/jobs/')
  })

  it('falls back rather than throwing when there is no usable base', () => {
    expect(resolveRpcBaseUrl(undefined, 'http://host')).toBe('http://host')
    expect(resolveRpcBaseUrl('not a url', 'http://host')).toBe('http://host')
  })
})

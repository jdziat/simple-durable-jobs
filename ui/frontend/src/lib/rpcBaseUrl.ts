/**
 * resolveRpcBaseUrl computes the base URL every Connect-RPC call is addressed at,
 * from the document's own base URL.
 *
 * The dashboard is served both at the root of a listener and — per the shipped
 * instructions — under a prefix via `http.StripPrefix`. `window.location.origin`
 * discards the prefix, so a dashboard at http://host/jobs/ would POST to
 * http://host/jobs.v1.JobsService/<Method>: outside the mount, and a 404 from the
 * operator's router. Resolving '.' against document.baseURI yields the DIRECTORY
 * the shell was served from (http://host/jobs/ or http://host/), with any
 * hash-route fragment and query dropped — exactly the prefix the Connect handler
 * is reachable under.
 *
 * connect-web joins with `baseUrl.replace(/\/?$/, "/svc/Method")`, so the trailing
 * slash this returns produces exactly one separator.
 *
 * Kept in its own side-effect-free module on purpose: client.ts touches
 * window.location and createConnectTransport at module scope, and vitest.config.ts
 * sets no `environment`, so importing client.ts under node throws.
 */
export function resolveRpcBaseUrl(baseURI: string | undefined, fallback: string): string {
  if (!baseURI) return fallback
  try {
    return new URL('.', baseURI).href
  } catch {
    return fallback
  }
}

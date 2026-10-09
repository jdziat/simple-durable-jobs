# Security Policy

## Supported versions

Security fixes are applied to the latest `4.x` release line. Pin a released tag
(`v4.x.y`) rather than tracking `main`.

CI runs `govulncheck` against the root module on every pull request and on
pushes to `main`; the release job depends on it, so a finding blocks the tag.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public GitHub issue.

- Preferred: use GitHub's [private vulnerability reporting](https://github.com/jdziat/simple-durable-jobs/security/advisories/new)
  ("Report a vulnerability" under the repository's Security tab).
- Alternatively, email the maintainer at the address on their GitHub profile.

Please include a description, affected version(s), the backend (SQLite /
Postgres / MySQL), and a reproduction if possible.

You can expect an acknowledgement within a few days. Once a fix is available
we'll coordinate a release and credit you in the advisory unless you prefer to
remain anonymous.

## Scope notes

This is a library that runs in your process against your database. A few things
are your responsibility, not vulnerabilities in the library:

- **Handler input.** Job arguments are application data; validate them in your
  handlers as you would any input.
- **The embedded UI** mounts a dashboard and a Connect-RPC API. Every RPC is
  denied by default: unless you configure authentication the handler returns
  `permission_denied` on every call. The primary mechanism is `ui.WithAuthorizer`,
  which is consulted per RPC (classified as a read or a mutation) — use it to gate
  access and, if you run multi-tenant, to pin the principal. State-changing
  (mutating) RPCs additionally enforce an origin check: a cross-origin browser
  caller must be allow-listed with `ui.WithAllowedOrigins` (same-origin is
  permitted automatically), which blocks CSRF against write endpoints. Reads are
  gated by the authorizer, not by origin. `ui.WithMiddleware` wraps the whole handler (and runs
  inside the H2C handler, so it applies to every HTTP/2 stream, not only the
  upgrade request) but does NOT satisfy the default-deny gate on its own — pairing
  middleware-only auth with the dashboard requires
  `ui.WithInsecureAllowUnauthenticated()`, which makes your middleware the sole
  gate; only do that when the middleware genuinely fails closed. Never expose the
  dashboard unauthenticated, especially with write endpoints enabled.
- **Database access.** The library is only as isolated as the database
  credentials and network you give it.

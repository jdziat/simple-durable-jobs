# Security Policy

## Supported versions

Security fixes are applied to the latest `1.x` release line. Pin a released tag
(`v1.x.y`) rather than tracking `main`.

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
- **The embedded UI** mounts a dashboard and a Connect-RPC API. It does not add
  authentication — wrap it with your own auth middleware (`ui.WithMiddleware`)
  and never expose it unauthenticated, especially with write endpoints enabled.
- **Database access.** The library is only as isolated as the database
  credentials and network you give it.

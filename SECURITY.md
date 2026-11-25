# Security policy

## Supported versions

Only the latest release receives security fixes. Upgrade before reporting a problem that an
earlier version shows.

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

Use GitHub's private reporting instead: on the repository page, **Security → Report a
vulnerability**. Include the version or commit, the configuration that matters (never the
secrets themselves), and the steps to reproduce.

Reports are handled on a best-effort basis; the fix is described in the changelog once released.

## Scope

In scope: anything that breaks a documented guarantee (a `200` for a message that is not durable,
an acknowledged message that is lost or silently altered), signature verification bypasses, ways to
read or write outside `DATA_DIR`, and denial of service that the documented limits should prevent.

Out of scope, by design (see the README):

- The admin listener has no authentication. Keep it on localhost or a trusted network.
- The engine does not terminate TLS. Put a reverse proxy in front of it.
- The data directory is not encrypted. It holds whatever the webhooks carried; protect the disk.

## How the project keeps itself current

CI runs `govulncheck` against the pinned minimum Go toolchain and the dependencies on every push,
the CI actions are pinned to commits, and Dependabot proposes updates for all three weekly.

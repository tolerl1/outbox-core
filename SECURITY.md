# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for security problems. Instead, use
GitHub's private vulnerability reporting on this repository
(*Security → Report a vulnerability*), which delivers the report privately
to the maintainer.

You can expect an acknowledgment within a week. Please include a minimal
reproduction and the version/commit affected.

## Supported versions

`outbox-core` is pre-1.0: only the latest `0.x` release receives fixes.
Security fixes are released as a new version, never as an in-place edit of
an existing one.

## Scope notes for reporters

Things that are known, documented behavior rather than vulnerabilities:

- `last_error` stores provider exception text verbatim for the retention
  window - the README documents that providers should sanitize what they
  raise and that the column should be treated as sensitive.
- The library executes no user-supplied SQL and interpolates no caller data
  into SQL text (bind parameters only); reports demonstrating otherwise are
  very much in scope.
- Delivery is at-least-once by design; duplicate delivery alone is not a
  vulnerability.

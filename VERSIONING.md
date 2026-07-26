# Versioning policy

`outbox-core` follows [Semantic Versioning](https://semver.org/), with rules
specific to this library because it has **two public surfaces that version
differently**: the Python API, and the database schema it ships as DDL. A
schema change can break a consumer in production with zero Python code
touched, so it needs its own rules, not just generic SemVer.

## What counts as what

| Change | Counts as |
|---|---|
| Python API — new function/parameter, new optional field | minor |
| Python API — signature change, removed/renamed public symbol | major |
| `MessageProvider` protocol shape change | **major, no exceptions** |
| Schema — new nullable column, new index | minor |
| Schema — column removed/renamed/retyped, new `NOT NULL` without a default | major, ships with a migration file + upgrade note |
| Public default value changes (e.g. default retention guidance, default backoff) | minor, called out explicitly in the changelog |
| Dropped Python / SQLAlchemy / Postgres version support | minor pre-1.0, major post-1.0 |

`MessageProvider` gets special treatment because it's the one type this
library doesn't just consume internally — every provider package (Storage
Queue, SQS, Event Grid, whatever gets built against it) implements it. A
change there breaks every downstream provider, not just direct callers of
`outbox-core`, so it's treated as breaking regardless of how small the actual
diff looks.

## 0.x

While the package sits at `0.x`, a breaking change bumps **minor**, not
major — that's standard SemVer for major version 0 ("anything may change at
any time"). `1.0.0` won't be cut until the public surface — `OutboxWriter`,
the `MessageProvider` protocol, `RelayConfig` — is something this project is
ready to hold stable.

`requires-python = ">=3.12"` is a **floor, not a sliding window** — 3.12
stays supported as newer minors are added to the CI matrix; it's never
dropped just for being old. Raising the floor is a deliberate,
changelog-flagged breaking change, versioned per the table above (minor
while at `0.x`, major after `1.0`).

## Supporting policy

- **Deprecation window** — once at 1.0+, a public symbol gets at least one
  minor release emitting `DeprecationWarning` before removal. Pre-1.0, no
  guaranteed window; `0.x` is explicitly still moving.
- **Upgrade notes** — any release with a schema change ships an explicit
  "Upgrading from vX" entry in `CHANGELOG.md` with the migration SQL, not
  just a bumped version number.
- **Consumer pinning guidance** — pin `>=0.a.b,<0.(a+1)` during `0.x`, since
  a minor bump can still break you there; standard caret pinning once at
  `1.0+`.
- **Supported runtime matrix** — Python `>=3.12` (floor, see above), the
  SQLAlchemy 2.x line, Postgres N-2 major versions (matching typical
  managed-Postgres support windows).

## Mechanics

Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
(`feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`, `build`, `perf`,
plus `!` or a `BREAKING CHANGE:` footer for breaks). Releases are cut with
[Commitizen](https://commitizen-tools.github.io/commitizen/):

```
cz bump --changelog
```

which reads commit history since the last tag, bumps the version in
`pyproject.toml`, writes `CHANGELOG.md`, and creates the git tag in one step.

PR titles (not individual commits) are the enforcement point, since this
repo squash-merges — a small CI check validates PR titles are
conventional-commit-shaped before merge.

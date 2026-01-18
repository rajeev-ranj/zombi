# Versioning Strategy

Zombi uses Semantic Versioning: `MAJOR.MINOR.PATCH`.

## Meaning
- `MAJOR`: breaking changes to API or storage compatibility.
- `MINOR`: new features or non-breaking behavior changes.
- `PATCH`: bug fixes, performance improvements, or docs.

## Release Channels
- `main` is stable and always releasable.
- Optional `develop` for integration work.
- `release/vX.Y` branches only when cutting a release.

## Release Process
1) Update `CHANGELOG.md` with user-facing changes.
2) Tag the release: `vX.Y.Z`.
3) Publish Docker images tagged `vX.Y.Z` and `latest`.

## Compatibility & Deprecation
- Deprecate features in one minor release with clear notices.
- Remove deprecated features in the next minor or next major release.

## Pre-1.0 Notes
- Breaking changes may occur in `0.x`, but must be documented in `CHANGELOG.md`.

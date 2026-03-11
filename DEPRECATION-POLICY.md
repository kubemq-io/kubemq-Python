# KubeMQ Python SDK — Deprecation Policy

## Timeline

- Deprecated APIs receive a `DeprecationWarning` at runtime (visible with `-Wd` flag).
- Deprecated APIs are documented in the CHANGELOG under a "Deprecated" section.
- Minimum notice: **2 minor versions or 6 months** (whichever is longer) before removal.
- Deprecated APIs continue to function identically until removed.
- Removal is a MAJOR version bump (semver).

## How Deprecation Is Communicated

1. **Runtime warning:** `warnings.warn("X is deprecated. Use Y instead.", DeprecationWarning)`
2. **CHANGELOG:** Entry in the "Deprecated" section of the version that introduces the deprecation.
3. **Migration guide:** For major version upgrades, a dedicated migration guide (e.g., `MIGRATION-v3-to-v4.md`).
4. **Type annotations:** Where supported by tooling (e.g., `typing_extensions.deprecated` for PEP 702 in Python ≥ 3.13).

## For SDK Developers

When deprecating an API:
1. Add `@deprecated(replacement="new_api()", since="X.Y.Z")` decorator.
2. Add a CHANGELOG entry.
3. Update this file if the deprecated API is a major feature.
4. Set the planned removal version (next MAJOR).

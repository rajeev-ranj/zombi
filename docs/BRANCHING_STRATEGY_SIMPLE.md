# Development Workflow (v0.x)

> Simplified workflow for single-maintainer, pre-v1.0 development

**When to upgrade:** Add process when you have external contributors or approach v1.0. See `BRANCHING_STRATEGY.md` for the full version.

---

## The Basics

```
main ──●──●──●──●──●──●──► (always deployable)
       │     │
       └─PR──┘ (optional, for larger changes)
```

**Two modes:**
1. **Direct push to main** — Small changes, CI protects quality
2. **PR with self-review** — Larger changes, documents decisions

---

## When to Use PRs

| Change Type | Direct Push | PR |
|-------------|-------------|-----|
| Bug fix (<50 lines) | ✅ | |
| Small feature (<100 lines) | ✅ | |
| Refactoring | | ✅ |
| New feature (>100 lines) | | ✅ |
| Architecture change | | ✅ |
| Anything you might regret | | ✅ |

**Rule of thumb:** If you'd want to explain it to future-you, use a PR.

---

## Branch Naming

```
<type>/<short-description>

Examples:
  feature/quic-endpoint
  fix/flush-deadlock
  refactor/storage-cleanup
```

Issue numbers optional until you have contributors.

---

## Commit Messages

```
<type>: <what changed>

Optional body explaining why.
```

**Types:** `feat`, `fix`, `perf`, `docs`, `test`, `refactor`, `chore`

**Examples:**
```
feat: add QUIC write endpoint
fix: prevent deadlock in flush path
perf: batch RocksDB writes
```

---

## CI Gates (Non-negotiable)

These run on every push and must pass:

| Check | What it catches |
|-------|-----------------|
| `cargo fmt --check` | Formatting issues |
| `cargo clippy -- -D warnings` | Code smells, bugs |
| `cargo test` | Broken functionality |
| `cargo deny check` | Vulnerable dependencies |

**If CI fails, fix it before moving on.**

---

## Pre-commit Hook

Install once:
```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

Catches issues before they hit CI:
- Formatting problems
- Clippy warnings
- TODOs without issue links
- Unwrap in non-test code (warning)

---

## 5 Guardrails for AI-Heavy Development

### 1. Issue-First Development

Create a GitHub issue before writing code. Even a one-liner like:
```
Title: Add health check endpoint
Body: Need /health endpoint for k8s probes
```

**Why:** Prevents AI from generating solutions to problems that don't exist.

### 2. No Orphan TODOs

```rust
// ❌ BAD
// TODO: handle errors

// ✅ GOOD
// TODO: #42 - handle timeout errors
```

**Why:** TODOs without issues never get done.

### 3. Meaningful Test Assertions

```rust
// ❌ BAD - proves nothing
assert!(result.is_ok());

// ✅ GOOD - verifies behavior
let seq = storage.write("topic", 0, b"data").unwrap();
assert_eq!(seq, 1);
let records = storage.read("topic", 0, 0, 10).unwrap();
assert_eq!(records[0].payload, b"data");
```

**Why:** AI writes tests that pass but don't verify correctness.

### 4. Justify New Dependencies

Before adding a crate, answer:
- Can I do this in <100 lines myself?
- Is it maintained? (Last commit, open issues)
- How many transitive deps does it add?

**Why:** AI suggests heavy dependencies for simple tasks.

### 5. The 30-Minute Rule

If you've spent 30 minutes debugging AI code without understanding the root cause:
1. Stop
2. Delete the code
3. Write a clear spec
4. Try again (or write it yourself)

**Why:** AI code is cheap to generate. Your time isn't.

---

## Releases

### Tagging a Release

```bash
# Update version in Cargo.toml
# Update CHANGELOG.md

git add -A
git commit -m "chore: release v0.2.0"
git tag v0.2.0
git push origin main --tags
```

CI automatically:
- Builds Docker image with version tag
- Runs integration tests
- Creates GitHub release

### Version Bumping

- **Patch (0.1.x):** Bug fixes
- **Minor (0.x.0):** New features
- **Major (x.0.0):** Breaking changes (wait for v1.0)

---

## Benchmarks

**Don't run on every PR.** They're slow and noisy on CI.

**When to benchmark:**
- Before releases
- After performance-critical changes
- When you suspect a regression

```bash
# Run locally
cargo bench

# Compare against baseline
cargo bench -- --save-baseline before
# make changes
cargo bench -- --baseline before
```

---

## Quick Reference

### Daily Workflow

```bash
# Start work
git checkout main
git pull

# Small change - direct push
# ... make changes ...
cargo test
git add -A
git commit -m "fix: handle empty payload"
git push

# Larger change - use PR
git checkout -b feature/new-endpoint
# ... make changes ...
git push -u origin feature/new-endpoint
gh pr create --fill
# self-review, then merge
gh pr merge --squash --delete-branch
```

### Before Pushing

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

### Creating a Release

```bash
# Edit Cargo.toml version
# Edit CHANGELOG.md
git add -A
git commit -m "chore: release v0.2.0"
git tag v0.2.0
git push origin main --tags
```

---

## When to Add More Process

| Trigger | Add |
|---------|-----|
| First external contributor | PR requirements, code review |
| v1.0 release | Release branches, stricter gates |
| Second maintainer | Approval requirements, CODEOWNERS |
| Production users | Integration test requirements |
| Security-sensitive deployment | Security review checklist |

Until then, keep it simple.

---

**Document Status:** Active for v0.x
**Full Version:** See `BRANCHING_STRATEGY.md` when ready to scale up

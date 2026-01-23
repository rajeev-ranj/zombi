# Branching, Merging & Release Strategy

> Development workflow and contribution guidelines for Zombi

**Version:** 1.0  
**Last Updated:** 2026-01-23  
**Related Docs:** [VERSIONING.md](../VERSIONING.md), [CHANGELOG.md](../CHANGELOG.md), [testing_strategy.md](../testing_strategy.md)

---

## Table of Contents

1. [Branch Structure](#branch-structure)
2. [Development Workflow](#development-workflow)
3. [Pull Request Requirements](#pull-request-requirements)
4. [Code Justification Requirements](#code-justification-requirements)
5. [Benchmark Regression Gates](#benchmark-regression-gates)
6. [Documentation Coherence](#documentation-coherence)
7. [Release Process](#release-process)
8. [Branch Protection Rules](#branch-protection-rules)
9. [Guardrails for AI-Heavy Contributions](#guardrails-for-ai-heavy-contributions)
10. [Semantic Review Guidelines](#semantic-review-guidelines)
11. [Fast-Track Process](#fast-track-process)

---

## Branch Structure

### Main Branches

| Branch | Purpose | Lifetime | Protected |
|--------|---------|----------|-----------|
| `main` | Production-ready code, always releasable | Permanent | ✅ Yes |
| `release/vX.Y` | Release preparation and backports | Per release | ✅ Yes |

### Working Branches

| Prefix | Purpose | Example | Merged Into |
|--------|---------|---------|-------------|
| `feature/` | New features, enhancements | `feature/streaming-endpoint` | `main` |
| `bugfix/` | Bug fixes | `bugfix/issue-42-offset-leak` | `main` |
| `perf/` | Performance improvements | `perf/rocksdb-write-batch` | `main` |
| `docs/` | Documentation only | `docs/update-api-spec` | `main` |
| `hotfix/` | Critical production fixes | `hotfix/v0.2.1-data-loss` | `main` + `release/*` |

### Branch Naming Convention

```
<type>/<issue-number>-<short-description>

Examples:
  feature/30-quic-write-path
  bugfix/issue-45-flush-deadlock
  perf/benchmark-write-throughput
  docs/fix-openapi-spec
```

**Rules:**
- All branches MUST reference a GitHub issue (except `docs/` branches)
- Use lowercase with hyphens, no underscores
- Keep descriptions short (3-5 words max)
- No personal branches (no `user/name-feature`)

---

## Development Workflow

### 1. Issue-First Development

**Every PR must be linked to a GitHub issue.**

```bash
# Check existing issues
gh issue list --label "enhancement"

# Create new issue if none exists
gh issue create --title "Add QUIC write path" --label "enhancement" --body "..."

# Remember the issue number for branch naming
```

**Exceptions:**
- Documentation-only changes (still recommended to have issues)
- Typo fixes in comments/docs
- CI/build configuration tweaks

### 2. Create Feature Branch

```bash
# Ensure main is up to date
git checkout main
git pull origin main

# Create branch with issue number
git checkout -b feature/30-quic-write-path

# Set upstream
git push -u origin feature/30-quic-write-path
```

### 3. Development Cycle

```bash
# Make changes
# Write tests FIRST (TDD preferred)
# Implement feature
# Verify tests pass

cargo test
cargo clippy -- -D warnings
cargo fmt --check

# Run benchmarks if touching performance-critical code
cargo bench
```

### 4. Commit Guidelines

**Commit Message Format:**
```
<type>(<scope>): <subject>

<body>

Fixes #<issue-number>
```

**Types:** `feat`, `fix`, `perf`, `docs`, `test`, `refactor`, `chore`

**Example:**
```
feat(storage): add QUIC write path

Implement QUIC-based write endpoint for reduced latency.
Uses quinn for async QUIC handling.

Performance: 30% reduction in p99 write latency.
Benchmark: write_quic shows 0.7µs vs 1.0µs HTTP.

Fixes #30
```

**Use the commit template:**
```bash
# One-time setup
git config commit.template .gitmessage
```

### 5. Push and Create PR

```bash
# Push changes
git push origin feature/30-quic-write-path

# Create PR with gh CLI
gh pr create \
  --title "Add QUIC write path" \
  --body "Closes #30" \
  --label "enhancement" \
  --assignee @me
```

---

## Pull Request Requirements

### Mandatory Sections (enforced by template)

1. **Changes Summary** - What changed and why
2. **Issue Reference** - `Closes #XXX` or `Fixes #XXX`
3. **Code Justification** - Why new code was necessary
4. **Testing Done** - What tests were added/run
5. **Performance Impact** - Benchmark results if applicable
6. **Breaking Changes** - Any API/behavior changes
7. **Documentation Updated** - Which docs were modified

### Pre-Submit Checklist

Before creating a PR, verify:

- [ ] All tests pass (`cargo test`)
- [ ] Linting passes (`cargo clippy -- -D warnings`)
- [ ] Formatting is correct (`cargo fmt --check`)
- [ ] Benchmarks compile (`cargo bench --no-run`)
- [ ] Documentation is updated (if needed)
- [ ] CHANGELOG.md updated (if user-facing)
- [ ] Issue is linked in PR description

### PR Size Guidelines

| Lines Changed | Classification | Review Requirement |
|---------------|----------------|-------------------|
| 1-50 | Trivial | 1 approval |
| 51-200 | Small | 1 approval |
| 201-500 | Medium | 1 approval + tests |
| 501-1000 | Large | 2 approvals + detailed rationale |
| 1000+ | Too Large | **Split into smaller PRs** |

**Note:** Generated code (e.g., `proto/`) excluded from line count.

### Review Process

1. **Automated Checks** (must pass before review)
   - CI tests (unit, integration, property)
   - Linting and formatting
   - Benchmark regression check
   - Documentation coherence check

2. **Human Review**
   - Code quality and correctness
   - Test coverage adequacy
   - Performance implications
   - Documentation completeness

3. **Approval Requirements**
   - **Code changes:** 1 maintainer approval
   - **Storage layer:** 2 maintainer approvals
   - **Breaking changes:** 2 maintainer approvals + deprecation plan
   - **Docs only:** 1 approval (can be fast-tracked)

### Merge Strategy

**Rebase and merge only** - No merge commits

```bash
# Update branch before merge
git checkout feature/30-quic-write-path
git fetch origin main
git rebase origin/main

# Resolve conflicts if any
git push --force-with-lease

# Merge via GitHub after approval
gh pr merge --rebase --delete-branch
```

---

## Code Justification Requirements

### Every Line Must Have a Reason

**Principle:** In AI-heavy repos, code can proliferate without necessity. Every new line must be justified.

### What Requires Justification

| Change Type | Justification Required |
|-------------|----------------------|
| New file (>100 lines) | Why this abstraction is needed |
| New dependency | Why existing tools insufficient |
| Algorithm change | Performance/correctness improvement |
| Code duplication | Why abstraction not possible |
| Large refactor | Benefits vs. risk |

### Where to Document Justification

1. **PR Description** - "Code Justification" section (mandatory)
2. **Commit Message** - Body should explain "why"
3. **Code Comments** - For non-obvious decisions

### Examples

**Good Justification:**
```
## Code Justification

Added new `QuicWriteHandler` (250 lines) because:
- HTTP overhead accounts for 40% of write latency (see benchmark)
- QUIC provides 0-RTT reconnection for persistent clients
- quinn library well-maintained, adds only 2 transitive deps
- Alternative (keep HTTP) would require connection pooling (more complex)

Benchmark comparison: write_http: 1.0µs → write_quic: 0.7µs
```

**Bad Justification:**
```
## Code Justification

Added QUIC support for better performance.
```

### Enforcement

- **Pre-commit hook:** Warns on large diffs (>200 lines) without issue link
- **PR template:** Required "Code Justification" section
- **Review:** Reviewers will reject PRs with inadequate justification

---

## Benchmark Regression Gates

### Performance is a Feature

**Any significant performance regression blocks the merge.**

### Regression Thresholds

| Regression | Action | Blocks Merge |
|------------|--------|--------------|
| 0-5% | ⚠️ Warning | No (log for tracking) |
| 5-10% | ⚠️ Requires justification | No (needs approval) |
| 10-20% | 🚫 Blocks merge | Yes (unless critical fix) |
| 20%+ | 🚫 Blocks merge | Yes (always) |

### How Regression is Measured

Benchmarks run **on-demand** (not automatically on every PR) to save CI resources.

**To trigger benchmark regression check:**
1. Add `performance` label to the PR, OR
2. Include `[benchmark]` in the PR title or body

When triggered, CI compares benchmarks against `main` branch:

```bash
# Run on PR branch
cargo bench --bench write_throughput -- --save-baseline pr-branch

# Compare against main
cargo bench --bench write_throughput -- --baseline main
```

**Note:** Benchmarks always run on releases (v* tags) regardless of labels.

Output shows percentage change:
```
write_single/1kb    time: [1.56 µs 1.58 µs 1.60 µs]
                    change: [-2.1% +0.3% +2.8%] (no significant change)
```

### Feature Releases with Benchmarks

**Before ANY feature release (v0.X.0), benchmarks MUST be run and pass regression gates.**

Process:
1. Run full benchmark suite: `cargo bench`
2. Compare against previous release baseline
3. Document results in CHANGELOG.md
4. If regression >5%, include justification in release notes

Example:
```markdown
## Baseline: v0.3.0 - Release Benchmarks

**Date:** 2026-02-15
**Compared To:** v0.2.0

### Changes
- write_single/1kb: 1.56µs → 1.68µs (+7.7%)
  - **Justification:** QUIC handshake overhead, mitigated by 0-RTT
  - **Net impact:** Positive for persistent connections (benchmarked separately)
```

### Exemptions

Regressions may be accepted if:
- **Critical bug fix** (data loss, security)
- **Necessary for correctness** (e.g., adding validation)
- **Trade-off documented** (e.g., +10% latency for -50% memory)

**All exemptions require 2 maintainer approvals + CHANGELOG entry.**

---

## Documentation Coherence

### Single Source of Truth Principle

Documentation must be consistent across all files.

### Documentation Hierarchy

1. **Authoritative Sources** (highest priority)
   - `../SPEC.md` - System design and API specification
   - `../README.md` - Quick start and overview
   - `../testing_strategy.md` - Testing approach
   - `openapi.yaml` - API contract

2. **Supporting Docs**
   - `CHANGELOG.md` - Version history
   - `VERSIONING.md` - Release strategy
   - `BENCHMARKS.md` - Performance baselines
   - `docs/BRANCHING_STRATEGY.md` (this file)

3. **Developer Guides**
   - `CLAUDE.md` - AI assistant instructions

4. **Archive** (may be outdated)
   - `docs/archive/*` - Historical documents

### Coherence Checks

**Automated validation script** (`scripts/check-docs-coherence.sh`) verifies:

1. **Version consistency**
   - `Cargo.toml` version matches CHANGELOG.md
   - README.md version references are current
   - SPEC.md version matches Cargo.toml

2. **Cross-reference validity**
   - File links exist (e.g., `[SPEC.md](../SPEC.md)`)
   - Issue references are valid GitHub issues
   - API endpoints in SPEC.md match openapi.yaml

3. **Feature completeness**
   - Features in CHANGELOG.md documented in SPEC.md
   - Config vars in SPEC.md exist in code

**Run before every PR:**
```bash
./scripts/check-docs-coherence.sh
```

**CI runs this automatically** - Failed checks block merge.

### Updating Documentation

When making changes that affect docs:

| Change Type | Docs to Update |
|-------------|----------------|
| New API endpoint | SPEC.md, openapi.yaml, README.md (examples) |
| New config var | SPEC.md, README.md (config table) |
|| Performance change | CHANGELOG.md (if >5% impact) |
| New feature | CHANGELOG.md, SPEC.md, README.md |
| Breaking change | CHANGELOG.md, VERSIONING.md, SPEC.md |
| Architecture change | SPEC.md, CLAUDE.md (if affects dev) |

---

## Release Process

### Release Types

Following [Semantic Versioning](https://semver.org/):

- **Major (X.0.0):** Breaking changes to API or storage
- **Minor (0.X.0):** New features, non-breaking changes
- **Patch (0.0.X):** Bug fixes, performance improvements

### Release Cadence

- **Minor releases:** Every 2-4 weeks (when features are ready)
- **Patch releases:** As needed for critical bugs
- **Major releases:** Rare, planned in advance

### Release Checklist

#### 1. Pre-Release Validation

```bash
# Ensure all tests pass
cargo test --all

# Run full benchmark suite
cargo bench

# Compare benchmarks against last release
cargo bench -- --baseline v0.2.0

# Check documentation coherence
./scripts/check-docs-coherence.sh

# Verify integration tests
docker-compose up -d minio
cargo test --test integration_tests
```

#### 2. Update Documentation

- [ ] Update `CHANGELOG.md` with all changes since last release
- [ ] Update version in `Cargo.toml`
- [ ] Update version references in `README.md`
- [ ] Document benchmark results in CHANGELOG.md (if feature release)
- [ ] Create migration guide if breaking changes

#### 3. Create Release Branch

```bash
# For minor/major releases
git checkout -b release/v0.3 main
git push origin release/v0.3
```

#### 4. Tag Release

```bash
# Create annotated tag
git tag -a v0.3.0 -m "Release v0.3.0"
git push origin v0.3.0
```

#### 5. Build and Publish

CI automatically:
- Builds Docker image tagged with version
- Publishes to GitHub Container Registry
- Runs integration tests against new image

#### 6. Create GitHub Release

```bash
gh release create v0.3.0 \
  --title "Zombi v0.3.0" \
  --notes-file CHANGELOG.md \
  --draft  # Review before publishing
```

#### 7. Post-Release

- [ ] Announce in README.md (update "Current Version")
- [ ] Update roadmap in SPEC.md if milestone reached
- [ ] Close completed GitHub issues
- [ ] Create issues for known limitations from release

### Hotfix Process (Critical Bugs)

```bash
# Create hotfix branch from release tag
git checkout -b hotfix/v0.2.1-data-loss v0.2.0

# Fix bug, commit, push
git commit -m "fix: prevent data loss in flush path"
git push origin hotfix/v0.2.1-data-loss

# Create PR targeting both main and release branch
gh pr create --base main
gh pr create --base release/v0.2

# After merge, tag and release
git tag v0.2.1
git push origin v0.2.1
```

---

## Branch Protection Rules

### GitHub Branch Protection (main)

**Configure via gh CLI:**

```bash
# Enable branch protection
gh api repos/:owner/:repo/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["lint","test","docs-check"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1}' \
  --field restrictions=null \
  --field required_linear_history=true \
  --field allow_force_pushes=false \
  --field allow_deletions=false
```

### Rules Enforced

1. **Required status checks:**
   - `lint` - Clippy and rustfmt
   - `test` - All test suites pass (includes benchmark compile check)
   - `docs-check` - Documentation coherence

2. **Pull request reviews:**
   - At least 1 approval required
   - Dismiss stale reviews on new commits
   - Code owners review for sensitive paths

3. **Linear history:**
   - Rebase and merge only
   - No merge commits
   - No force pushes to main

4. **Protected files** (require code owner review):
   - `src/contracts/*` - Interface definitions
   - `src/storage/*` - Storage layer
   - `src/flusher/*` - Flusher logic
   - `SPEC.md`, `VERSIONING.md` - Critical docs

### Code Owners

Create `.github/CODEOWNERS`:
```
# Storage layer - critical paths require owner review
/src/storage/ @rajeev-ranj
/src/contracts/ @rajeev-ranj
/src/flusher/ @rajeev-ranj

# Critical documentation
/SPEC.md @rajeev-ranj
/VERSIONING.md @rajeev-ranj

# CI/CD and build changes
/.github/workflows/ @rajeev-ranj
/Cargo.toml @rajeev-ranj
```

**Note:** For multi-maintainer projects, use `@org/team-name` syntax and list multiple owners for critical paths to require multiple reviews.

---

## Guardrails for AI-Heavy Contributions

### Context: Heavy AI Assistance

This project uses AI tools (Claude, GitHub Copilot, etc.) extensively. These guardrails ensure quality despite automation.

### Guardrails

#### 1. Mandatory Issue Linkage

**No orphan PRs allowed.**

- Every PR must close/reference a GitHub issue
- Issue must be created BEFORE coding starts
- Issue should describe problem, not implementation

**Enforcement:**
- PR template requires issue link
- Reviewers reject PRs without issues (except docs)
- Branch naming convention includes issue number

#### 2. Code Justification (see dedicated section)

- Every significant addition must explain "why"
- Pre-commit hook warns on large diffs
- PR template has mandatory justification section

#### 3. Test Coverage Requirements

| Module | Minimum Coverage | Status |
|--------|-----------------|--------|
| `contracts/` | 95% | 📋 Target |
| `storage/` | 90% | 📋 Target |
| `api/` | 85% | 📋 Target |
| Overall | 80% | 📋 Target |

**Note:** Coverage thresholds are targets for code review, not currently enforced by CI. Reviewers should reject PRs that significantly decrease coverage without justification.

**Future:** Consider adding `cargo-tarpaulin` or `cargo-llvm-cov` to CI for automated coverage tracking.

#### 4. Test Quality Verification

**Problem:** AI can achieve high coverage with meaningless tests.

**Red Flags in AI-Generated Tests:**
- Assertions that only check `is_ok()` or `is_some()` without verifying values
- Tests that mirror implementation logic (testing the code with itself)
- Missing edge cases (empty inputs, boundaries, error paths)
- Mock setups that return exactly what the code expects

**Review Checklist for Tests:**
- [ ] Would this test fail if the code had a bug?
- [ ] Does it test behavior, not implementation?
- [ ] Are error paths tested, not just happy paths?
- [ ] Do assertions verify meaningful values, not just `true`?

**Bad Example:**
```rust
#[test]
fn test_write() {
    let storage = RocksDbStorage::new(temp_dir());
    let result = storage.write("topic", 0, b"data");
    assert!(result.is_ok()); // Only checks no panic
}
```

**Good Example:**
```rust
#[test]
fn write_returns_monotonic_sequence() {
    let storage = RocksDbStorage::new(temp_dir());
    let seq1 = storage.write("topic", 0, b"first").unwrap();
    let seq2 = storage.write("topic", 0, b"second").unwrap();
    assert!(seq2 > seq1, "sequences must be monotonic");

    // Verify data actually persisted
    let records = storage.read("topic", 0, seq1, 10).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, b"first");
}
```

**Mutation Testing (Optional but Recommended):**
```bash
# Install cargo-mutants
cargo install cargo-mutants

# Run mutation testing on critical modules
cargo mutants --package zombi -- --lib
```

If mutations survive (tests still pass with bugs injected), tests are inadequate.

#### 5. Benchmark Regression Gates (see dedicated section)

- Benchmarks run on-demand (add `performance` label or `[benchmark]` keyword)
- >10% regression blocks merge when benchmarks are run
- Feature releases (v* tags) always run full benchmark suite

#### 6. No TODOs in Production Code

```rust
// ❌ BAD - Will be rejected
fn write(&self) -> Result<()> {
    // TODO: handle errors properly
    Ok(())
}

// ✅ GOOD - Create issue instead
fn write(&self) -> Result<()> {
    // See issue #123 for proper error handling
    Ok(())
}
```

**Enforcement:**
- Pre-commit hook scans for TODO/FIXME
- Must link to GitHub issue or be removed

#### 7. Dependency Management

**New dependencies require strong justification in PR.**

**Questions to answer:**
- Why can't existing dependencies solve this?
- What's the maintenance status? (stars, last commit, downloads)
- What's the transitive dependency cost?
- Are there lighter alternatives?

**Use `cargo deny` for automated checks:**

```bash
# Install
cargo install cargo-deny

# Check for vulnerabilities, duplicates, licenses
cargo deny check
```

**Configure in `deny.toml`:**
```toml
[advisories]
vulnerability = "deny"
unmaintained = "warn"
yanked = "deny"

[licenses]
allow = ["MIT", "Apache-2.0", "BSD-3-Clause"]

[bans]
multiple-versions = "warn"
```

**AI Dependency Risks:**

AI tends to:
- Suggest popular but heavy dependencies for simple tasks
- Not consider transitive dependency costs
- Recommend crates it was trained on (potentially outdated)

**Rule:** Before adding a dependency, check if the needed functionality is <100 lines to implement. If so, prefer vendoring or implementing directly.

#### 8. Small, Focused PRs

**AI can generate large changes quickly - resist this.**

- Keep PRs <500 lines (excluding generated code)
- One logical change per PR
- Split large features into incremental PRs

#### 9. Human-in-the-Loop for Critical Paths

**These areas require extra human scrutiny:**
- Storage layer (`src/storage/`)
- Contracts/interfaces (`src/contracts/`)
- Flusher logic (`src/flusher/`)
- Data serialization

**Requirement:** 2 human reviews, no rubber-stamping

#### 10. Security-Sensitive Code Review

**These paths require explicit security consideration in PR review:**

| Path/Pattern | Security Concern | Review Focus |
|--------------|------------------|--------------|
| `src/api/` input handling | Injection, DoS | Input validation, size limits |
| Serialization (`serde`, `prost`) | Deserialization attacks | Untrusted input handling |
| S3/external calls | SSRF, credential leaks | URL validation, error messages |
| File path handling | Path traversal | Canonicalization, sandboxing |
| Any `unsafe` block | Memory safety | Justification, alternatives |

**Security Review Checklist:**
- [ ] User input validated before use?
- [ ] Error messages don't leak internal details?
- [ ] Resource limits in place (max payload size, timeout)?
- [ ] No credentials in logs or error responses?
- [ ] External URLs validated (no SSRF)?

**AI Security Blind Spots:**

AI often generates code that:
- Trusts input structure without validation
- Uses `.unwrap()` on user-provided data (DoS via panic)
- Logs full request bodies (may contain secrets)
- Constructs file paths from user input without sanitization

**Require security-focused review for any PR touching these areas, regardless of size.**

#### 11. Recognizing When to Start Over

**Sometimes AI code should be rejected entirely, not fixed.**

**Signs the code should be thrown away:**

| Signal | Why It's a Problem |
|--------|-------------------|
| You've fixed 3+ bugs and found another | AI misunderstood the problem fundamentally |
| The "fix" keeps growing | Patching symptoms, not addressing cause |
| You can't explain what it does | If you can't review it, you can't maintain it |
| It passes tests but feels wrong | Trust your instincts; add better tests |
| Heavy use of workarounds | AI generated cargo-cult code |

**The 30-Minute Rule:**

If you've spent 30 minutes debugging AI-generated code without understanding the root cause:
1. Stop debugging
2. Write a clear spec of what the code should do
3. Try generating fresh with better prompts, or write it manually

**Sunk Cost Trap:**

AI code is cheap to generate. Don't fall into "but I've already spent time on this." Starting fresh with better understanding is often faster than continued debugging.

**Documentation Requirement:**

If AI-generated code is rejected, note why in the issue:
- What was the approach?
- Why did it fail?
- What approach worked instead?

This builds institutional knowledge about AI limitations for your codebase.

---

## Semantic Review Guidelines

### Problem: Syntactically Similar, Semantically Different

AI changes can look syntactically similar but alter behavior subtly.

### High-Risk Patterns to Watch

| Pattern | Risk | What to Check |
|---------|------|---------------|
| Renamed variables | Logic unchanged? | Ensure no accidental shadowing |
| Reordered operations | Order-dependent? | Check for side effects, short-circuits |
| Changed error handling | Errors still propagated? | Verify `?` vs `.unwrap()` vs `if let` |
| "Simplified" conditionals | Same truth table? | Test boundary conditions |
| Extracted functions | Same call context? | Check borrowed vs owned, lifetimes |

### Review Technique: The "What Changed" Test

For any non-trivial PR, ask:
1. What inputs would behave differently after this change?
2. What errors would be handled differently?
3. What ordering/timing assumptions changed?

If the PR author (human or AI) can't answer these clearly, the change isn't understood well enough to merge.

### Dangerous AI Refactoring Patterns

**Watch for "improvements" that change semantics:**

```rust
// BEFORE: Early return on error
fn process(data: &[u8]) -> Result<(), Error> {
    validate(data)?;  // Returns early if invalid
    transform(data)?;
    save(data)
}

// AFTER: AI "simplified" to
fn process(data: &[u8]) -> Result<(), Error> {
    let _ = validate(data);  // Oops: error silently ignored
    transform(data)?;
    save(data)
}
```

**Require explicit confirmation:** "This is a pure refactor with no behavior change" or "This changes behavior in the following ways: ..."

---

## Fast-Track Process

### When Speed Matters

Some situations require moving faster than standard guardrails allow:
- Production incident response
- Time-sensitive security patches
- Demo/deadline pressure (use sparingly)

### Fast-Track Labels

| Label | Skips | Requires | Post-Merge |
|-------|-------|----------|------------|
| `fast-track:hotfix` | PR size limits, benchmark gates | 1 maintainer approval | 48hr follow-up review |
| `fast-track:security` | All non-security checks | Security-focused review | Immediate post-merge audit |

**NOT skipped even with fast-track:**
- Tests must pass
- Linting must pass
- At least 1 human approval

### Using Fast-Track

```bash
# Create fast-track PR
gh pr create --label "fast-track:hotfix" \
  --title "fix: emergency patch for data corruption" \
  --body "Production issue affecting X users. Full review to follow."
```

### Post-Merge Requirements

Fast-tracked PRs create tech debt. Within 48 hours:
- [ ] Full code review completed
- [ ] Missing tests added
- [ ] Documentation updated
- [ ] Benchmark run (if applicable)
- [ ] Follow-up issue created for any shortcuts taken

### Abuse Prevention

- Fast-track usage is logged and reviewed monthly
- Repeated fast-tracks for same component = process problem, not speed problem
- More than 2 fast-tracks/month triggers retrospective

**Fast-track is a pressure valve, not a workflow.**

---

## Enforcement Scripts

### Pre-commit Hook (`scripts/pre-commit`)

```bash
#!/bin/bash
# Install: ln -s ../../scripts/pre-commit .git/hooks/pre-commit

# Check for large diffs
DIFF_LINES=$(git diff --cached --numstat | awk '{sum+=$1+$2} END {print sum}')
if [ "$DIFF_LINES" -gt 200 ]; then
    echo "⚠️  Large diff detected ($DIFF_LINES lines)"
    echo "   Ensure code justification is documented in commit message"
fi

# Check for TODOs without issue links
if git diff --cached | grep -E "^\+.*TODO" | grep -v "TODO.*#[0-9]"; then
    echo "❌ TODO found without issue link. Link issue or remove TODO."
    exit 1
fi

# Run tests on changed files
if git diff --cached --name-only | grep -E "\.rs$"; then
    cargo test || exit 1
fi
```

### Documentation Coherence Check (`scripts/check-docs-coherence.sh`)

```bash
#!/bin/bash
# Check version consistency
CARGO_VERSION=$(grep "^version" Cargo.toml | cut -d'"' -f2)
README_VERSION=$(grep "Current Version" README.md | grep -oE "[0-9]+\.[0-9]+\.[0-9]+")

if [ "$CARGO_VERSION" != "$README_VERSION" ]; then
    echo "❌ Version mismatch: Cargo.toml=$CARGO_VERSION, README.md=$README_VERSION"
    exit 1
fi

# Check for broken links
for file in *.md docs/*.md; do
    grep -oE '\[.*\]\([^)]+\)' "$file" | grep -oE '\([^)]+\)' | tr -d '()' | while read link; do
        if [[ $link != http* ]] && [ ! -f "$link" ]; then
            echo "❌ Broken link in $file: $link"
            exit 1
        fi
    done
done

echo "✅ Documentation coherence check passed"
```

---

## Quick Reference

### Starting Work on an Issue

```bash
# Find or create issue
gh issue create --title "Add feature X" --label "enhancement"

# Create branch
git checkout -b feature/123-feature-x main

# Make changes, test, commit
git commit -m "feat(scope): description\n\nFixes #123"

# Push and create PR
git push -u origin feature/123-feature-x
gh pr create --fill
```

### Before Submitting PR

```bash
# Validate
cargo test
cargo clippy -- -D warnings
cargo fmt --check
cargo bench --no-run
./scripts/check-docs-coherence.sh

# Update docs if needed
# Update CHANGELOG.md if user-facing change
```

### Merging PR

```bash
# After approval
gh pr merge --rebase --delete-branch
```

### Creating Release

```bash
# Update versions, CHANGELOG.md
# Run benchmarks, update BENCHMARKS.md
git tag v0.3.0
git push origin v0.3.0
gh release create v0.3.0 --notes-file CHANGELOG.md
```

---

## Questions?

- **Strategy questions:** Open issue with `documentation` label
- **Process improvements:** Submit PR to this document
- **Urgent clarifications:** Ask in PR comments

---

**Document Status:** ✅ Active  
**Next Review:** When v1.0.0 is released

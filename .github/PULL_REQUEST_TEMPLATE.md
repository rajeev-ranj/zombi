## Changes Summary

<!-- Provide a clear, concise description of what this PR does and why -->


## Issue Reference

<!-- Link to the GitHub issue this PR addresses -->
Closes #
<!-- Or use: Fixes #, Resolves #, Relates to # -->


## Code Justification

<!-- 
For significant code changes, explain WHY this code is necessary:
- Why is this approach chosen over alternatives?
- What problem does this solve?
- Why can't existing code/libraries solve this?
- For new dependencies: Why are they needed?
-->



## Testing Done

<!-- Describe the tests you added and/or ran -->

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Property tests added/updated (if touching invariants)
- [ ] All tests pass locally (`cargo test`)
- [ ] Benchmarks compile (`cargo bench --no-run`)

**Test coverage:**
<!-- Mention specific test files or functions added -->


## Performance Impact

<!-- For performance-critical changes, provide benchmark results -->

**Benchmarks run:** [ ] Yes / [ ] No / [ ] N/A

<!-- If yes, paste benchmark comparison output here -->
```
# Example:
write_single/1kb  time: [1.56 µs 1.58 µs 1.60 µs]
                  change: [-2.1% +0.3% +2.8%] (no significant change)
```

**Regression threshold:** [ ] Within acceptable limits (<10%)


## Breaking Changes

<!-- Does this PR introduce any breaking changes? -->

- [ ] No breaking changes
- [ ] Breaking changes (describe below)

<!-- If breaking changes, describe:
- What breaks?
- Migration path for users
- Deprecation timeline (if applicable)
-->


## Documentation Updated

<!-- Check all that apply -->

- [ ] CHANGELOG.md updated (if user-facing change)
- [ ] SPEC.md updated (if API/architecture change)
- [ ] README.md updated (if affects usage)
- [ ] BENCHMARKS.md updated (if performance baseline changed)
- [ ] Code comments added for non-obvious logic
- [ ] No documentation changes needed


## Pre-Submission Checklist

<!-- Verify all items before submitting -->

- [ ] All tests pass (`cargo test`)
- [ ] Linting passes (`cargo clippy -- -D warnings`)
- [ ] Formatting is correct (`cargo fmt --check`)
- [ ] Documentation coherence check passes (`./scripts/check-docs-coherence.sh`)
- [ ] Issue is linked in PR description
- [ ] Code justification provided (for significant changes)
- [ ] Commit messages follow convention (type(scope): subject)


## Additional Context

<!-- Any other relevant information, screenshots, or context -->


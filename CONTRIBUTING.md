# Contributing to Zombi

Thanks for your interest in contributing to Zombi!

## Quick Start

```bash
# Clone and setup
git clone https://github.com/rajeev-ranj/zombi.git
cd zombi

# Install pre-commit hook
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit

# Verify everything works
cargo test
cargo clippy -- -D warnings
```

## Development Workflow

See [`docs/BRANCHING_STRATEGY_SIMPLE.md`](docs/BRANCHING_STRATEGY_SIMPLE.md) for the full workflow.

### Short Version

1. **Create an issue first** — Describe what you want to do
2. **Fork and branch** — `git checkout -b feature/your-feature`
3. **Make changes** — Write tests, implement, verify
4. **Submit PR** — Reference the issue with `Closes #123`

### Before Submitting

```bash
cargo fmt           # Format code
cargo clippy -- -D warnings  # Check for issues
cargo test          # Run tests
```

## What We're Looking For

- Bug fixes with tests
- Performance improvements with benchmarks
- Documentation improvements
- New features (discuss in issue first)

## Code Style

- Run `cargo fmt` before committing
- No `unwrap()` or `expect()` in library code (use `?`)
- Write meaningful test assertions (not just `assert!(result.is_ok())`)
- Keep PRs focused — one logical change per PR

## Commit Messages

```
<type>: <description>

Types: feat, fix, perf, docs, test, refactor, chore
```

Examples:
```
feat: add batch write API
fix: prevent panic on empty payload
perf: reduce allocations in hot path
```

## Getting Help

- Open an issue for questions
- Check existing issues and PRs for context
- Read `SPEC.md` for architecture overview

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

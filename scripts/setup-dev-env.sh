#!/bin/bash
#
# Developer Environment Setup
# Run this once to configure your local development environment
#

set -e

echo "🛠️  Setting up Zombi development environment..."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 1. Git Configuration
# ─────────────────────────────────────────────────────────────────────────────

echo "1️⃣  Configuring git..."

# Set commit message template
if git config --get commit.template > /dev/null 2>&1; then
    echo "   ✓ Commit template already configured"
else
    git config commit.template .gitmessage
    echo "   ✓ Commit template configured (.gitmessage)"
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 2. Pre-commit Hook
# ─────────────────────────────────────────────────────────────────────────────

echo "2️⃣  Installing pre-commit hook..."

if [ -L .git/hooks/pre-commit ]; then
    echo "   ✓ Pre-commit hook already installed"
else
    ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
    echo "   ✓ Pre-commit hook installed"
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 3. Check Dependencies
# ─────────────────────────────────────────────────────────────────────────────

echo "3️⃣  Checking dependencies..."

# Check Rust
if command -v rustc &> /dev/null; then
    RUST_VERSION=$(rustc --version | cut -d' ' -f2)
    echo "   ✓ Rust $RUST_VERSION"
else
    echo "   ❌ Rust not found. Install from https://rustup.rs/"
    exit 1
fi

# Check protobuf compiler
if command -v protoc &> /dev/null; then
    PROTOC_VERSION=$(protoc --version | cut -d' ' -f2)
    echo "   ✓ protoc $PROTOC_VERSION"
else
    echo "   ⚠️  protoc not found. Install:"
    echo "      macOS: brew install protobuf"
    echo "      Linux: apt-get install protobuf-compiler"
fi

# Check gh CLI
if command -v gh &> /dev/null; then
    echo "   ✓ GitHub CLI (gh)"
else
    echo "   ⚠️  GitHub CLI not found. Recommended for workflow:"
    echo "      https://cli.github.com/"
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 4. Verify Build
# ─────────────────────────────────────────────────────────────────────────────

echo "4️⃣  Verifying build..."

if cargo build --quiet 2>&1 | tail -5; then
    echo "   ✓ Build successful"
else
    echo "   ❌ Build failed"
    exit 1
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 5. Run Tests
# ─────────────────────────────────────────────────────────────────────────────

echo "5️⃣  Running tests..."

if cargo test --quiet --lib 2>&1 | tail -10; then
    echo "   ✓ Tests passed"
else
    echo "   ❌ Tests failed"
    exit 1
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 6. Documentation Check
# ─────────────────────────────────────────────────────────────────────────────

echo "6️⃣  Checking documentation coherence..."

if ./scripts/check-docs-coherence.sh; then
    echo "   ✓ Documentation coherent"
else
    echo "   ⚠️  Documentation issues detected"
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════════════════════"
echo "✅ Development environment setup complete!"
echo ""
echo "Next steps:"
echo "  1. Read: docs/BRANCHING_STRATEGY.md"
echo "  2. Find an issue: gh issue list"
echo "  3. Create branch: git checkout -b feature/123-feature-name"
echo "  4. Make changes, test, commit"
echo "  5. Create PR: gh pr create --fill"
echo ""
echo "Useful commands:"
echo "  cargo test                    # Run tests"
echo "  cargo clippy -- -D warnings   # Lint"
echo "  cargo fmt                     # Format"
echo "  cargo bench                   # Benchmarks"
echo "  ./scripts/check-docs-coherence.sh  # Validate docs"
echo ""
echo "Happy coding! 🚀"

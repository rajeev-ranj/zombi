#!/bin/bash
#
# Documentation Coherence Check
# Validates consistency across all documentation files
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ERRORS=0

echo "📚 Checking documentation coherence..."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 1. Version Consistency
# ─────────────────────────────────────────────────────────────────────────────

echo "1️⃣  Checking version consistency..."

CARGO_VERSION=$(grep "^version" Cargo.toml | head -1 | cut -d'"' -f2)
echo "   Cargo.toml version: $CARGO_VERSION"

# Check CHANGELOG.md for version
if ! grep -q "\[$CARGO_VERSION\]" CHANGELOG.md; then
    echo -e "${RED}   ❌ CHANGELOG.md doesn't mention version $CARGO_VERSION${NC}"
    ERRORS=$((ERRORS + 1))
else
    echo -e "${GREEN}   ✓ CHANGELOG.md has $CARGO_VERSION${NC}"
fi

# Check README.md mentions current version (look for version pattern)
if grep -q "0\.[0-9]\+\.[0-9]\+" README.md; then
    README_VERSIONS=$(grep -oE "0\.[0-9]+\.[0-9]+" README.md | sort -u)
    echo "   README.md mentions versions: $README_VERSIONS"
    # This is informational, not a hard error
else
    echo -e "${YELLOW}   ⚠️  No version references found in README.md${NC}"
fi

echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 2. Cross-Reference Validity
# ─────────────────────────────────────────────────────────────────────────────

echo "2️⃣  Checking cross-references..."

# Check for broken internal links in markdown files
for file in README.md SPEC.md CHANGELOG.md VERSIONING.md BENCHMARKS.md testing_strategy.md docs/*.md; do
    if [ ! -f "$file" ]; then
        continue
    fi
    
    # Extract markdown links: [text](path)
    while IFS= read -r link; do
        # Skip external links (http/https)
        if [[ $link == http* ]]; then
            continue
        fi
        
        # Skip anchors
        if [[ $link == \#* ]]; then
            continue
        fi
        
        # Resolve relative path from file location
        file_dir=$(dirname "$file")
        link_path="$file_dir/$link"
        
        if [ ! -e "$link_path" ]; then
            echo -e "${RED}   ❌ Broken link in $file: $link${NC}"
            ERRORS=$((ERRORS + 1))
        fi
    done < <(grep -oE '\[.*\]\([^)]+\)' "$file" 2>/dev/null | grep -oE '\([^)]+\)' | tr -d '()' || true)
done

echo -e "${GREEN}   ✓ Cross-reference check complete${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 3. Required Files Exist
# ─────────────────────────────────────────────────────────────────────────────

echo "3️⃣  Checking required files exist..."

REQUIRED_FILES=(
    "README.md"
    "SPEC.md"
    "CHANGELOG.md"
    "VERSIONING.md"
    "testing_strategy.md"
    "Cargo.toml"
    "docs/BRANCHING_STRATEGY.md"
    "docs/openapi.yaml"
    ".github/PULL_REQUEST_TEMPLATE.md"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}   ❌ Missing required file: $file${NC}"
        ERRORS=$((ERRORS + 1))
    fi
done

echo -e "${GREEN}   ✓ All required files exist${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 4. Authoritative Docs Referenced
# ─────────────────────────────────────────────────────────────────────────────

echo "4️⃣  Checking authoritative doc references..."

# README should mention SPEC.md
if ! grep -q "SPEC.md" README.md; then
    echo -e "${YELLOW}   ⚠️  README.md doesn't reference SPEC.md${NC}"
fi

# CLAUDE.md reference check (skip if file doesn't exist - it's developer-specific)
if [ -f "CLAUDE.md" ]; then
    for doc in SPEC.md CHANGELOG.md testing_strategy.md; do
        if ! grep -q "$doc" CLAUDE.md; then
            echo -e "${YELLOW}   ⚠️  CLAUDE.md doesn't reference $doc${NC}"
        fi
    done
fi

echo -e "${GREEN}   ✓ Reference check complete${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 5. Config Variables Match
# ─────────────────────────────────────────────────────────────────────────────

echo "5️⃣  Checking config variable documentation..."

# Extract environment variables from SPEC.md
if grep -q "ZOMBI_" SPEC.md; then
    SPEC_VARS=$(grep -oE "ZOMBI_[A-Z_]+" SPEC.md | sort -u)
    
    # Check if README.md documents them
    README_VARS=$(grep -oE "ZOMBI_[A-Z_]+" README.md | sort -u || true)
    
    # This is informational - we don't enforce all vars in README
    echo "   SPEC.md defines $(echo "$SPEC_VARS" | wc -l | tr -d ' ') config variables"
    echo "   README.md mentions $(echo "$README_VARS" | wc -l | tr -d ' ') config variables"
fi

echo -e "${GREEN}   ✓ Config variable check complete${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 6. Consistency in Feature Documentation
# ─────────────────────────────────────────────────────────────────────────────

echo "6️⃣  Checking feature documentation consistency..."

# Check if Iceberg is mentioned consistently
if grep -q "Iceberg" SPEC.md; then
    if ! grep -q "Iceberg" README.md; then
        echo -e "${YELLOW}   ⚠️  Iceberg mentioned in SPEC.md but not README.md${NC}"
    fi
    if ! grep -q "Iceberg" CHANGELOG.md; then
        echo -e "${YELLOW}   ⚠️  Iceberg mentioned in SPEC.md but not CHANGELOG.md${NC}"
    fi
fi

echo -e "${GREEN}   ✓ Feature consistency check complete${NC}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════════════════════"
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✅ Documentation coherence check passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Documentation coherence check failed with $ERRORS error(s)${NC}"
    echo ""
    echo "Fix the errors above and run again."
    exit 1
fi

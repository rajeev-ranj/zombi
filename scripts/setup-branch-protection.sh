#!/bin/bash
#
# Setup GitHub branch protection rules for main branch
# Run this once to enable branch protection
#

set -e

OWNER="rajeev-ranj"
REPO="zombi"
BRANCH="main"

echo "🔒 Setting up branch protection for $OWNER/$REPO:$BRANCH..."

# Note: The default branch is currently 'feature/zombi-updates'
# This script will protect 'main' when it becomes the default branch

# Enable branch protection with required status checks
gh api \
  --method PUT \
  "/repos/$OWNER/$REPO/branches/$BRANCH/protection" \
  --field required_status_checks='{"strict":true,"contexts":["lint","test","docs-check"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true,"require_code_owner_reviews":true}' \
  --field restrictions=null \
  --field required_linear_history=true \
  --field allow_force_pushes=false \
  --field allow_deletions=false

echo "✅ Branch protection configured successfully!"
echo ""
echo "Protection settings:"
echo "  - Required status checks: lint, test, docs-check"
echo "  - At least 1 approving review required"
echo "  - Code owner reviews required"
echo "  - Linear history enforced (rebase only)"
echo "  - No force pushes allowed"
echo "  - No deletions allowed"
echo ""
echo "To view protection settings:"
echo "  gh api repos/$OWNER/$REPO/branches/$BRANCH/protection | jq"

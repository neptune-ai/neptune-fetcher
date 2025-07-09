#!/bin/bash

set -euo pipefail

COMMIT_MESSAGE="Update pyproject.toml for neptune-query

This is an automated commit updating pyproject.toml, README.md and release.yml for the neptune-query branch."

get_remote_hash() {
  curl -s https://api.github.com/repos/neptune-ai/neptune-fetcher/commits/$1 | jq -r .sha
}

# get git repository root
REPO_ROOT=$(git rev-parse --show-toplevel)
if ! [ -n "$REPO_ROOT" ]; then
  echo "Error: Could not determine repository root."
  exit 1
fi

# Verify we're on the same commit as origin/main
if [ "$(git rev-parse HEAD)" != "$(get_remote_hash main)" ]; then
  echo "Error: Local main is not in sync with origin/main."
  exit 1
fi

# Verify the source is clean
if ! git diff-index --quiet HEAD --; then
  echo "Error: Working directory is not clean."
  exit 1
fi

# Inject neptune-query versions of README.md, pyproject.toml, and release.yml
cp "$REPO_ROOT/.github/neptune_query/pyproject.toml" "$REPO_ROOT/pyproject.toml"
cp "$REPO_ROOT/.github/neptune_query/README.md" "$REPO_ROOT/README.md"
cp "$REPO_ROOT/.github/neptune_query/release.yml" "$REPO_ROOT/.github/workflows/release.yml"
git add "$REPO_ROOT/pyproject.toml" "$REPO_ROOT/README.md" "$REPO_ROOT/.github/workflows/release.yml"

# Set remote with GitHub token
git remote set-url origin https://x-access-token:${GITHUB_TOKEN}@github.com/neptune-ai/neptune-fetcher.git

# Configure Git user - doing it here, so it only works if run from GitHub Actions
git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Commit and push
git commit -m "$COMMIT_MESSAGE"
git fetch origin neptune-query
git push origin HEAD:neptune-query --force-with-lease

#!/bin/bash
#
# Required env variables:
#  - NEPTUNE_WORKSPACE - neptune workspace for testing
#  - NEPTUNE_API_TOKEN
#

set -e

if [[ -z "$NEPTUNE_WORKSPACE" ]]; then
  echo "Error: NEPTUNE_WORKSPACE is not set"
  exit 1
fi

if [[ -z "$NEPTUNE_API_TOKEN" ]]; then
  echo "Error: NEPTUNE_API_TOKEN is not set"
  exit 1
fi

REPOS="neptune-client neptune-fetcher neptune-api neptune-client-scale"
# Only those repos will be tested. We're skipping neptune-client because it's not
# currently compatible with Neptune Scale.
TEST_REPOS="neptune-fetcher neptune-api neptune-client-scale"
EXIT_CODE=-1

PROJECT="pye2e-$(date +%Y-%m-%d_%H-%M-%S)-$RANDOM"

cleanup() {
  set +e
  echo "Cleaning up..."

  # Go back to the initial directory
  pushd -0 > /dev/null && dirs -c > /dev/null

  echo "Deleting project $NEPTUNE_WORKSPACE/$PROJECT"
  python rest.py delete_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Exiting with code $EXIT_CODE"
  exit $EXIT_CODE
}

# Make sure cleanup is called at exit
trap cleanup SIGINT SIGTERM EXIT

# Update all repos to the latest version. Note that neptune-fetcher is on a special branch,
# this is ensured in Dockerfile when initially checking out the repo.
update_repos() {
  for repo in $REPOS; do
    echo "Updating ${repo}"
    pushd "$repo" >/dev/null

    branch="origin/$(git rev-parse --abbrev-ref HEAD)"
    echo "Resetting $repo to $branch"
    git fetch origin
    git reset --hard "$branch"
    git clean -fd

    popd > /dev/null
  done
}

update_dependencies() {
  for repo in $TEST_REPOS; do
    if [[ -f "$repo/dev_requirements.txt" ]]; then
      echo "Installing dev_requirements.txt for $repo"
      pushd "$repo" >/dev/null
      pip install -U -r dev_requirements.txt
      popd > /dev/null
    fi
  done

  # Global deps
  pip install -U -r requirements.txt
}

run_tests() {
  export NEPTUNE_PROJECT="$NEPTUNE_WORKSPACE/$PROJECT"

  echo "Creating project $NEPTUNE_PROJECT"
  python rest.py create_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Preparing test data"
  pushd neptune-fetcher >/dev/null
  python tests/populate_projects.py
  popd > /dev/null

  fail=0

  echo "Running tests..."
  for repo in $TEST_REPOS; do
    echo "Testing $repo"
    pushd "$repo" >/dev/null

    # This won't trigger an immediate exit if a test fails (we used `set -e` above),
    # so we can run all tests regardless of the outcome of a single one.
    if ! pytest -n auto --junitxml="/reports/$repo/test-results.xml"; then
      fail=1
    fi

    popd > /dev/null
  done

  EXIT_CODE="$fail"
}

update_repos
update_dependencies
run_tests

#!/bin/bash
#
# Required env variables:
#  - NEPTUNE_WORKSPACE - neptune workspace for testing
#  - NEPTUNE_API_TOKEN - the API token to use

set -e

if [[ -z "$NEPTUNE_WORKSPACE" ]]; then
  echo "Error: NEPTUNE_WORKSPACE is not set"
  exit 1
fi

if [[ -z "$NEPTUNE_API_TOKEN" ]]; then
  echo "Error: NEPTUNE_API_TOKEN is not set"
  exit 1
fi

EXIT_CODE=-1

UTF8_CHARS="你好()*+,-.;<=>@[]"
PROJECT="pye2e-fetcher-$(date +%Y-%m-%d_%H-%M-%S)-$RANDOM-$UTF8_CHARS"

cleanup() {
  # Don't fail tests if cleanup fails
  set +e

  echo "Cleaning up..."

  echo "Deleting project $NEPTUNE_WORKSPACE/$PROJECT"
  python .github/scripts/rest.py delete_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Exiting with code $EXIT_CODE"
  exit $EXIT_CODE
}

# Make sure cleanup is called at exit
trap cleanup SIGINT SIGTERM EXIT ERR

run_tests() {
  export NEPTUNE_PROJECT="$NEPTUNE_WORKSPACE/$PROJECT"

  echo "Creating project $NEPTUNE_PROJECT"
  python .github/scripts/rest.py create_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Preparing test data"
  python tests/populate_projects.py

  echo "Running tests..."
  pytest -n auto --dist loadgroup --junitxml="test-results/test-e2e.xml" tests/e2e

  EXIT_CODE=$?
}

run_tests

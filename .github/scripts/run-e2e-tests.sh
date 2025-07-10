#!/bin/bash
#
# Required env variables:
#  - NEPTUNE_WORKSPACE - neptune workspace for testing
#  - NEPTUNE_API_TOKEN - the API token to use
#  - PROJECT_DIR - where to cd before running pytest
#  - TESTS_DIR - what tests to run

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

# Our setup script requires neptune_fetcher, which is unfortunate...
# TODO: rewrite test setup

UV_PYTHON="uv run --no-project --with=ipython,neptune_fetcher,neptune_scale"

cleanup() {
  # Don't fail tests if cleanup fails
  set +e

  echo "Cleaning up..."

  echo "Deleting project $NEPTUNE_WORKSPACE/$PROJECT"
  $UV_PYTHON .github/scripts/rest.py delete_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Exiting with code $EXIT_CODE"
  exit $EXIT_CODE
}

# Make sure cleanup is called at exit
trap cleanup SIGINT SIGTERM EXIT ERR

run_tests() {
  export NEPTUNE_E2E_PROJECT_PREPOPULATED="$NEPTUNE_WORKSPACE/$PROJECT"

  echo "Creating project $NEPTUNE_E2E_PROJECT_PREPOPULATED"
  $UV_PYTHON .github/scripts/rest.py create_project "$NEPTUNE_WORKSPACE" "$PROJECT"

  echo "Preparing test data"
  NEPTUNE_PROJECT="${NEPTUNE_E2E_PROJECT_PREPOPULATED}" $UV_PYTHON tests/populate_projects.py

  echo "Running tests..."
  cd "$PROJECT_DIR" &&
  pytest --junitxml="test-results/test-e2e.xml" "$TESTS_DIR"

  EXIT_CODE=$?
}

run_tests

#!/bin/bash

# Required params:
#  - $1 - version to build (git tag)

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <git_ref>"
  echo "Example: $0 0.15.2-beta.2"
  exit 1
fi

GIT_REF="$1"
VERSION=$(echo "$GIT_REF" | sed -e 's,.*/\(.*\),\1,')

if [ -d dist ]; then
  rm -r dist
fi

# TODO: Remove this workaround once https://github.com/orgs/community/discussions/4924 gets resolved
# poetry-dynamic-versioning requires annotated tags to them by creation time

echo "VERSION=${VERSION}"
POETRY_DYNAMIC_VERSIONING_BYPASS="$VERSION" poetry build

echo "Build completed. Here are the built files:"
ls -lR dist/

# Fail the script if dist is empty:
ls dist/* 1>/dev/null

#!/bin/bash

# Required params:
#  - $1 - neptune_fetcher or neptune_query
#  - $2 - version to build (git tag)

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <package_name> <git_ref>"
  echo "Example: $0 neptune_fetcher neptune_fetcher/0.15.2-beta.2"
  exit 1
fi

PKG_NAME="$1"
GIT_REF="$2"

if [ "$PKG_NAME" != "neptune_fetcher" ] && [ "$PKG_NAME" != "neptune_query" ]; then
    echo "Invalid package name: $PKG_NAME (expected neptune_fetcher or neptune_query)"
    exit 1
fi

case "$GIT_REF" in
  neptune_fetcher/*)
    GIT_REF_PKG=neptune_fetcher
  ;;
  neptune_query/*)
    GIT_REF_PKG=neptune_query
  ;;
  *)
    echo "Invalid GIT_REF: $GIT_REF (expected neptune_fetcher/* or neptune_query/*)"
    exit 1
  ;;
esac

if [ "$GIT_REF_PKG" != "$PKG_NAME" ]; then
  echo "Package name mismatch: $PKG_NAME (expected $GIT_REF_PKG). Not building"
  exit 1
fi

VERSION=$(echo "$GIT_REF" | sed -e 's,.*/\(.*\),\1,')

if [ -d dist ]; then
  rm -r dist
fi

if [ -d neptune_fetcher/dist ]; then
  rm -r neptune_fetcher/dist
fi

if [ "$PKG_NAME" = "neptune_fetcher" ]; then
  cd neptune_fetcher
fi

# TODO: Remove this workaround once https://github.com/orgs/community/discussions/4924 gets resolved
# poetry-dynamic-versioning requires annotated tags to them by creation time

echo "VERSION=${VERSION}"
POETRY_DYNAMIC_VERSIONING_BYPASS="$VERSION" poetry build

if [ "$PKG_NAME" = "neptune_fetcher" ]; then
  cd ..
  mv neptune_fetcher/dist dist
fi

echo "Build completed. Here are the built files:"
tree dist/

# Fail the script if dist is empty:
ls dist/* 1>/dev/null

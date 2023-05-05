#!/usr/bin/env bash

set -e


(
GOOS=linux  go list ./... | grep -vE "tests|open_src" | xargs staticcheck -go=1.18 -tests=false -f binary
GOOS=darwin go list ./... | grep -vE "tests|open_src" | xargs staticcheck -go=1.18 -tests=false -f binary
) | staticcheck -merge | tee

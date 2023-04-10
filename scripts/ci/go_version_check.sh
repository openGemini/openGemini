#!/usr/bin/env bash

set -eo pipefail


function main () {
    local -r version_diff=$(go mod edit -go=$(go version | sed -n 's/^.*go\([0-9]*.[0-9]*\).*$/\1/p') -print | diff - go.mod)
    if [ -n "$version_diff" ]; then
        >&2 echo Error: unexpected difference in go version:
        >&2 echo "$version_diff"
        exit 1
    fi
}

main

#!/usr/bin/env bash

set -eo pipefail

function main () {
    # Compare language versions, so go.mod accepts both 1.25 and 1.25.0.
    local current_version required_version
    current_version=$(go version | sed -nE 's/^.*go([0-9]+\.[0-9]+).*$/\1/p')
    required_version=$(sed -nE 's/^[[:space:]]*go[[:space:]]+([0-9]+\.[0-9]+)(\.[0-9]+)?([[:space:]]|$).*/\1/p' go.mod)
    if [ -z "$current_version" ] || [ -z "$required_version" ] || [ "$current_version" != "$required_version" ]; then
        >&2 echo "Error: unexpected Go version (toolchain: ${current_version:-unknown}, go.mod: ${required_version:-unknown})"
        exit 1
    fi
}

main

#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
    printf 'Usage: %s container-vMAJOR.MINOR.PATCH\n' "$0" >&2
    exit 2
fi

reference=$1
if [[ ! "$reference" =~ ^container-v((0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*))$ ]]; then
    printf 'Container release tag must be container-vMAJOR.MINOR.PATCH.\n' >&2
    exit 2
fi
tag_version=${BASH_REMATCH[1]}
metadata=$(cargo metadata --locked --no-deps --format-version 1)
runtime_version=$(printf '%s' "$metadata" | jq -er \
    '[.packages[] | select(.name == "agent-broker-runtime")] | if length == 1 then .[0].version else error("expected exactly one agent-broker-runtime package") end')
if [[ ! "$runtime_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'agent-broker-runtime Cargo version must be plain numeric SemVer.\n' >&2
    exit 2
fi
if [[ "$tag_version" != "$runtime_version" ]]; then
    printf 'Container tag version %s does not match agent-broker-runtime %s.\n' \
        "$tag_version" "$runtime_version" >&2
    exit 2
fi

printf 'version=%s\n' "$runtime_version"

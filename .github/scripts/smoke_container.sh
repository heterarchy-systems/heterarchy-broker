#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
    printf 'Usage: %s <image-reference>\n' "$0" >&2
    exit 2
fi

image=$1
container="heterarchy-broker-ghcr-smoke-${GITHUB_RUN_ID:-local}-$$"
cleanup() {
    exit_status=$?
    if docker inspect "$container" >/dev/null 2>&1; then
        if (( exit_status != 0 )); then
            docker logs "$container" >&2 || true
        fi
        docker rm --force --volumes "$container" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

docker run --detach --platform linux/amd64 --name "$container" "$image" >/dev/null

for attempt in {1..30}; do
    health_status=$(docker inspect --format '{{.State.Health.Status}}' "$container")
    if [[ "$health_status" == healthy ]]; then
        printf 'Broker Docker health check passed.\n'
        exit 0
    fi
    container_status=$(docker inspect --format '{{.State.Status}}' "$container")
    if [[ "$container_status" != running ]]; then
        break
    fi
    sleep 2
done

printf 'Broker container did not become healthy.\n' >&2
exit 1

#!/usr/bin/env bash
#
# Episodic integration test runner for fiber-extras.
#
# Boots every broker (Redis, PostgreSQL, NATS JetStream, RabbitMQ) plus a
# one-shot Go test-runner container, waits for broker healthchecks, runs the
# integration suite, and tears everything down -- including volumes -- the
# moment the suite finishes (pass or fail). Exit code mirrors `go test`.
#
# Usage:
#   ./scripts/test-all-brokers.sh
#
# Requires exactly one of: docker compose | podman-compose | docker-compose.

set -uo pipefail

cd "$(dirname "$0")/.."

COMPOSE_FILE="docker-compose.test.yml"

# --- resolve the compose runtime -------------------------------------------
if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v podman-compose >/dev/null 2>&1; then
    COMPOSE=(podman-compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    echo "error: no compose runtime found (install 'docker compose' or 'podman-compose')" >&2
    exit 1
fi

echo "=== fiber-extras broker integration tests ($COMPOSE) ==="

# --- guarantee teardown on every exit path (pass, fail, Ctrl+C) -------------
cleanup() {
    echo ""
    echo "=== tearing down containers and volumes ==="
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- one-shot run: exit code comes from the test-runner container -----------
"${COMPOSE[@]}" -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from test-runner
status=$?

if [ "$status" -eq 0 ]; then
    echo "=== all broker integration tests passed ==="
else
    echo "=== broker integration tests failed (exit $status) ===" >&2
fi

exit "$status"

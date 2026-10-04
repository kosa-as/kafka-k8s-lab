#!/usr/bin/env bash
set -u

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WRAPPER="$SCRIPT_DIR/kafka_liveness.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

ORIGINAL="$TMP_DIR/original.sh"
cat > "$ORIGINAL" <<'EOF'
#!/usr/bin/env sh
exit "${ORIGINAL_RC:-0}"
EOF
chmod +x "$ORIGINAL"

run_case() {
  name=$1
  original_rc=$2
  status_body=$3
  expected=$4
  ttl=${5:-30}
  status_file="$TMP_DIR/status"

  if [ "$status_body" = __MISSING__ ]; then
    rm -f "$status_file"
  else
    printf '%b\n' "$status_body" > "$status_file"
  fi

  if ORIGINAL_RC="$original_rc" \
    KAFKA_HEALTH_ORIGINAL_CHECK="$ORIGINAL" \
    KAFKA_HEALTH_STATUS_FILE="$status_file" \
    KAFKA_HEALTH_STATUS_TTL_SECONDS="$ttl" \
    "$WRAPPER" >/dev/null 2>&1; then
    actual=0
  else
    actual=$?
  fi

  if [ "$actual" -ne "$expected" ]; then
    printf 'FAIL: %s (expected %s, got %s)\n' "$name" "$expected" "$actual" >&2
    return 1
  fi
  printf 'PASS: %s\n' "$name"
}

NOW=$(date +%s)
run_case 'fresh failure + original success' 0 "status=failure\nupdated_at=$NOW" 1
run_case 'stale failure + original success' 0 "status=failure\nupdated_at=$((NOW - 31))" 0
run_case 'malformed failure + original success' 0 "status=failure\nupdated_at=not-a-timestamp" 0
run_case 'missing status + original success' 0 __MISSING__ 0
run_case 'healthy status + original success' 0 "status=healthy\nupdated_at=$NOW" 0
run_case 'fresh failure + original failure' 1 "status=failure\nupdated_at=$NOW" 1
run_case 'healthy status + original failure' 1 "status=healthy\nupdated_at=$NOW" 1

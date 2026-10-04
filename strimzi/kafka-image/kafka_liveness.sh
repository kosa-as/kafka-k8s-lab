#!/usr/bin/env sh

status_file=${KAFKA_HEALTH_STATUS_FILE:-/var/run/kafka-health/status}
original_check=${KAFKA_HEALTH_ORIGINAL_CHECK:-/opt/kafka/kafka_liveness_strimzi.sh}
ttl_override=${KAFKA_HEALTH_STATUS_TTL_SECONDS:-30}

case "$ttl_override" in
  ''|*[!0-9]*) ttl=30 ;;
  *)
    if [ "$ttl_override" -gt 0 ] 2>/dev/null; then
      ttl=$ttl_override
    else
      ttl=30
    fi
    ;;
esac

if "$original_check"; then
  original_rc=0
else
  original_rc=$?
fi

if [ -f "$status_file" ] && [ -r "$status_file" ]; then
  parsed=$(awk '
    BEGIN { status_count = 0; timestamp_count = 0; timestamp_valid = 1 }
    /^status=/ {
      status_count++
      status = substr($0, 8)
    }
    /^updated_at=/ {
      timestamp_count++
      timestamp = substr($0, 12)
      if (timestamp !~ /^[0-9]+$/) timestamp_valid = 0
    }
    END {
      if (status_count != 1 || timestamp_count != 1 || !timestamp_valid) {
        print "invalid"
      } else {
        print status "|" timestamp
      }
    }
  ' "$status_file" 2>/dev/null) || parsed=invalid

  case "$parsed" in
    failure\|[0-9]*)
      updated_at=${parsed#*|}
      now=$(date +%s 2>/dev/null) || now=0
      age=$((now - updated_at))
      if [ "$age" -ge 0 ] && [ "$age" -le "$ttl" ]; then
        if [ "$original_rc" -eq 0 ]; then
          printf '%s\n' 'Kafka Health Sidecar reported a fresh failure' >&2
          exit 1
        fi
      fi
      ;;
  esac
fi

exit "$original_rc"

#!/bin/bash
# Run inside the disposable test container described in README.md.
set -eu
root=/var/run/kafka-health
mkdir -p /tmp/m1-mock-bin
for cmd in pgrep timeout; do
  printf '#!/bin/sh\nexit ${M1_NATIVE_RESULT:-0}\n' > "/tmp/m1-mock-bin/$cmd"
  chmod +x "/tmp/m1-mock-bin/$cmd"
done
export PATH="/tmp/m1-mock-bin:$PATH"
expect() {
  local expected=$1 mode=$2 rc=0
  bash /work/probe.sh "$mode" || rc=$?
  if [[ $rc != "$expected" ]]; then echo "Expected $expected for $mode, got $rc" >&2; exit 1; fi
}
snapshot() {
  printf 'topic=health-validation-m1-b2\ngeneration=%s\ntime=%s\nstarted=true\nready=%s\nexecFault=%s\n' "$1" "$2" "$3" "$4" > "$root/decision"
}
echo enforce > /opt/m1/mode
echo generation=current > "$root/epoch"
expect 1 ready
expect 0 live
snapshot current "$(date +%s%3N)" true 0
expect 0 ready
expect 0 live
snapshot current "$(( $(date +%s%3N) - 10000 ))" true 0
expect 1 ready
expect 0 live
snapshot current "$(( $(date +%s%3N) - 10000 ))" false 2
expect 1 live
snapshot previous "$(date +%s%3N)" true 2
expect 1 ready
expect 0 live
echo shadow > /opt/m1/mode
expect 0 ready
export M1_NATIVE_RESULT=1
expect 1 ready
expect 1 live
echo 'Probe freshness, generation, shadow, native and confirmed execution cases passed.'

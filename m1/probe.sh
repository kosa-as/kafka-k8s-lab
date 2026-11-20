#!/bin/bash
set -u
root=/var/run/kafka-health
readkey() { awk -F= -v key="$1" '$1 == key {print substr($0, length(key)+2); exit}' <<< "$snapshot"; }
if [[ $1 == live ]]; then pgrep -f kafka >/dev/null || exit 1
else timeout 2 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9093' || exit 1
fi
[[ $(cat /opt/m1/mode 2>/dev/null) != shadow ]] || exit 0
snapshot=$(cat "$root/decision" 2>/dev/null) || snapshot=""
epoch=$(sed -n 's/^generation=//p' "$root/epoch" 2>/dev/null)
topic=$(cat "$root/target" 2>/dev/null) || topic=health-validation-m1-b2
if [[ -z "$epoch" || $(readkey generation) != "$epoch" || $(readkey topic) != "$topic" ]]; then
    [[ $1 == live ]]; exit $?
fi
if [[ $1 == live ]]; then [[ $(readkey execFault) != 2 ]]; exit $?; fi
stamp=$(readkey time)
[[ $stamp =~ ^[0-9]+$ ]] || exit 1
age=$(( $(date +%s%3N) - stamp ))
[[ $age -ge 0 && $age -le 8000 && $(readkey ready) == true && $(readkey started) == true ]]

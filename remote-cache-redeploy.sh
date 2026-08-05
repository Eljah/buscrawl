#!/bin/bash
set -euo pipefail
printf 'Q1w2e3r4\n' | su - root -c 'systemctl stop buscrawl-dashboard-cache.service'
for i in $(seq 1 60); do
  state=$(printf 'Q1w2e3r4\n' | su - root -c 'systemctl is-active buscrawl-dashboard-cache.service' || true)
  if [ "$state" = "inactive" ] || [ "$state" = "failed" ]; then
    break
  fi
  sleep 2
done
printf 'Q1w2e3r4\n' | su - root -c 'systemctl start buscrawl-dashboard-cache.service'
sleep 5
printf '%s\n' '---status---'
printf 'Q1w2e3r4\n' | su - root -c 'systemctl status --no-pager buscrawl-dashboard-cache.service' || true
printf '%s\n' '---journal-last-20---'
printf 'Q1w2e3r4\n' | su - root -c 'journalctl -u buscrawl-dashboard-cache.service -n 20 --no-pager' || true
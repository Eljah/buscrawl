#!/bin/bash
set -euo pipefail
printf 'Q1w2e3r4\n' | su - root -c 'systemctl kill -s KILL buscrawl-dashboard-cache.service' || true
sleep 2
printf 'Q1w2e3r4\n' | su - root -c 'systemctl reset-failed buscrawl-dashboard-cache.service' || true
printf 'Q1w2e3r4\n' | su - root -c 'systemctl start buscrawl-dashboard-cache.service'
sleep 5
printf '%s\n' '---status---'
printf 'Q1w2e3r4\n' | su - root -c 'systemctl status --no-pager buscrawl-dashboard-cache.service' || true
printf '%s\n' '---journal-last-30---'
printf 'Q1w2e3r4\n' | su - root -c 'journalctl -u buscrawl-dashboard-cache.service -n 30 --no-pager' || true
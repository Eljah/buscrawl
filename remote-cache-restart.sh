#!/bin/bash
set -euo pipefail
printf 'Q1w2e3r4\n' | su - root -c 'systemctl restart buscrawl-dashboard-cache.service'
printf '%s\n' '---restarted---'
printf 'Q1w2e3r4\n' | su - root -c 'systemctl status --no-pager buscrawl-dashboard-cache.service' || true
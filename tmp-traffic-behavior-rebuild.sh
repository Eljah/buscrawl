#!/bin/bash
set -euo pipefail
cd /home/eljah/apps/buscrawl
rm -rf /home/eljah/data/buscrawl/traffic-behavior
mkdir -p /home/eljah/data/buscrawl/traffic-behavior
./bin/run-traffic-behavior-aggregation.sh
./bin/run-overtake-cache.sh
./bin/run-rubberiness-cache.sh
printf 'Q1w2e3r4\n' | su - root -c 'systemctl restart buscrawl-dashboard.service'

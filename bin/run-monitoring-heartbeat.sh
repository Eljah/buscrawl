#!/usr/bin/env bash
set -euo pipefail

TEXTFILE_DIR=${BUS_MONITORING_TEXTFILE_DIR:-/home/eljah/data/monitoring/node_exporter_textfile}
STATE_FILE=${BUS_MONITORING_HEARTBEAT_STATE_FILE:-/home/eljah/data/monitoring/heartbeat-state.tsv}
INTERVAL_SECONDS=${BUS_MONITORING_HEARTBEAT_INTERVAL_SECONDS:-1}
STALL_THRESHOLD_SECONDS=${BUS_MONITORING_HEARTBEAT_STALL_THRESHOLD_SECONDS:-30}
METRIC_FILE="$TEXTFILE_DIR/buscrawl-heartbeat.prom"

mkdir -p "$TEXTFILE_DIR" "$(dirname "$STATE_FILE")"

exec python3 - "$METRIC_FILE" "$STATE_FILE" "$INTERVAL_SECONDS" "$STALL_THRESHOLD_SECONDS" <<'PY'
import os
import sys
import time

metric_file = sys.argv[1]
state_file = sys.argv[2]
interval = float(sys.argv[3])
stall_threshold = float(sys.argv[4])


def read_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r", encoding="ascii") as fh:
            return fh.read().strip()
    except OSError:
        return "unknown"


def read_state():
    state = {
        "boot_id": "",
        "last_wall": 0.0,
        "stall_total": 0.0,
        "reboot_total": 0.0,
        "max_delay": 0.0,
        "last_delay": 0.0,
        "previous_boot_last_seen": 0.0,
    }
    try:
        with open(state_file, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line or "\t" not in line:
                    continue
                key, value = line.split("\t", 1)
                if key == "boot_id":
                    state[key] = value
                elif key in state:
                    try:
                        state[key] = float(value)
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return state


def atomic_write(path, content):
    tmp = path + ".tmp." + str(os.getpid())
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(content)
    os.replace(tmp, path)


def write_state(state):
    lines = []
    for key in (
        "boot_id",
        "last_wall",
        "stall_total",
        "reboot_total",
        "max_delay",
        "last_delay",
        "previous_boot_last_seen",
    ):
        lines.append(f"{key}\t{state[key]}\n")
    atomic_write(state_file, "".join(lines))


def write_metrics(state, wall, mono, delay):
    lines = [
        "# HELP buscrawl_host_heartbeat_timestamp_seconds Last successful host heartbeat wall-clock timestamp.\n",
        "# TYPE buscrawl_host_heartbeat_timestamp_seconds gauge\n",
        f"buscrawl_host_heartbeat_timestamp_seconds {wall:.3f}\n",
        "# HELP buscrawl_host_heartbeat_monotonic_seconds Last successful host heartbeat monotonic timestamp.\n",
        "# TYPE buscrawl_host_heartbeat_monotonic_seconds gauge\n",
        f"buscrawl_host_heartbeat_monotonic_seconds {mono:.3f}\n",
        "# HELP buscrawl_host_heartbeat_loop_delay_seconds Delay between heartbeat loop iterations.\n",
        "# TYPE buscrawl_host_heartbeat_loop_delay_seconds gauge\n",
        f"buscrawl_host_heartbeat_loop_delay_seconds {delay:.3f}\n",
        "# HELP buscrawl_host_heartbeat_max_loop_delay_seconds Maximum observed heartbeat loop delay since state creation.\n",
        "# TYPE buscrawl_host_heartbeat_max_loop_delay_seconds gauge\n",
        f"buscrawl_host_heartbeat_max_loop_delay_seconds {state['max_delay']:.3f}\n",
        "# HELP buscrawl_host_heartbeat_stall_events_total Count of heartbeat loop delays above threshold, i.e. recovered stalls.\n",
        "# TYPE buscrawl_host_heartbeat_stall_events_total counter\n",
        f"buscrawl_host_heartbeat_stall_events_total {state['stall_total']:.0f}\n",
        "# HELP buscrawl_host_heartbeat_reboot_events_observed_total Count of heartbeat writer starts after a different boot id.\n",
        "# TYPE buscrawl_host_heartbeat_reboot_events_observed_total counter\n",
        f"buscrawl_host_heartbeat_reboot_events_observed_total {state['reboot_total']:.0f}\n",
        "# HELP buscrawl_host_heartbeat_previous_boot_last_seen_timestamp_seconds Last heartbeat timestamp observed before the previous boot ended.\n",
        "# TYPE buscrawl_host_heartbeat_previous_boot_last_seen_timestamp_seconds gauge\n",
        f"buscrawl_host_heartbeat_previous_boot_last_seen_timestamp_seconds {state['previous_boot_last_seen']:.3f}\n",
        "# HELP buscrawl_host_heartbeat_stall_threshold_seconds Loop delay threshold used to count recovered stalls.\n",
        "# TYPE buscrawl_host_heartbeat_stall_threshold_seconds gauge\n",
        f"buscrawl_host_heartbeat_stall_threshold_seconds {stall_threshold:.3f}\n",
    ]
    atomic_write(metric_file, "".join(lines))


state = read_state()
boot_id = read_boot_id()
if state["boot_id"] and state["boot_id"] != boot_id:
    state["reboot_total"] += 1.0
    state["previous_boot_last_seen"] = state["last_wall"]
state["boot_id"] = boot_id

last_mono = time.monotonic()
while True:
    wall = time.time()
    mono = time.monotonic()
    delay = max(0.0, mono - last_mono)
    if delay >= stall_threshold:
        state["stall_total"] += 1.0
    state["last_wall"] = wall
    state["last_delay"] = delay
    state["max_delay"] = max(float(state["max_delay"]), delay)
    write_state(state)
    write_metrics(state, wall, mono, delay)
    last_mono = mono
    time.sleep(interval)
PY

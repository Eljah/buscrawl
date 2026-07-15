#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TCP_HOST=${BUS_TCP_HOST:-127.0.0.1}
export BUS_TCP_PORT=${BUS_TCP_PORT:-9999}
export BUS_INGEST_SOURCE=${BUS_INGEST_SOURCE:-spool}
export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_READY_DIR=${BUS_RAW_SPOOL_READY_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool/ready}
export BUS_RAW_SPOOL_MAX_FILES_PER_TRIGGER=${BUS_RAW_SPOOL_MAX_FILES_PER_TRIGGER:-64}
export BUS_PARQUET_STAGING_DIR=${BUS_PARQUET_STAGING_DIR:-${BUS_STORAGE_ROOT}/bus-data-parquet-staging}
export BUS_RAW_PARQUET_MANIFEST_FILE=${BUS_RAW_PARQUET_MANIFEST_FILE:-${BUS_STORAGE_ROOT}/bus-data-parquet-manifest.tsv}
export BUS_STREAM_REPAIR_CHECKPOINT_ON_START=${BUS_STREAM_REPAIR_CHECKPOINT_ON_START:-true}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

# File source checkpoints are durable. Do not delete them during normal restarts.
if [[ "$BUS_STREAM_REPAIR_CHECKPOINT_ON_START" == "true" ]]; then
  python3 - "$BUS_STORAGE_ROOT" <<'PY'
import os
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

storage_root = Path(sys.argv[1])
checkpoint = storage_root / "bus-data-checkpoint"
backup_root = storage_root / "checkpoint-corrupt-backup" / ("stream-start-" + datetime.now().strftime("%Y%m%d%H%M%S"))
scan_limit = int(os.environ.get("BUS_STREAM_REPAIR_SCAN_LIMIT", "80"))

numeric_name = re.compile(r"^\d+$")
compact_name = re.compile(r"^(\d+)\.compact$")
tmp_name = re.compile(r"^\.\..*\.tmp(\.crc)?$")

def numeric_files(directory):
    if not directory.exists():
        return {}
    result = {}
    for path in directory.iterdir():
        if path.is_file() and numeric_name.match(path.name):
            result[int(path.name)] = path
    return result

def compact_files(directory):
    if not directory.exists():
        return {}
    result = {}
    for path in directory.iterdir():
        if not path.is_file():
            continue
        match = compact_name.match(path.name)
        if match:
            result[int(match.group(1))] = path
    return result

def has_compact_covering(directory, batch_id):
    if not directory.exists():
        return False
    for path in directory.iterdir():
        if not path.is_file() or not path.name.endswith(".compact"):
            continue
        prefix = path.name.split(".", 1)[0]
        if prefix.isdigit() and int(prefix) >= batch_id and path.stat().st_size > 0:
            return True
    return False

def backup_and_remove(path):
    if not path.exists():
        return False
    target_dir = backup_root / path.parent.relative_to(checkpoint)
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, target_dir / path.name)
    path.unlink()
    return True

def remove_record(directory, batch_id):
    removed = False
    removed |= backup_and_remove(directory / str(batch_id))
    removed |= backup_and_remove(directory / ("." + str(batch_id) + ".crc"))
    return removed

def remove_compact_record(directory, batch_id):
    removed = False
    removed |= backup_and_remove(directory / (str(batch_id) + ".compact"))
    removed |= backup_and_remove(directory / ("." + str(batch_id) + ".compact.crc"))
    return removed

def valid_regular(path):
    return path.exists() and path.is_file() and path.stat().st_size > 0

def valid_json_lines(path):
    if not valid_regular(path):
        return False
    try:
        with path.open("r", encoding="utf-8") as fh:
            version = fh.readline()
            if version.rstrip("\n\r") != "v1":
                return False
            saw_payload = False
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    return False
                json.loads(stripped)
                saw_payload = True
            return saw_payload
    except Exception:
        return False

def remove_tmp_files(directory):
    if not directory.exists():
        return False
    removed = False
    for path in directory.iterdir():
        if path.is_file() and tmp_name.match(path.name):
            removed |= backup_and_remove(path)
    return removed

sources_dir = checkpoint / "sources" / "0"
offsets_dir = checkpoint / "offsets"
commits_dir = checkpoint / "commits"

if not checkpoint.exists():
    raise SystemExit(0)

try:
    backup_root.mkdir(parents=True, exist_ok=True)
except PermissionError:
    # Older manual repairs may leave the shared backup directory root-owned.
    # The stream runs as eljah, so fall back to a checkpoint-local backup path
    # rather than failing before Spark can repair/replay the tail.
    backup_root = checkpoint / "_repair-backup" / ("stream-start-" + datetime.now().strftime("%Y%m%d%H%M%S"))
    backup_root.mkdir(parents=True, exist_ok=True)

sources = numeric_files(sources_dir)
source_compacts = compact_files(sources_dir)
offsets = numeric_files(offsets_dir)
commits = numeric_files(commits_dir)
all_ids = set(sources) | set(source_compacts) | set(offsets) | set(commits)
if not all_ids:
    raise SystemExit(0)

changed = False
changed |= remove_tmp_files(sources_dir)
changed |= remove_tmp_files(offsets_dir)
changed |= remove_tmp_files(commits_dir)

while True:
    repaired_round = False
    for batch_id in sorted(all_ids, reverse=True)[:scan_limit]:
        compact_path = source_compacts.get(batch_id)
        if compact_path is not None and not valid_json_lines(compact_path):
            if remove_compact_record(sources_dir, batch_id):
                changed = True
                repaired_round = True
            continue

        source_path = sources.get(batch_id)
        offset_path = offsets.get(batch_id)
        commit_path = commits.get(batch_id)

        source_invalid = source_path is not None and not valid_json_lines(source_path)
        offset_invalid = offset_path is not None and not valid_regular(offset_path)
        commit_invalid = commit_path is not None and not valid_regular(commit_path)

        remove_source = False
        remove_offset = False
        remove_commit = False

        if source_invalid:
            # FileStreamSource cannot recover a zero/incomplete source log at the tail.
            remove_source = True
            remove_offset = True
            remove_commit = True
        elif source_path is None and not has_compact_covering(sources_dir, batch_id) and (offset_path is not None or commit_path is not None):
            # Spark reports "batch N doesn't exist" when offsets point beyond source logs.
            remove_offset = True
            remove_commit = True
        elif offset_invalid:
            # Offsets are generated by Spark; a zero tail offset can be safely retried.
            remove_offset = True
            remove_commit = True
        elif commit_invalid:
            remove_commit = True

        if remove_source and remove_record(sources_dir, batch_id):
            changed = True
            repaired_round = True
        if remove_offset and remove_record(offsets_dir, batch_id):
            changed = True
            repaired_round = True
        if remove_commit and remove_record(commits_dir, batch_id):
            changed = True
            repaired_round = True

    if not repaired_round:
        break

    sources = numeric_files(sources_dir)
    source_compacts = compact_files(sources_dir)
    offsets = numeric_files(offsets_dir)
    commits = numeric_files(commits_dir)
    all_ids = set(sources) | set(source_compacts) | set(offsets) | set(commits)

if changed:
    print(f"{datetime.now().isoformat()} repaired streaming checkpoint tail; backup={backup_root}", flush=True)
PY
fi

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusDataSparkStreaming

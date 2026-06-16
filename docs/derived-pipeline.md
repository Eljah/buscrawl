# Derived Pipeline

The raw ingestion job is the only critical always-on writer. It writes raw parquet first and must not be blocked by downstream processing.

Raw JSON spool files under `/home/eljah/data/buscrawl/raw-json-spool/ready` are only a delivery journal between the realtime client and Spark raw parquet writer. They are not cleaned by Spark itself.

Derived processing is split into two classes of work:

1. Critical catchup: `bin/run-derived-data-catchup.sh`
2. Optional UI cache refresh: `bin/run-derived-ui-cache.sh`

## Critical Catchup

`run-derived-data-catchup.sh` owns `/home/eljah/data/buscrawl/derived-jobs.lock` only while it advances durable state:

```text
raw parquet compaction
stop visit events
traffic-behavior parquet
dashboard stats cache
```

After `dashboard stats cache` is written, the script releases the critical lock. This is intentional: new raw data catchup must not wait for map tiles, heatmaps, or large UI detail caches.

The critical traffic-behavior step runs with `BUS_TRAFFIC_BEHAVIOR_EVENTS_ONLY=true`. It writes durable event facts and advances traffic state, but does not rebuild all overtake/rubberiness summary parquet in the critical lock.

By default the script starts `run-derived-ui-cache.sh` in the background after releasing the critical lock. Set `BUS_DERIVED_RUN_UI_CACHE_AFTER=false` to disable that.

## UI Cache Refresh

`run-derived-ui-cache.sh` owns a separate non-blocking lock:

```text
/home/eljah/data/buscrawl/derived-ui-cache.lock
```

If a previous UI cache refresh is still running, a new UI refresh exits without blocking the critical catchup path.

Default UI refresh steps:

```text
stop-last-pass daily/summary parquet
stop-last-pass cache
overtake/rubberiness summary/cache refresh work
overtake cache
rubberiness cache
```

Speed heatmap refresh is disabled by default because it can be much heavier:

```text
BUS_REFRESH_SPEED_CACHE=true bin/run-derived-ui-cache.sh
```

## Operational Rule

Backlog health should be judged by the critical chain, not by slow UI caches:

```text
raw parquet latest mtime
compacted parquet latest mtime
stop-visit parquet latest mtime
traffic-behavior parquet latest mtime
dashboard stats cache mtime
```

If UI caches lag while the critical chain is current, raw and derived facts are still being preserved and processed. If compacted/stop/traffic/dashboard lag grows, the critical catchup path is overloaded or blocked and must be fixed before UI refresh work.

## Raw Spool Cleanup

`bin/run-raw-spool-cleanup.sh` removes raw JSON spool files only when all of these conditions are true:

1. The file path is listed in Spark file source checkpoint `bus-data-checkpoint/sources/0/<batchId>`.
2. The same `<batchId>` has a successful checkpoint commit in `bus-data-checkpoint/commits/<batchId>`.
3. The file is older than `BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS`, default `6`.
4. The per-run delete cap `BUS_RAW_SPOOL_CLEANUP_MAX_FILES`, default `5000`, is not exceeded.

The cleanup job defaults to dry-run:

```text
BUS_RAW_SPOOL_CLEANUP_DRY_RUN=true bin/run-raw-spool-cleanup.sh
```

To delete committed old files:

```text
BUS_RAW_SPOOL_CLEANUP_DRY_RUN=false bin/run-raw-spool-cleanup.sh
```

Do not replace this with age-only cleanup. If raw Spark streaming falls behind, age-only cleanup can delete files before they are written to parquet.

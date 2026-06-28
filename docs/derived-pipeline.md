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

Raw parquet compaction runs in multiple bounded batches per critical catchup invocation. Defaults:

```text
BUS_DERIVED_COMPACTION_MAX_BATCHES=8
BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN=1000
BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS=8
```

This is required because one small compaction batch per timer tick can fall behind during daytime source peaks. If compacted parquet lags, all dashboard graphs based on derived facts can look stopped even while raw ingestion is healthy.

Compaction must deduplicate raw source events by the canonical provider identity:

```text
internalRouteId, realRouteNumber, plate, latitude, longitude, speed, sourceTimestamp
```

The realtime client writes append-only raw events, and reconnect bugs or provider repeats must not inflate dashboard density or downstream facts. For one-off repair of an already inflated compacted layer, use `bin/run-compacted-parquet-dedup-duckdb.sh`. It writes a deduplicated compacted parquet tree to a temporary directory and does not switch production paths automatically; validate row counts and then perform a controlled cutover.

Stop-visit calculation intentionally processes one compacted file per inner iteration by default:

```text
BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN=1
```

Compacted files can be large during catchup; processing several of them in one Spark job can turn the next stage into a long high-memory stage.

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

## Nightly Transfer Potential

Potential transfer journeys are intentionally outside the critical 5-minute catchup chain. They are derived from already persisted `traffic-behavior/segment-trips` facts.

Production transfer potential is origin-specific. The server cannot keep up with a full all-stops to all-stops grid: a city-wide calculation can take longer than the period it is supposed to cover. The production nightly path reads enabled stop clusters from:

```text
/home/eljah/apps/buscrawl/dashboard-cache/accessibility-origins-config.json
```

and writes one independent parquet tree per origin under:

```text
/home/eljah/data/buscrawl/transfer-potential-accessibility-origins/<origin-slug>
```

The config is managed from `/buscrawl/accessibility-admin`. It starts with already rendered origins from `accessibility-map-index.json` and also exposes a generated catalog of all same-name stop clusters from `RouteTopology`: stops with the same normalized name are connected into one cluster when they are within `BUS_ACCESSIBILITY_STOP_CLUSTER_RADIUS_METERS` walking distance, default `180` meters. Each item has `slug`, `label`, `stopIds`, `stopNames`, `source`, and `enabled`. Keep the enabled set to a practical 20-30 clusters unless server capacity changes.

The legacy all-to-all output tree still exists and can be used manually for diagnostics/backfill experiments:

```text
/home/eljah/data/buscrawl/transfer-potential
```

The job is scheduled by `buscrawl-transfer-potential-nightly.timer` and guarded by `bin/run-transfer-potential-nightly.sh`. Production starts once at `00:00` local city time, targets the previous service date, reads the enabled origin list, and then runs until the selected origins for that service date are finished. There is no forced `04:00` stop in production: the important boundary is start time, not killing an already consistent parquet write. The nightly wrapper delegates to `bin/run-transfer-potential-origins-nightly.sh`. For each enabled origin it sets `BUS_TRANSFER_ORIGIN_STOP_IDS`, `BUS_TRANSFER_POTENTIAL_DIR`, and `BUS_TRANSFER_POTENTIAL_STATE_FILE` to origin-specific values before running `BusTransferPotentialJob`. `BUS_TRANSFER_MAX_BUCKETS_PER_ORIGIN_RUN` defaults high enough to cover the full previous day; lower values should be used only for manual diagnostics.

The static transfer graph is intentionally topological, not time-based. It stores several route candidates per origin-destination pair, ordered by fewer rides first and then by geometric route distance along stop-to-stop route geometry. Candidates that are clearly worse than the best geometry are pruned by `BUS_TRANSFER_STATIC_ALTERNATIVE_DISTANCE_RATIO` (default `1.5`), while a lower-ride candidate is kept even when a shorter multi-transfer geometry exists. Same-name stops inside `BUS_TRANSFER_STATIC_STOP_CLUSTER_RADIUS_METERS` (default `180`) are treated as one stop cluster at write time: if the opposite-direction stop has a simpler route graph, the candidate is written for the requested stop with a short walk edge inside the cluster. The production cache is split by origin stop (`paths-max-transfers-N-by-origin/<originStopId>.bin`) so `BusTransferPotentialJob` can lazily read only the origin currently being evaluated instead of loading a city-wide graph into memory. `BUS_TRANSFER_STATIC_ORIGIN_CACHE_SIZE` controls the small LRU cache of recently read origin files, default `16`. JSONL export is disabled by default and can be enabled with `BUS_TRANSFER_STATIC_WRITE_JSONL=true` only for diagnostics. Day-specific travel time is applied later by `BusTransferPotentialJob` from actual `segment-trips`; the project does not currently maintain stable morning/day/evening average segment-speed profiles suitable for static routing.

Outputs:

```text
transfer-potential/journeys
transfer-potential/journey-fragments
transfer-potential/request-grid-counts
transfer-potential/daily-od-summary
transfer-potential/daily-od-bucket-summary
transfer-potential/summary-od-all-days
transfer-potential/summary-od-by-weekday
transfer-potential/summary-od-by-bucket
transfer-potential/summary-od-by-weekday-bucket
transfer-potential/daily-od-route-pattern-summary
transfer-potential/daily-od-bucket-route-pattern-summary
transfer-potential/summary-od-route-pattern-all-days
transfer-potential/summary-od-route-pattern-by-weekday
transfer-potential/summary-od-route-pattern-by-bucket
transfer-potential/summary-od-route-pattern-by-weekday-bucket
```

For an origin-specific run, the request grid is every selected origin stop to every other route stop for every 10-minute bucket that has observed movement on the service date. Storing one explicit unreachable row for every OD pair and time bucket would create excessive rows, so unreachable demand is represented exactly in `request-grid-counts` as `possibleRequestCount - reachableRequestCount`; detailed `journeys` and `journey-fragments` are stored for reachable shortest journeys. Most frequent transfer variants are represented by the `*route-pattern*` summaries: select the highest `sampleCount` for the requested OD scope, then use the average wait/ride/journey fields for that pattern.

The job stores progress per `serviceDate|departureBucketMinute` and writes `journeys`, `journey-fragments`, and `request-grid-counts` partitioned by `serviceDate/departureBucketMinute`. This is deliberate: a full service day is too large to treat as one all-or-nothing write. The nightly service starts in the quiet window and may finish after `04:00`; raw ingestion remains disk-first and is not allowed to depend on transfer-potential completion.

The default search mode is `BUS_TRANSFER_SEARCH_MODE=static-graph-cache`. It reads the precomputed static transfer graph from `BUS_TRANSFER_STATIC_GRAPH_DIR`, preferring per-origin binary files (`paths-max-transfers-N-by-origin/<originStopId>.bin`) and falling back to older monolithic binary/JSONL files only for compatibility. The transfer job then tests only those route/transfer candidates against the observed segment trips for each departure bucket. This keeps the expensive spatial/route branching outside the nightly factual timing calculation. The static cache must contain ordered `edges`; older cache files without `edges` are still readable, but they can only replay ride legs and cannot charge ordered walking transfer time correctly.

`BUS_TRANSFER_STATIC_GRAPH_MAX_TRANSFERS=2` selects which static cache directory is used. Here `2` means up to two transfers, or three ride legs. `BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_ROUTE_PATTERN=1` means one nearest observed vehicle per static route leg pattern. Higher values can be used for validation, but they are more expensive.

The previous `route-network` mode remains available for comparison. It indexes observed segment trips by vehicle/route/direction and builds a transfer-stop set from stops served by more than one route pattern. The old flat event scan is still available with `BUS_TRANSFER_SEARCH_MODE=legacy`; in that mode `BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_STOP=120` controls the flat per-stop event scan.

Use `bin/run-transfer-rebuild-when-idle.sh` for a full guarded rebuild. The launcher waits until heavy derived/dashboard jobs are absent and raw compaction backlog is below `BUS_TRANSFER_WAIT_RAW_BACKLOG_MAX`, rebuilds the static graph cache with ordered edges, swaps it atomically, moves the previous `transfer-potential` tree aside, and starts a low-priority `static-graph-cache` backfill. This prevents transfer backfill from competing with the critical raw-to-derived catchup path.

Use `bin/run-compacted-uniform-cutover-when-ready.sh` after a mixed-layout compacted backlog has fully caught up. It waits for low raw backlog and no heavy derived/dashboard jobs, rewrites compacted parquet into a clean `serviceDate=...` layout through the DuckDB dedup job, copies the compaction state, atomically swaps the compacted directory, and restarts derived/dashboard timers.

After this cutover, incremental compacted writes must keep the same layout. `BusRawParquetCompactionJob` writes each new batch into a temporary partitioned directory and then moves the resulting parquet files under `bus-data-parquet-compacted/serviceDate=.../`. Root-level `compact-*.parquet` files indicate an old writer or an interrupted repair and should be removed by another controlled cutover.

The main dashboard transfer graphs read the enabled origin-specific trees and sum them without showing per-origin detail. If no enabled origin trees are available yet, the dashboard falls back to the legacy all-to-all `/home/eljah/data/buscrawl/transfer-potential` tree.

This is an additive layer. Existing `segment-trips`, overtake, dwell, speed-map, stop-last-pass, and non-transfer dashboard calculations must not depend on transfer-potential outputs.

## Accessibility Map Tile Cache

Accessibility PNG overlays are intentionally a UI cache, not a factual data layer. The factual input is `transfer-potential`; tiles can be deleted and regenerated.

Current production rendering policy:

```text
BUS_ACCESSIBILITY_TILE_MIN_ZOOM=10
BUS_ACCESSIBILITY_TILE_MAX_ZOOM=15
BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM=12
BUS_ACCESSIBILITY_INDEXED_PNG=true
BUS_ACCESSIBILITY_PNG_COMPRESSION_QUALITY=0.0
BUS_ACCESSIBILITY_TOTAL_NORMALIZED_MAX_MINUTES=800
```

The base map may still zoom past `12`; the accessibility overlay reuses and stretches the `z=12` PNG tiles for higher zooms. This preserves the color signal while avoiding the explosive `z=13..15` overlay tile count.

`total time normalized` is rendered as a separate `total-normalized` tile layer with a fixed `0..800` minute scale. This layer is the correct visual basis for future cross-day/month/year raster averaging. `total time balanced` is intentionally per-snapshot and should not be averaged as PNG because the same color can mean different minute values. For exact monthly/yearly products, store or derive a numeric raster/time cache first and render PNG as an output artifact; averaging already-paletted PNG should be treated as an approximation only.

Monthly normalized aggregates are produced by `BusAccessibilityNormalizedAggregateJob`, launched by `buscrawl-accessibility-normalized-aggregate.timer` on the first calendar day of the month at `06:30` MSK. The delay is deliberate: the previous day's transfer/accessibility calculations still belong to the previous calendar month and should finish first. The job reads rendered origin JSON from `BUS_ACCESSIBILITY_AGGREGATE_ORIGIN_DIR`, uses only `totalNormalizedTileOverlayTemplate`, averages non-transparent PNG pixels by `month + weekday + 15-minute departure`, and writes aggregate tiles under:

```text
dashboard-cache/tiles/accessibility-aggregates/<origin>/month/<YYYY-MM>/weekday-<1..7>/<HHmm>/total-normalized
```

It then rebuilds year-to-date weekday aggregates for the same year by averaging the monthly aggregate PNGs:

```text
dashboard-cache/tiles/accessibility-aggregates/<origin>/year/<YYYY>/weekday-<1..7>/<HHmm>/total-normalized
```

Per-origin aggregate metadata is stored separately from daily render JSON:

```text
dashboard-cache/accessibility-map-aggregates/<origin>.json
```

`BusDashboardServer` merges this metadata into `/api/accessibility-map?origin=...` as `aggregateSnapshots`; the UI exposes the aggregate datasets only when those records exist.

Measured benchmark on `2026-06-21 08:00`, origin `Tasma` (`12078,12112`), all four overlay modes (`total,totalLog,walk,stopTransport`), transfer-potential source `/home/eljah/data/buscrawl/transfer-potential-tasma-20260621`:

```text
Cold one-snapshot job elapsed: 2m10s
In-process render+contour stages for one snapshot: ~17s
Output PNG files for one snapshot: 240
Output tile size for one snapshot: 2.8M
Max RSS: ~3.8G
```

The cold elapsed time includes JVM/Spark startup and loading the transfer reachability cache. A full-day run for one origin should amortize that startup across all 15-minute snapshots, so per-snapshot steady-state time should be judged from the stage logs rather than the cold one-snapshot wall time.

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

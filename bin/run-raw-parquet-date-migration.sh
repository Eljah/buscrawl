#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE=${BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv}
export BUS_RAW_PARQUET_MIGRATED_LEGACY_MANIFEST_BUILDING_FILE=${BUS_RAW_PARQUET_MIGRATED_LEGACY_MANIFEST_BUILDING_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv.migrated-building}
export BUS_RAW_PARQUET_DATE_MIGRATION_STATE_FILE=${BUS_RAW_PARQUET_DATE_MIGRATION_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.date-migration-state.json}
export BUS_RAW_PARQUET_DATE_MIGRATION_ZONE=${BUS_RAW_PARQUET_DATE_MIGRATION_ZONE:-Europe/Moscow}
export BUS_RAW_PARQUET_DATE_MIGRATION_MAX_RECORDS_PER_RUN=${BUS_RAW_PARQUET_DATE_MIGRATION_MAX_RECORDS_PER_RUN:-5000}
export BUS_RAW_PARQUET_DATE_MIGRATION_CHECKPOINT_EVERY=${BUS_RAW_PARQUET_DATE_MIGRATION_CHECKPOINT_EVERY:-200}
export BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_EVERY_RECORDS=${BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_EVERY_RECORDS:-200}
export BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_MILLIS=${BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_MILLIS:-50}

/usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetDatePartitionMigrationJob

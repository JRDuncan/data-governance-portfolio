#!/usr/bin/env bash
set -e

command -v metadata >/dev/null 2>&1 || {
  echo "❌ metadata CLI not found. Run this inside openmetadata_ingestion container."
  exit 1
}

export $(grep -v '^#' openmetadata/env/openmetadata.env | xargs)

metadata ingest -c openmetadata/ingestion/kafka.yml
metadata ingest -c openmetadata/ingestion/sqlserver_oltp.yml
metadata ingest -c openmetadata/ingestion/sqlserver_staging.yml
metadata ingest -c openmetadata/ingestion/dbt_lineage.yml
metadata ingest -c openmetadata/lineage/kafka_to_sql_lineage.yml

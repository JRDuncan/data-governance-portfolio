#!/usr/bin/env bash
set -e

echo "▶ Register Debezium SQL Server CDC"
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/configs/sqlserver-cdc.json

echo "▶ Register JDBC Sink (Bronze)"
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/configs/sqlserver-jdbc-sink.json

echo "✅ Kafka Connect configured"

#!/usr/bin/env bash
set -euo pipefail

echo "⏳ Waiting for OpenMetadata API..."
until curl -sf http://localhost:8585/api/v1/system/health | grep -q 'OK'; do
  echo "Still waiting for OpenMetadata..."
  sleep 5
done
echo "✅ OpenMetadata is healthy"

echo "⏳ Waiting for Kafka Connect..."
until curl -sf -o /dev/null http://localhost:8083/; do
  echo "Still waiting for Kafka Connect..."
  sleep 5
done
echo "✅ Kafka Connect is ready"

echo "⏳ Waiting for SQL Server..."

# Load the same password as docker-compose uses
SA_PASSWORD="${MSSQL_SA_PASSWORD:-$(cat secrets/sqlserver_password 2>/dev/null || echo '')}"

if [ -z "$SA_PASSWORD" ]; then
  echo "❌ Error: MSSQL_SA_PASSWORD is not set and secrets/sqlserver_password is empty or missing!"
  exit 1
fi

until docker exec sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$SA_PASSWORD" \
  -C -N \
  -Q "SELECT 1" >/dev/null 2>&1; do
  echo "Still waiting for SQL Server... (password may be incorrect)"
  sleep 5
done

echo "✅ SQL Server is accepting connections"

echo "🎉 All core services are ready"

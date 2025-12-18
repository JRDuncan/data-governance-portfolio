#!/bin/bash
set -e

echo "🚀 Starting dbt pipeline..."

# Run dbt commands inside the dbt container
docker exec -e SA_PASSWORD=${MSSQL_SA_PASSWORD} dbt bash -c "
  echo '🔹 Running dbt debug...'
  dbt debug || { echo '❌ dbt debug failed'; exit 1; }

  echo '🔹 Running dbt run...'
  dbt run

  echo '🔹 Running dbt test...'
  dbt test

  echo '✅ dbt pipeline completed successfully.'
"


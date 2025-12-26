#!/usr/bin/env bash
set -e

# Load secrets on host
OPENMETADATA_JWT=$(cat secrets/openmetadata_jwt)
SQLSERVER_PASSWORD=$(cat secrets/sqlserver_password)

# Run everything inside the container
docker exec openmetadata_ingestion bash -c "
  export OPENMETADATA_JWT='$OPENMETADATA_JWT'
  export SQLSERVER_PASSWORD='$SQLSERVER_PASSWORD'

  CONFIG_DIR=\"/opt/openmetadata/ingestion\"
  echo \"Running metadata ingestion workflows...\"
  for cfg in \"\$CONFIG_DIR\"/*.yml; do
    if [ -f \"\$cfg\" ]; then
      echo \"▶ Ingesting: \$(basename \"\$cfg\")\"
      metadata ingest -c \"\$cfg\"
    else
      echo \"No YAML files found in \$CONFIG_DIR\"
      exit 1
    fi
  done
  echo \"✅ All ingestions completed\"
"

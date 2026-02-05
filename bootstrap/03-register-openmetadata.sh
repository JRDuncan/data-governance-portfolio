#!/usr/bin/env bash
set -e

OM_JWT=$(cat secrets/openmetadata_jwt)
# Add this line near the top of ./bootstrap/03-register-openmetadata.sh
export SQLSERVER_PASSWORD=$(cat secrets/sqlserver_password)

register() {
  local TYPE=$1
  local FILE=$2

  local NAME=$(grep '^name:' "$FILE" | sed 's/^name: *//; s/"//g; s/^[ \t]*//; s/[ \t]*$//' | tr -d '\r\n')
  if [ -z "$NAME" ]; then
    echo "❌ Could not extract 'name' from $FILE"
    exit 1
  fi

  echo "▶ Checking $NAME ($TYPE)..."

  local CHECK_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $OM_JWT" \
    "http://localhost:8585/api/v1/services/${TYPE}/name/$NAME")

  if [ "$CHECK_CODE" = "200" ]; then
    echo "✅ $NAME already exists → skipping"
    return
  elif [ "$CHECK_CODE" != "404" ]; then
    echo "❌ Check failed: HTTP $CHECK_CODE"
    curl -s -H "Authorization: Bearer $OM_JWT" \
      "http://localhost:8585/api/v1/services/${TYPE}/name/$NAME"
    exit 1
  fi

  echo "Creating $NAME..."

  # Convert YAML to JSON with jq 1.7+
  # Use 'envsubst' to replace ${SQLSERVER_PASSWORD} with the actual value
  # before passing it to yq or the curl command
  local RAW_YAML=$(envsubst < "$FILE")
  local JSON_PAYLOAD=$(echo "$RAW_YAML" | yq .)
  #local JSON_PAYLOAD=$(yq . "$FILE")
  
  if [ -z "$JSON_PAYLOAD" ] || [ "$JSON_PAYLOAD" = "null" ]; then
    echo "❌ Failed to convert $FILE to JSON"
    exit 1
  fi

  echo "Payload sent: $JSON_PAYLOAD"  # Debug

  local RESPONSE=$(curl -s \
    -X POST "http://localhost:8585/api/v1/services/${TYPE}" \
    -H "Authorization: Bearer $OM_JWT" \
    -H "Content-Type: application/json" \
    -d "$JSON_PAYLOAD")

  echo "$RESPONSE" | jq . 2>/dev/null || echo "Response: $RESPONSE"
}

register messagingServices openmetadata/services/kafka.yml
register databaseServices openmetadata/services/sqlserver_oltp.yml
register databaseServices openmetadata/services/sqlserver_staging.yml

echo "Clean dbt"
docker exec dbt dbt clean 
echo "Install dbt dependencies"
docker exec dbt dbt deps
echo "Debug dbt"
docker exec dbt dbt debug
echo "Run dbt"
docker exec dbt dbt run
echo "Dbt generate docs"
docker exec dbt dbt docs generate

echo "✅ Registration complete"

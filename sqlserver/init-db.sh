#!/bin/bash
set -e

#echo "🧩 Waiting for SQL Server to start..."
#sleep 20
echo "🧩 Waiting for SQL Server to become ready..."
for i in {1..30}; do
  if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" &>/dev/null; then
    echo "✅ SQL Server is ready!"
    break
  else
    echo "⏳ Still waiting... ($i/30)"
    sleep 5
  fi
done


echo "🔍 Checking for backup files..."
if [ ! -f /var/opt/mssql/backup/AdventureWorks2022.bak ]; then
  echo "❌ Backup file not found in mounted volume. Expected AdventureWorks2022.bak"
  exit 1
fi

echo "⚙️  Restoring AdventureWorks2022..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -Q "
  RESTORE DATABASE AdventureWorks2022
  FROM DISK = '/var/opt/mssql/backup/AdventureWorks2022.bak'
  WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022.mdf',
       MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022.ldf',
       REPLACE;
"

echo "⚙️  Restoring AdventureWorksDW2022..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -Q "
  RESTORE DATABASE AdventureWorksDW2022
  FROM DISK = '/var/opt/mssql/backup/AdventureWorksDW2022.bak'
  WITH MOVE 'AdventureWorksDW2022' TO '/var/opt/mssql/data/AdventureWorksDW2022.mdf',
       MOVE 'AdventureWorksDW2022_log' TO '/var/opt/mssql/data/AdventureWorksDW2022.ldf',
       REPLACE;
"

# ... (at the bottom of your existing script)
echo "✅ Both databases restored successfully!"


/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C <<'EOF'

-- Create staging database
IF DB_ID('AdventureWorks_Staging') IS NULL
CREATE DATABASE AdventureWorks_Staging;
GO

USE AdventureWorks_Staging;
GO

-- Schema represents a governed landing zone
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

USE AdventureWorks_Staging;
GO
CREATE LOGIN cdc_sink_user WITH PASSWORD = 'SinkStrongPassword!';
CREATE USER cdc_sink_user FOR LOGIN cdc_sink_user;
ALTER ROLE db_datareader ADD MEMBER cdc_sink_user;
ALTER ROLE db_datawriter ADD MEMBER cdc_sink_user;
GO

USE Adventureworks2022
GO

-- Enable CDC at DB level
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on Sales tables
EXEC sys.sp_cdc_enable_table
  @source_schema = 'Sales',
  @source_name = 'SalesOrderHeader',
  @role_name = 'cdc_reader';
GO

EXEC sys.sp_cdc_enable_table
  @source_schema = 'Sales',
  @source_name = 'SalesOrderDetail',
  @role_name = 'cdc_reader';
GO

-- Debezium service account
CREATE LOGIN debezium_user WITH PASSWORD = 'DebeziumStrong!';
CREATE USER debezium_user FOR LOGIN debezium_user;
EXEC sp_addrolemember 'db_datareader', 'debezium_user';
GO

-- dbt service account
CREATE LOGIN dbt_user WITH PASSWORD = 'DbtStrong!', CHECK_POLICY = OFF;
CREATE USER dbt_user FOR LOGIN dbt_user;
EXEC sp_addrolemember 'db_datareader', 'dbt_user';
GO

USE Adventureworks_Staging;
CREATE USER dbt_user FOR LOGIN dbt_user;
EXEC sp_addrolemember 'db_owner', 'dbt_user';
GO

EOF

echo "SQL Server CDC and staging initialized."

touch /tmp/init_finished

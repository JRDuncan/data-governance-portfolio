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

echo "⚙️  Create AdventureWorks_Staging..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C <<'EOF'

-- =============================================
-- Create staging database + user (in one batch)
-- =============================================
IF DB_ID('AdventureWorks_Staging') IS NULL
    CREATE DATABASE AdventureWorks_Staging;
GO
USE AdventureWorks_Staging;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO
-- Create login/user/role membership (login is server-level, so only once)
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'cdc_sink_user')
    CREATE LOGIN cdc_sink_user WITH PASSWORD = 'SinkStrongPassword!';
GO
USE AdventureWorks_Staging;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'cdc_sink_user')
BEGIN
    CREATE USER cdc_sink_user FOR LOGIN cdc_sink_user;
    ALTER ROLE db_datareader ADD MEMBER cdc_sink_user;
    ALTER ROLE db_datawriter ADD MEMBER cdc_sink_user;
END
GO
EOF

echo "⚙️  Enable CDC..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C <<'EOF'
USE AdventureWorks2022;
GO

-- 1. Enable CDC at DB level (idempotent)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled at database level.';
END
ELSE
    PRINT 'CDC already enabled at database level.';
GO

-- 2. Create gating role if missing
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'cdc_reader' AND type = 'R')
BEGIN
    CREATE ROLE cdc_reader;
    PRINT 'Created cdc_reader role.';
END
GO

-- 3. Table list + enable loop
DECLARE @tables TABLE (SchemaName sysname NOT NULL, TableName sysname NOT NULL);
INSERT INTO @tables (SchemaName, TableName)
VALUES
    ('Person', 'BusinessEntity'),
    ('Person', 'Person'),
    ('Person', 'Address'),
    ('Person', 'BusinessEntityAddress'),
    ('Person', 'EmailAddress'),
    ('Person', 'PersonPhone'),
    ('Sales', 'Customer'),
    ('Sales', 'SalesOrderHeader'),
    ('Sales', 'SalesOrderDetail')
    -- Add ('Production', 'ProductInventory') if you need inventory changes captured
;

DECLARE 
    @SchemaName sysname,
    @TableName sysname,
    @sql nvarchar(max),
    @msg nvarchar(4000);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR 
    SELECT SchemaName, TableName FROM @tables;

OPEN cur;
FETCH NEXT FROM cur INTO @SchemaName, @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (
        SELECT 1 FROM cdc.change_tables 
        WHERE source_object_id = OBJECT_ID(QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName))
    )
    BEGIN
        PRINT CONCAT('CDC already enabled on ', @SchemaName, '.', @TableName);
    END
    ELSE
    BEGIN
        BEGIN TRY
            SET @sql = N'
EXEC sys.sp_cdc_enable_table
    @source_schema         = @s,
    @source_name           = @t,
    @role_name             = ''cdc_reader'',
    @supports_net_changes  = 1,
    @captured_column_list  = NULL;';  -- NULL = capture all columns

            EXEC sp_executesql 
                @sql, 
                N'@s sysname, @t sysname',
                @s = @SchemaName, 
                @t = @TableName;

            PRINT CONCAT('CDC ENABLED successfully on ', @SchemaName, '.', @TableName);
        END TRY
        BEGIN CATCH
            SET @msg = CONCAT('FAILED to enable CDC on ', @SchemaName, '.', @TableName, ': ', ERROR_MESSAGE());
            PRINT @msg;
            RAISERROR(@msg, 16, 1);  -- Fail the batch if any error
        END CATCH
    END

    FETCH NEXT FROM cur INTO @SchemaName, @TableName;
END

CLOSE cur;
DEALLOCATE cur;
GO

-- 4. NOW grant SELECT on cdc schema (after tables enabled → schema exists)
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cdc')
BEGIN
    GRANT SELECT ON SCHEMA::cdc TO cdc_reader;
    PRINT 'Granted SELECT on cdc schema to cdc_reader role.';
END
ELSE
    PRINT 'cdc schema still missing – check enable failures above.';
GO

-- 5. Verification: List enabled capture instances
SELECT 
    s.name AS source_schema,
    t.name AS source_table,
    ct.capture_instance,
    ct.start_lsn,
    ct.end_lsn,
    ct.supports_net_changes
FROM cdc.change_tables ct
INNER JOIN sys.tables t ON ct.source_object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('Person', 'Sales')
ORDER BY s.name, t.name;
GO

PRINT 'CDC setup complete. If no rows above, check for errors in the loop.';
GO

EOF

echo "SQL Server CDC and staging initialized."
touch /tmp/init_finished

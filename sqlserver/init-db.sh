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
touch /tmp/init_finished

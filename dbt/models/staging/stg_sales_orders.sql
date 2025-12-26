select
    cast(SalesOrderID as int) as order_id,
    cast(TotalDue as decimal(18,2)) as total_due,

    -- SQL Server–native SHA-256 hashing
    convert(
        varchar(64),
        hashbytes(
            'SHA2_256',
            cast(CreditCardID as varchar(255))
        ),
        2
    ) as creditcard_hash
from {{ source(
  'adventureworks_bronze',
  'bronze_aw_AdventureWorks2022_Sales_SalesOrderHeader'
) }}

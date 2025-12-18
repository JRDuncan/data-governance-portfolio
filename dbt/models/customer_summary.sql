-- dbt/models/customer_summary.sql
SELECT
    Gender,
    COUNT(*) AS CustomerCount
FROM AdventureWorksDW2022.dbo.DimCustomer
GROUP BY Gender;


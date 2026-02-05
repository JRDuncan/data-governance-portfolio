#!/usr/bin/env python3
"""
OLTP data generator:
- Creates dg_customers and dg_orders tables (if not exists) in the target DB
- Inserts new customers and orders at random intervals
- Injects configurable fraction of 'bad' rows
"""
import pyodbc
import uuid
import os
import time
import random
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from faker import Faker
from dotenv import load_dotenv

load_dotenv()  # allows local testing by reading .env if present

# Logging setup
logging.basicConfig(level=os.getenv("GEN_LOG_LEVEL", "INFO"))
log = logging.getLogger("oltp-generator")

# Generator config
BAD_DATA_FRACTION = float(os.getenv("BAD_DATA_FRACTION", "0.15"))  # 15% of rows will be 'bad' by default
MIN_INTERVAL = float(os.getenv("GEN_MIN_INTERVAL", "300.0"))        # seconds
MAX_INTERVAL = float(os.getenv("GEN_MAX_INTERVAL", "1500.0"))       # seconds
BATCH_SIZE = int(os.getenv("GEN_BATCH_SIZE", "1"))                # number of events per cycle
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in ("1", "true", "yes")

# Initialize Faker
fake = Faker('en_US')

# DB config from env (defaults to your compose variables)
DB_HOST = os.getenv("DB_HOST", os.getenv("DBT_SERVER", "sqlserver"))
DB_PORT = int(os.getenv("DB_PORT", os.getenv("MSSQL_PORT", "1433")))
DB_USER = os.getenv("DB_USER", os.getenv("DBT_USER"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("MSSQL_SA_PASSWORD"))
DB_NAME = os.getenv("DB_NAME", os.getenv("MSSQL_DATABASE_OLTP", "AdventureWorks2022"))

# Build the connection string dynamically
conn_str = (
    r'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={DB_HOST},{DB_PORT};'
    f'DATABASE={DB_NAME};'
    f'UID={DB_USER};'
    f'PWD={DB_PASSWORD};'
    r'Encrypt=yes;'
    r'TrustServerCertificate=yes;'
)
print('conn_str: ', conn_str)
log.debug("Connection string (password hidden): %s",
          conn_str.replace(f'PWD={DB_PASSWORD}', 'PWD=********'))

conn = pyodbc.connect(conn_str)
conn.autocommit = False   # already default, just being explicit
cursor = conn.cursor()
cursor.fast_executemany = True  # optional performance

# Helper functions
def get_existing_customer_ids():
    """Fetch list of existing CustomerIDs."""
    cursor.execute("SELECT CustomerID FROM Sales.Customer")
    return [row[0] for row in cursor.fetchall()]

def get_random_product_info():
    """Fetch random sellable ProductID, UnitPrice, and StandardCost that support SpecialOfferID=1."""
    cursor.execute("""
        SELECT TOP 1 p.ProductID, p.ListPrice, p.StandardCost
        FROM Production.Product p
        INNER JOIN Sales.SpecialOfferProduct sop ON p.ProductID = sop.ProductID
        WHERE sop.SpecialOfferID = 1
          AND (p.SellEndDate IS NULL OR p.SellEndDate > GETDATE())
        ORDER BY NEWID()
    """)
    row = cursor.fetchone()
    if row is None:
        raise ValueError("No products found for SpecialOfferID = 1")
    return row

def get_random_territory_id():
    """Fetch random TerritoryID."""
    cursor.execute("SELECT TOP 1 TerritoryID FROM Sales.SalesTerritory ORDER BY NEWID()")
    return cursor.fetchone()[0]

def get_random_ship_method_id():
    """Fetch random ShipMethodID."""
    cursor.execute("SELECT TOP 1 ShipMethodID FROM Purchasing.ShipMethod ORDER BY NEWID()")
    return cursor.fetchone()[0]

def get_state_province_id(city):
    """Get a random StateProvinceID."""
    cursor.execute("SELECT TOP 1 StateProvinceID FROM Person.StateProvince ORDER BY NEWID()")
    return cursor.fetchone()[0]

def create_new_customer():
    """Create a new individual customer and populate all relevant tables. Returns CustomerID."""
    try:
        cursor.execute("BEGIN TRANSACTION")
        log.debug("Starting new customer creation transaction")

        # 1. Insert into Person.BusinessEntity
        business_entity_rowguid = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO Person.BusinessEntity (rowguid, ModifiedDate) 
            OUTPUT inserted.BusinessEntityID
            VALUES (?, GETDATE());
        """, business_entity_rowguid)
        business_entity_id = cursor.fetchone()[0]
        log.debug("Created BusinessEntityID: %d", business_entity_id)
        log.debug("Rows affected after Person.BusinessEntity: %d", cursor.rowcount)

        # 2. Insert into Person.Person
        person_rowguid = str(uuid.uuid4())
        first_name = fake.first_name()
        last_name = fake.last_name()
        person_type = 'IN'
        email_promotion = random.randint(0, 2)
        cursor.execute("""
            INSERT INTO Person.Person (BusinessEntityID, PersonType, FirstName, LastName, EmailPromotion, rowguid, ModifiedDate)
            VALUES (?, ?, ?, ?, ?, ?, GETDATE());
        """, business_entity_id, person_type, first_name, last_name, email_promotion, person_rowguid)
        log.debug("Rows affected after Person.Person: %d", cursor.rowcount)

        # 3. Insert into Person.Address
        address_line1 = fake.street_address()
        city = fake.city()
        postal_code = fake.zipcode()
        state_province_id = get_state_province_id(city)
        address_rowguid = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO Person.Address (AddressLine1, City, StateProvinceID, PostalCode, rowguid, ModifiedDate) 
            OUTPUT inserted.AddressID
            VALUES (?, ?, ?, ?, ?, GETDATE());
        """, address_line1, city, state_province_id, postal_code, address_rowguid)
        address_id = cursor.fetchone()[0]
        log.debug("Rows affected after Person.Address: %d", cursor.rowcount)

        # 4. Insert into Person.BusinessEntityAddress (AddressTypeID 2 = Home)
        bea_rowguid = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO Person.BusinessEntityAddress (BusinessEntityID, AddressID, AddressTypeID, rowguid, ModifiedDate)
            VALUES (?, ?, 2, ?, GETDATE());
        """, business_entity_id, address_id, bea_rowguid)
        log.debug("Rows affected after Person.BusinessEntityAddress: %d", cursor.rowcount)

        # Optional: Insert email
        email = fake.email()
        cursor.execute("""
            INSERT INTO Person.EmailAddress (BusinessEntityID, EmailAddress, rowguid, ModifiedDate)
            VALUES (?, ?, ?, GETDATE());
        """, business_entity_id, email, str(uuid.uuid4()))
        log.debug("Rows affected after Person.EmailAddress: %d", cursor.rowcount)

        # Optional: Insert phone
        phone = fake.phone_number()
        phone_number_type_id = 1  # Cell
        cursor.execute("""
            INSERT INTO Person.PersonPhone (BusinessEntityID, PhoneNumber, PhoneNumberTypeID, ModifiedDate)
            VALUES (?, ?, ?, GETDATE());
        """, business_entity_id, phone, phone_number_type_id)
        log.debug("Rows affected after Person.PersonPhone: %d", cursor.rowcount)

        # 5. Insert into Sales.Customer
        territory_id = get_random_territory_id()
        customer_rowguid = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO Sales.Customer (PersonID, TerritoryID, rowguid, ModifiedDate) 
            OUTPUT inserted.CustomerID
            VALUES (?, ?, ?, GETDATE());
        """, business_entity_id, territory_id, customer_rowguid)
        customer_id = cursor.fetchone()[0]
        log.debug("Rows affected after Sales.Customer: %d", cursor.rowcount)

        cursor.execute("COMMIT")
        conn.commit()               # ← add this line (extra safety)
        log.info("Created new customer: %d - %s %s (committed)", customer_id, first_name, last_name)

        return customer_id, address_id

    except Exception as e:
        cursor.execute("ROLLBACK")
        log.error("Failed to create new customer: %s | Exception type: %s | Full traceback: %s",
              str(e), type(e).__name__, traceback.format_exc())
        raise

def create_transaction(customer_id, address_id):
    """Create a new transaction (sales order) for the given customer."""
    try:
        cursor.execute("BEGIN TRANSACTION")
        log.debug("Starting transaction for CustomerID: %d", customer_id)

        line_items = []
        sub_total = Decimal('0.00')
        for _ in range(random.randint(1, 3)):
            product_id, unit_price, standard_cost = get_random_product_info()
            order_qty = random.randint(1, 5)
            discount = Decimal('0')  # Set discount to Zero for now. Do not complicate as it is based on Sales.SpecialOffer
            line_total = Decimal(order_qty) * unit_price * (Decimal('1') - discount)
            sub_total += line_total
            special_offer_id = 1
            line_items.append((order_qty, product_id, special_offer_id, unit_price, discount, line_total, standard_cost))

        tax_amt = sub_total * Decimal('0.08')
        freight = sub_total * Decimal('0.025')
        total_due = sub_total + tax_amt + freight

        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        due_date = order_date + timedelta(days=7)
        ship_date = order_date + timedelta(days=random.randint(1, 5)) if random.random() > 0.2 else None
        status = 1
        online_order_flag = random.choice([0, 1])
        ship_method_id = get_random_ship_method_id()
        territory_id = get_random_territory_id()
        header_rowguid = str(uuid.uuid4())

        cursor.execute("""
            INSERT INTO Sales.SalesOrderHeader
            (RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag,
             CustomerID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID,
             SubTotal, TaxAmt, Freight, rowguid, ModifiedDate) 
            OUTPUT inserted.SalesOrderID
            VALUES (8, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
        """,
            order_date, due_date, ship_date, status, online_order_flag,
            customer_id, territory_id, address_id, address_id, ship_method_id,
            float(sub_total), float(tax_amt), float(freight), header_rowguid)

        sales_order_id = cursor.fetchone()[0]
        log.debug("Generated SalesOrderID: %d", sales_order_id)
        log.debug("Rows affected after Sales.SalesOrderHeader: %d", cursor.rowcount)

        # Fetch SalesOrderNumber for logging
        cursor.execute("SELECT SalesOrderNumber FROM Sales.SalesOrderHeader WHERE SalesOrderID = ?", sales_order_id)
        so_number = cursor.fetchone()[0]

        for order_qty, product_id, special_offer_id, unit_price, discount, line_total, standard_cost in line_items:
            detail_rowguid = str(uuid.uuid4())
            log.debug("Inserting SalesOrderDetail for OrderQty=%d, ProductID=%d", order_qty, product_id)
            log.debug("Detail parameters: %s", [sales_order_id, special_offer_id, order_qty, product_id,
                                                float(unit_price), float(discount), detail_rowguid])

            cursor.execute("""
                INSERT INTO Sales.SalesOrderDetail
                (SalesOrderID, SpecialOfferID, OrderQty, ProductID, UnitPrice, UnitPriceDiscount, rowguid, ModifiedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE());
            """, sales_order_id, special_offer_id, order_qty, product_id,
                 float(unit_price), float(discount), detail_rowguid)
            log.debug("Rows affected after Sales.SalesOrderDetail: %d", cursor.rowcount)

            # Inventory update with error handling
            try:
                cursor.execute("""
                    UPDATE Production.ProductInventory
                    SET Quantity = Quantity - ?
                    WHERE ProductID = ? AND LocationID = 6;
                """, order_qty, product_id)

                if cursor.rowcount == 0:
                    log.warning("No inventory record found for ProductID=%d at LocationID=6", product_id)

            except pyodbc.Error as e:
                log.warning("Inventory update failed for ProductID=%d (Qty %d): %s - continuing anyway",
                            product_id, order_qty, str(e))

        cursor.execute("COMMIT")
        log.info("Created transaction: %s (ID %d) for CustomerID %d", so_number, sales_order_id, customer_id)

    except Exception as e:
        cursor.execute("ROLLBACK")
        log.error("Transaction failed for CustomerID %d: %s", customer_id, str(e))
        raise

def populate_inventory_at_location(location_id=6):
    try:
        cursor.execute("BEGIN TRANSACTION")
        log.debug("Starting new customer creation transaction")

        # 1. Insert into Person.BusinessEntity
        business_entity_rowguid = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO Production.ProductInventory 
            (ProductID, LocationID, Shelf, Bin, Quantity)
            SELECT 
                p.ProductID,
                6,
               'A',
                0,
               500 + ABS(CHECKSUM(NEWID())) % 500   -- random 500–1000 units
            FROM Production.Product p
            WHERE p.FinishedGoodsFlag = 1           -- only sellable products
                AND NOT EXISTS (
                SELECT 1 FROM Production.ProductInventory i 
                WHERE i.ProductID = p.ProductID AND i.LocationID = ?
            );
        """, location_id)
        log.debug("Update Inventory for location_id : %d", location_id)
        log.debug("Rows affected after Production.ProductInventory: %d", cursor.rowcount)
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        log.error("Transaction failed for  location_id %d: %s", location_id, str(e))
        raise

# Main script
def main(num_transactions=5, new_customer_prob=0.3):
    cursor.execute("SELECT @@SPID, DB_NAME(), SYSTEM_USER, USER_NAME()")
    spid, db, sysuser, dbuser = cursor.fetchone()
    log.info("Session info: SPID=%s, DB=%s, SystemUser=%s, DbUser=%s", spid, db, sysuser, dbuser)
    log.info("Starting batch of %d transactions (new customer prob: %.2f)", num_transactions, new_customer_prob)

    populate_inventory_at_location(6)

    existing_customers = get_existing_customer_ids()
    if not existing_customers:
        log.error("No existing customers found in database")
        raise ValueError("No existing customers found. Create some first.")

    processed = 0
    while processed < num_transactions:
        if random.random() < new_customer_prob:
            try:
                customer_id, address_id = create_new_customer()
            except Exception as e:
                log.error("Failed to create new customer in batch: %s", str(e))
                continue
        else:
            customer_id = random.choice(existing_customers)
            cursor.execute("""
                SELECT TOP 1 AddressID FROM Person.BusinessEntityAddress
                WHERE BusinessEntityID = (SELECT PersonID FROM Sales.Customer WHERE CustomerID = ?)
                ORDER BY NEWID()
            """, customer_id)
            row = cursor.fetchone()
            if row:
                address_id = row[0]
            else:
                log.warning("Skipping transaction for CustomerID %d: No address found", customer_id)
                continue
        try:
            create_transaction(customer_id, address_id)
            processed += 1
        except Exception as e:
            log.error("Failed to create transaction for CustomerID %d: %s", customer_id, str(e))
            # Optional: continue or break depending on policy

if __name__ == "__main__":
    if RUN_ONCE:
        main(num_transactions=20, new_customer_prob=0.3)
    else:
        while True:
            try:
                main(num_transactions=5, new_customer_prob=0.3)
            except Exception as e:
                log.error("Batch failed: %s - continuing after delay", str(e))
            time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))

    conn.close()
    log.info("Generator shutting down")

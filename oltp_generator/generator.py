#!/usr/bin/env python3
"""
OLTP data generator:
- Creates dg_customers and dg_orders tables (if not exists) in the target DB
- Inserts new customers and orders at random intervals
- Injects configurable fraction of 'bad' rows
"""

import os
import time
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
import pymssql
from dotenv import load_dotenv

load_dotenv()  # allows local testing by reading .env if present

# Logging
logging.basicConfig(level=os.getenv("GEN_LOG_LEVEL", "INFO"))
log = logging.getLogger("oltp-generator")

# DB config from env (defaults to your compose variables)
DB_HOST = os.getenv("DB_HOST", os.getenv("DBT_SERVER", "sqlserver"))
DB_PORT = int(os.getenv("DB_PORT", os.getenv("MSSQL_PORT", "1433")))
DB_USER = os.getenv("DB_USER", os.getenv("DBT_USER", "sa"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd"))
DB_NAME = os.getenv("DB_NAME", os.getenv("MSSQL_DATABASE_OLTP", "AdventureWorks2022"))

# generator config
BAD_DATA_FRACTION = float(os.getenv("BAD_DATA_FRACTION", "0.15"))  # 15% of rows will be 'bad' by default
MIN_INTERVAL = float(os.getenv("GEN_MIN_INTERVAL", "1.0"))   # seconds
MAX_INTERVAL = float(os.getenv("GEN_MAX_INTERVAL", "10.0"))  # seconds
BATCH_SIZE = int(os.getenv("GEN_BATCH_SIZE", "1"))           # number of events per cycle
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in ("1", "true", "yes")

fake = Faker()

def get_conn():
    # pymssql uses host, user, password, database, port
    return pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, timeout=5)

def ensure_schema():
    sql_create_customers = """
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dg')
        EXEC('CREATE SCHEMA dg');
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dg.dg_customers') AND type in (N'U'))
        CREATE TABLE dg.dg_customers (
            id INT IDENTITY(1,1) PRIMARY KEY,
            first_name NVARCHAR(100),
            last_name NVARCHAR(100),
            email NVARCHAR(255),
            phone NVARCHAR(50),
            created_at DATETIME2 DEFAULT SYSUTCDATETIME()
        );
    """
    sql_create_orders = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dg.dg_orders') AND type in (N'U'))
        CREATE TABLE dg.dg_orders (
            id INT IDENTITY(1,1) PRIMARY KEY,
            customer_id INT,
            order_total DECIMAL(18,2),
            order_date DATETIME2,
            status NVARCHAR(50),
            note NVARCHAR(400),
            created_at DATETIME2 DEFAULT SYSUTCDATETIME()
        );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_create_customers)
            cur.execute(sql_create_orders)
            conn.commit()
    log.info("Ensured dg schema and tables exist")

def insert_customer(conn, payload):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dg.dg_customers (first_name, last_name, email, phone, created_at)
            VALUES (%s,%s,%s,%s,%s);
            SELECT SCOPE_IDENTITY();
        """, (payload['first_name'], payload['last_name'], payload['email'], payload['phone'], payload['created_at']))
        # fetch inserted id
        new_id = None
        try:
            row = cur.fetchone()
            if row:
                new_id = int(row[0])
        except Exception:
            # fallback - do a quick select of top 1 by created_at (not ideal but keeps flow)
            cur.execute("SELECT TOP 1 id FROM dg.dg_customers ORDER BY id DESC")
            row = cur.fetchone()
            new_id = row[0] if row else None
    conn.commit()
    return new_id

def insert_order(conn, payload):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dg.dg_orders (customer_id, order_total, order_date, status, note, created_at)
            VALUES (%s,%s,%s,%s,%s,%s);
        """, (payload['customer_id'], payload['order_total'], payload['order_date'], payload['status'], payload['note'], payload['created_at']))
    conn.commit()

def make_customer(bad=False):
    first = fake.first_name()
    last = fake.last_name()
    email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"
    phone = fake.phone_number()
    created_at = datetime.utcnow()
    # inject bad variants
    if bad:
        mode = random.choice(["missing_email", "invalid_email", "duplicate", "null_name", "weird_phone"])
        if mode == "missing_email":
            email = None
        elif mode == "invalid_email":
            email = "not-an-email"
        elif mode == "duplicate":
            # will be handled by returning an existing customer's data if one exists
            pass
        elif mode == "null_name":
            first = None
        elif mode == "weird_phone":
            phone = "000000000000000000000000"
    return {"first_name": first, "last_name": last, "email": email, "phone": phone, "created_at": created_at}

def make_order(customer_id, bad=False):
    order_total = round(random.uniform(5.0, 5000.0), 2)
    order_date = datetime.utcnow() - timedelta(days=random.randint(0, 30))
    status = random.choice(["NEW", "PROCESSING", "COMPLETE", "CANCELLED"])
    note = fake.sentence(nb_words=8)
    created_at = datetime.utcnow()
    if bad:
        mode = random.choice(["negative_total", "future_date", "null_customer", "huge_total", "invalid_status"])
        if mode == "negative_total":
            order_total = -abs(order_total)
        elif mode == "future_date":
            order_date = datetime.utcnow() + timedelta(days=random.randint(1, 365))
        elif mode == "null_customer":
            customer_id = None
        elif mode == "huge_total":
            order_total = 10**8
        elif mode == "invalid_status":
            status = "???"
    return {"customer_id": customer_id, "order_total": order_total, "order_date": order_date, "status": status, "note": note, "created_at": created_at}

def pick_existing_customer(conn):
    with conn.cursor(as_dict=True) as cur:
        cur.execute("SELECT TOP 1 id, email FROM dg.dg_customers ORDER BY NEWID()")
        row = cur.fetchone()
        return row['id'] if row else None

def main_loop():
    ensure_schema()
    conn = None
    try:
        conn = get_conn()
    except Exception as e:
        log.exception("Unable to connect to DB on startup: %s", e)
        raise

    log.info("Connected to %s:%s / DB=%s as %s", DB_HOST, DB_PORT, DB_NAME, DB_USER)

    cycle = 0
    while True:
        cycle += 1
        events = []
        for _ in range(BATCH_SIZE):
            do_bad = random.random() < BAD_DATA_FRACTION
            # randomly choose to create customer+order pair or only order for existing customer
            if random.random() < 0.6:
                # create a customer
                cust = make_customer(bad=do_bad)
                # duplicate scenario: sometimes reuse existing customer
                if not cust['email'] and random.random() < 0.2:
                    # intentionally missing email on new record
                    pass
                # if duplicate mode chosen earlier, try to copy an existing one
                if do_bad and random.random() < 0.1:
                    existing = pick_existing_customer(conn)
                    if existing:
                        # create an order for existing customer instead
                        order = make_order(existing, bad=True)
                        events.append(('order', order))
                        continue
                events.append(('customer', cust))
                # create an order for the customer after insert (in same cycle)
                events.append(('order_for_new_customer', {}))  # placeholder
            else:
                # create order for an existing customer (or null)
                existing = pick_existing_customer(conn)
                if existing:
                    order = make_order(existing, bad=do_bad)
                else:
                    order = make_order(None, bad=True)
                events.append(('order', order))

        # execute events
        last_customer_id = None
        for evtype, payload in events:
            try:
                if evtype == 'customer':
                    # handle duplicate by sometimes inserting same data twice
                    if payload.get('email') is None and random.random() < 0.05:
                        # intentionally insert with null email
                        pass
                    cid = insert_customer(conn, payload)
                    last_customer_id = cid
                    log.info("Inserted customer id=%s bad=%s email=%s", cid, (payload.get('email') is None or payload.get('first_name') is None), payload.get('email'))
                elif evtype == 'order_for_new_customer':
                    if last_customer_id:
                        order_payload = make_order(last_customer_id, bad=random.random() < BAD_DATA_FRACTION)
                        insert_order(conn, order_payload)
                        log.info("Inserted order for new customer id=%s total=%s bad=%s", last_customer_id, order_payload['order_total'], False)
                elif evtype == 'order':
                    insert_order(conn, payload)
                    log.info("Inserted order for customer=%s total=%s bad=%s", payload.get('customer_id'), payload.get('order_total'), (payload.get('order_total') is None or payload.get('order_total')<0))
            except Exception as e:
                log.exception("Error inserting event (%s): %s", evtype, e)

        # Exit if RUN_ONCE
        if RUN_ONCE:
            log.info("RUN_ONCE set: exiting after one cycle.")
            break

        # Sleep a random interval
        interval = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        log.debug("Sleeping %.2fs before next batch", interval)
        time.sleep(interval)

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        log.info("Interrupted, exiting")
    except Exception:
        log.exception("Unhandled error in generator")
        raise


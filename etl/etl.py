#!/usr/bin/env python
# --------------------------------------------------------------
# etl.py – Load, clean, transform, and populate the Oracle DW
# --------------------------------------------------------------

import logging
import os
from pathlib import Path

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import Integer, Numeric, DateTime, Date, String

from hybrid_settings import settings

# ----------------------------------------------------------------------
# 0️⃣ Logging
# ----------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
print(settings.DB_USER)
print(settings.DB_PASSWORD)

# ----------------------------------------------------------------------
# 1️⃣ Build the SQLAlchemy engine for Oracle
# ----------------------------------------------------------------------
def _oracle_url() -> str:
    """
    Build a URL suitable for SQLAlchemy's Oracle dialect.

    We use the pure-python driver ``oracledb`` (installed via pip).
    The format is:
        oracle+oracledb://user:pwd@host:port/?service_name=XE
    """
    return (
        f"oracle+oracledb://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}"
        f"/?service_name={settings.DB_SERVICE}"
    )

engine = create_engine(_oracle_url(), future=True, echo=False)

# ----------------------------------------------------------------------
# 2️⃣ Helper to read CSVs (paths defined in config.ini)
# ----------------------------------------------------------------------
def _read_csv(name: str) -> pd.DataFrame:
    path = settings.DATA_PATH / f"{name}.csv"
    log.info(f"Reading {path}")
    return pd.read_csv(path)

raw_customers = _read_csv("customers")
raw_products  = _read_csv("products")
raw_stores    = _read_csv("stores")
raw_sales     = _read_csv("sales")

# ----------------------------------------------------------------------
# 3️⃣ Cleaning / normalisation
# ----------------------------------------------------------------------
def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df["first_name"] = df["first_name"].str.strip()
    df["last_name"]  = df["last_name"].str.strip()
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")
    df = df[(df["age"] >= 18) & (df["age"] <= 100)]
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["cost"]  = pd.to_numeric(df["cost"], errors="coerce")
    df = df[df["cost"] < df["price"]]

    # 🔑 avoid Oracle reserved keyword
    if "size" in df.columns:
        df = df.rename(columns={"size": "size_"})

    return df

def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    df["store_name"] = df["store_name"].str.strip()
    return df

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    df["sales_date"] = pd.to_datetime(df["sales_date"], dayfirst=True,
                                      errors="coerce")
    df = df.dropna(subset=["sales_date"])

    int_cols = ["quantity", "customer_id", "product_id", "store_id"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df[df["quantity"] > 0]

    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
    df["unit_price"]   = pd.to_numeric(df["unit_price"], errors="coerce")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    return df

cust   = clean_customers(raw_customers)
prod   = clean_products(raw_products)
store  = clean_stores(raw_stores)
sales  = clean_sales(raw_sales)

# ----------------------------------------------------------------------
# 4️⃣ Build dim_date (derived from sales.sales_date)
# ----------------------------------------------------------------------
def build_dim_date(sales_df: pd.DataFrame) -> pd.DataFrame:
    uniq = sales_df["sales_date"].dt.normalize().drop_duplicates()
    dim = pd.DataFrame({
        "date_key": uniq.dt.strftime("%Y%m%d").astype(int),
        "calendar_date": uniq,
        "day": uniq.dt.day,
        "month": uniq.dt.month,
        "year": uniq.dt.year,
        "quarter": uniq.dt.quarter,
        "weekday": uniq.dt.weekday + 1   # 1 = Monday
    })
    return dim

dim_date = build_dim_date(sales)

# ----------------------------------------------------------------------
# 5️⃣ Write staging CSVs (optional – handy for manual inspection)
# ----------------------------------------------------------------------
def _write_staging(df: pd.DataFrame, name: str):
    out_path = settings.STAGING_PATH / f"stg_{name}.csv"
    df.to_csv(out_path, index=False)
    log.info(f"Wrote staging file {out_path}")

os.makedirs(settings.STAGING_PATH, exist_ok=True)
_write_staging(cust, "customer")
_write_staging(prod, "product")
_write_staging(store, "store")
_write_staging(dim_date, "date")
_write_staging(sales, "sales")   # still contains raw FK columns

# ----------------------------------------------------------------------
# 6️⃣ Helper: bulk-load a DataFrame into Oracle using `to_sql`
#     (for dimensions that rely on a sequence to fill surrogate key)
# ----------------------------------------------------------------------
def bulk_load(df: pd.DataFrame, table_name: str, pk_seq: str | None = None):
    """
    Bulk load a DataFrame into Oracle using pandas.to_sql with SQLAlchemy.
    Oracle does not support multi-row INSERTs, so we omit method="multi".

    For sequence-driven surrogate keys, we insert with the key column NULL
    (or absent) and then backfill using the supplied sequence.
    """
    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=settings.BATCH_SIZE,   # batching still works
            # method="multi",  # ❌ keep disabled for Oracle
        )
        log.info(f"Inserted {len(df):,} rows into {table_name}")

    # Backfill surrogate key from sequence if requested
    if pk_seq:
        with engine.begin() as conn:
            key_col = df.columns[0]
            sql = f"""
            MERGE INTO {table_name} t
            USING (SELECT rowid rid FROM {table_name} WHERE {key_col} IS NULL) src
            ON (t.rowid = src.rid)
            WHEN MATCHED THEN UPDATE SET {key_col} = {pk_seq}.NEXTVAL
            """
            conn.execute(text(sql))
            log.info(f"Populated {key_col} using sequence {pk_seq}")

# ----------------------------------------------------------------------
# 6.1️⃣ Idempotent loader for dim_date (avoids ORA-00001 on re-runs)
#       Uses a staging table + MERGE INSERT WHEN NOT MATCHED
# ----------------------------------------------------------------------
def upsert_dim_date(df: pd.DataFrame):
    """
    Load dim_date safely: insert only new date_key rows.
    This prevents ORA-00001 (unique constraint on date_key) across re-runs.
    """
    stg = "stg_dim_date"

    # Ensure types are explicit for the staging table (avoid Oracle FLOAT issues)
    date_dtype = {
        "date_key": Integer(),
        "calendar_date": Date(),
        "day": Integer(),
        "month": Integer(),
        "year": Integer(),
        "quarter": Integer(),
        "weekday": Integer(),
    }

    with engine.begin() as conn:
        # Create/replace staging table
        df.to_sql(
            name=stg,
            con=conn,
            if_exists="replace",
            index=False,
            dtype=date_dtype,
            chunksize=settings.BATCH_SIZE,
            # method="multi",  # keep disabled for Oracle
        )
        log.info("stg_dim_date created")

        # MERGE: insert only rows not already present in dim_date
        merge_sql = """
        MERGE INTO dim_date d
        USING stg_dim_date s
           ON (d.date_key = s.date_key)
        WHEN NOT MATCHED THEN
          INSERT (date_key, calendar_date, day, month, year, quarter, weekday)
          VALUES (s.date_key, s.calendar_date, s.day, s.month, s.year, s.quarter, s.weekday)
        """
        conn.execute(text(merge_sql))
        log.info("Upserted dim_date (inserted missing dates, skipped existing)")

        # Clean up staging table
        conn.execute(text("DROP TABLE stg_dim_date PURGE"))
        log.info("Removed temporary table stg_dim_date")

# ----------------------------------------------------------------------
# 7️⃣ Create schema if it does not exist (run DDL once)
# ----------------------------------------------------------------------
def initialise_schema():
    insp = inspect(engine)
    # Check for one of the dimension tables – if it exists, skip DDL
    if insp.has_table("dim_customer"):
        log.info("Schema already present – skipping DDL.")
        return

    ddl_path = Path(__file__).parents[2] / "sql" / "ddl_oracle.sql"
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    with engine.begin() as conn:
        log.info("Running DDL to create schema...")
        for stmt in ddl_sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    log.info("DDL executed successfully.")

# ----------------------------------------------------------------------
# 8️⃣ Load data into the warehouse
# ----------------------------------------------------------------------
def load_warehouse():
    # Dim tables – each gets its own sequence (except dim_date which is deterministic)
    bulk_load(cust,  "dim_customer", pk_seq="dim_customer_seq")
    bulk_load(prod,  "dim_product",  pk_seq="dim_product_seq")
    bulk_load(store, "dim_store",    pk_seq="dim_store_seq")

    # ⛔ Do NOT use bulk_load for dim_date (to avoid ORA-00001 on reruns)
    upsert_dim_date(dim_date)

    # --------------------------------------------------------------
    # Fact table – map surrogate keys in Python, then to_sql
    # --------------------------------------------------------------
    log.info("Preparing fact_sales payload…")

    # Pull surrogate key mappings from DB
    dim_customer_df = pd.read_sql("SELECT customer_id, customer_key FROM dim_customer", engine)
    dim_product_df  = pd.read_sql("SELECT product_id, product_key FROM dim_product", engine)
    dim_store_df    = pd.read_sql("SELECT store_id, store_key FROM dim_store", engine)
    dim_date_df     = pd.read_sql("SELECT calendar_date, date_key FROM dim_date", engine)

    # Build maps
    cust_map = dict(zip(dim_customer_df["customer_id"], dim_customer_df["customer_key"]))
    prod_map = dict(zip(dim_product_df["product_id"], dim_product_df["product_key"]))
    store_map = dict(zip(dim_store_df["store_id"], dim_store_df["store_key"]))
    date_map  = dict(zip(pd.to_datetime(dim_date_df["calendar_date"]).dt.normalize(),
                         dim_date_df["date_key"]))

    # Apply mappings
    sales["customer_key"] = sales["customer_id"].map(cust_map)
    sales["product_key"]  = sales["product_id"].map(prod_map)
    sales["store_key"]    = sales["store_id"].map(store_map)
    sales["date_key"]     = sales["sales_date"].dt.normalize().map(date_map)

    # Prepare final payload
    payload = sales[[
        "sales_id",
        "customer_key",
        "product_key",
        "store_key",
        "date_key",
        "quantity",
        "unit_price",
        "discount_pct",
        "total_amount"
    ]].copy()

    # Insert into fact_sales (let Oracle sequence assign sales_key automatically)
    with engine.begin() as conn:
        payload.to_sql(
            name="fact_sales",
            con=conn,
            if_exists="append",
            index=False,
            chunksize=settings.BATCH_SIZE,
        )
    log.info(f"Inserted {len(payload):,} rows into fact_sales")


# ----------------------------------------------------------------------
# 9️⃣ Main driver
# ----------------------------------------------------------------------
def main():
    log.info("=== Retail ETL – Oracle version ===")
    initialise_schema()
    load_warehouse()
    log.info("✅ ETL completed – data now lives in Oracle.")

if __name__ == "__main__":
    main()

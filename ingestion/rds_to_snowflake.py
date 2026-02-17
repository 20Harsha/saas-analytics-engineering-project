import os
import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -----------------------------
# RDS CONNECTION
# -----------------------------
rds_engine = create_engine(
    f"postgresql://{os.getenv('RDS_USER')}:{os.getenv('RDS_PASSWORD')}"
    f"@{os.getenv('RDS_HOST')}:{os.getenv('RDS_PORT')}/{os.getenv('RDS_DB')}"
)

print("Connected to RDS")

# -----------------------------
# SNOWFLAKE CONNECTION
# -----------------------------
sf_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

sf_cursor = sf_conn.cursor()
print("Connected to Snowflake")

# -----------------------------
# TABLES TO INGEST
# -----------------------------
tables = ["users", "plans", "subscriptions"]

for table in tables:
    print(f"\nProcessing table: {table}")
    target_table = f"{table}_raw".upper()

    # -----------------------------
    # STEP 1: Get last loaded timestamp (only for incremental tables)
    # -----------------------------
    if table in ["users", "subscriptions"]:
        try:
            query = f"""
            SELECT COALESCE(
                MAX(TO_TIMESTAMP_NTZ(updated_at::number / 1000000000)),
                '1900-01-01'::timestamp
            )
            FROM {target_table}
            """
            sf_cursor.execute(query)
            result = sf_cursor.fetchone()
            last_loaded_ts = result[0]

            print(f"Last loaded timestamp: {last_loaded_ts}")

        except Exception:
            last_loaded_ts = "1900-01-01 00:00:00"
            print("First run detected. Loading full table.")

    # -----------------------------
    # STEP 2: Read data from RDS
    # -----------------------------
    if table in ["users", "subscriptions"]:
        incremental_query = f"""
            SELECT *
            FROM {table}
            WHERE updated_at > '{last_loaded_ts}'
        """
    else:
        # plans table → truncate and full load
        print("Truncating plans_raw before full load.")
        sf_cursor.execute(f"TRUNCATE TABLE {target_table}")
        incremental_query = f"""
           SELECT * FROM {table}
        """

    df = pd.read_sql(incremental_query, rds_engine)
    print(f"Rows fetched: {len(df)}")

    if df.empty:
        print("No new data. Skipping load.")
        continue

    # -----------------------------
    # STEP 3: Normalize column names
    # -----------------------------
    df.columns = [col.upper() for col in df.columns]

    # -----------------------------
    # STEP 4: Convert timestamps safely
    # -----------------------------
    if "UPDATED_AT" in df.columns:
        df["UPDATED_AT"] = pd.to_datetime(df["UPDATED_AT"])

    if "CREATED_AT" in df.columns:
        df["CREATED_AT"] = pd.to_datetime(df["CREATED_AT"])

    # -----------------------------
    # STEP 5: Load into Snowflake
    # -----------------------------
    success, nchunks, nrows, _ = write_pandas(
        sf_conn,
        df,
        target_table
    )

    print(f"Loaded {nrows} rows into {target_table}")

# Close connections
sf_cursor.close()
sf_conn.close()

print("\nIncremental RDS ingestion completed successfully.")

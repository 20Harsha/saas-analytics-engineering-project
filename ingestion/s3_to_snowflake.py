import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cursor = conn.cursor()

print("Connected to Snowflake")

# -----------------------------
# Load events (CSV)
# -----------------------------
print("Loading events...")

# Step 1: load files into stage table
cursor.execute("""
COPY INTO events_stage
FROM @saas_raw_stage/events/
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,
    TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI'
)
ON_ERROR = 'CONTINUE';
""")

# Step 2: insert only new events
cursor.execute("""
INSERT INTO events_raw
SELECT s.*
FROM events_stage s
WHERE NOT EXISTS (
    SELECT 1
    FROM events_raw t
    WHERE t.event_id = s.event_id
);
""")

print("Events loaded (deduplicated)")

# -----------------------------
# Load payments (JSON)
# -----------------------------
print("Loading payments...")

cursor.execute("""
COPY INTO payments_json_raw
FROM @saas_raw_stage/payments/
FILE_FORMAT = (TYPE = 'JSON')
FORCE = TRUE;
""")

cursor.execute("""
INSERT INTO payments_raw
SELECT
    value:payment_id::STRING,
    value:user_id::STRING,
    value:subscription_id::STRING,
    value:amount::FLOAT,
    value:payment_date::DATE,
    value:payment_status::STRING
FROM payments_json_raw p,
LATERAL FLATTEN(input => raw) f
WHERE NOT EXISTS (
    SELECT 1
    FROM payments_raw t
    WHERE t.payment_id = f.value:payment_id::STRING
);
""")


print("Payments loaded")

cursor.close()
conn.close()

print("S3 ingestion completed successfully.")

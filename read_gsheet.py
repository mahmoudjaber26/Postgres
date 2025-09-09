import logging
import psycopg2
from psycopg2.extras import execute_values
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import json
import os

# ================== Absolute Path ==================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "powerbi-etl-e1ebfd104446.json")

# ================== CONFIGURE LOGGING ==================
logging.basicConfig(
    filename="logs.txt",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# ================== CONFIG ==================
CONFIG_FILE = r"C:\Users\mjaber\Downloads\Python Transfer Files\Google Sheets Read in PY\config.json"

DB_CONFIG = {
    "host": "127.0.0.1",
    "database": "postgres",
    "user": "postgres",
    "password": "0000",
    "port": "5432"
}

GSHEET_CREDENTIALS = r"C:\Users\mjaber\Downloads\Python Transfer Files\Google Sheets Read in PY\powerbi-etl-e1ebfd104446.json"

# ================== DB CONNECTION ==================
def connect_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("✅ Connected to PostgreSQL")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to PostgreSQL: {e}")
        raise

def connect_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            GSHEET_CREDENTIALS, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Connected to Google Sheets")
        return client
    except Exception as e:
        logging.error(f"❌ Failed to connect to Google Sheets: {e}")
        raise

# ================== HELPERS ==================
def create_table_if_not_exists(cursor, table_name, df):
    """Create table if not exists with inferred columns"""
    cols = []
    for col in df.columns:
        # Infer types
        if col.lower() == "submitted at":
            col_type = "TIMESTAMP"
        elif col.lower() == "cdn":
            col_type = "TEXT"
        else:
            col_type = "TEXT"
        cols.append(f"\"{col}\" {col_type}")
    col_defs = ", ".join(cols)

    # add unique constraint on cdn if exists
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {col_defs}
    );
    """
    cursor.execute(ddl)
    # If cdn exists, add unique index
    if "cdn" in [c.lower() for c in df.columns]:
        try:
            cursor.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_cdn_uniq" ON "{table_name}" ("cdn")'
            )
        except Exception as e:
            logging.warning(f"⚠ Could not create unique index on {table_name}: {e}")

def insert_new_rows(conn, table_name, df):
    """Insert new rows with deduplication"""
    if df.empty:
        logging.info(f"⚠ No data to insert for {table_name}")
        return

    cursor = conn.cursor()
    create_table_if_not_exists(cursor, table_name, df)

    # Ensure Submitted At column is timestamp
    if "Submitted At" in df.columns:
        df["Submitted At"] = pd.to_datetime(df["Submitted At"], errors="coerce")

    # Prepare insert
    cols = [f"\"{c}\"" for c in df.columns]
    values = [tuple(row) for row in df.to_numpy()]
    sql = f"""
        INSERT INTO "{table_name}" ({",".join(cols)})
        VALUES %s
    """
    if "cdn" in [c.lower() for c in df.columns]:
        sql += " ON CONFLICT (cdn) DO NOTHING"

    try:
        before = cursor.rowcount
        execute_values(cursor, sql, values)
        after = cursor.rowcount      
        conn.commit()
        inserted = after - before    
        logging.info(f"✅ Inserted {inserted} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Failed to insert rows into {table_name}: {e}")

# ================== MAIN ==================
if __name__ == "__main__":
    logging.info("🚀 Starting ETL Job")

    try:
        # connections
        conn = connect_postgres()
        gclient = connect_gsheet()

        # load config
        with open(CONFIG_FILE, "r") as f:
            sheet_files = json.load(f)
        logging.info("✅ Loaded config.json successfully")

        # iterate over files
        for _, cfg in sheet_files.items():
            file_name = cfg["file_name"]
            sheet_map = cfg.get("sheet_file", {})
            logging.info(f"📂 Processing file: {file_name}")
            gfile = gclient.open(file_name)

            for ws_name, table_name in sheet_map.items():
                try:
                    logging.info(f"➡ Loading sheet '{ws_name}' → table '{table_name}'")
                    worksheet = gfile.worksheet(ws_name)
                    data = worksheet.get_all_records()
                    df = pd.DataFrame(data)

                    if df.empty:
                        logging.info(f"⚠ Sheet {ws_name} is empty, skipping.")
                        continue

                    insert_new_rows(conn, table_name, df)

                except Exception as e:
                    logging.error(f"❌ Failed loading sheet {ws_name} into {table_name}: {e}")

    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("🔒 PostgreSQL connection closed")

    logging.info("✅ ETL Job Finished")
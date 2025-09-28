import logging
import os
import json
import hashlib
from typing import Dict, List, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================== PATHS ==================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

# ================== LOGGING ==================
logging.basicConfig(
    filename=os.path.join(BASE_DIR, "logs.txt"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(console)

# ================== ENV / SECRETS ==================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "database": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "sslmode": os.getenv("PGSSLMODE", "prefer"),  # set to "require" for managed Postgres
}
required = ["POSTGRES_HOST","POSTGRES_DB","POSTGRES_USER","POSTGRES_PASSWORD","POSTGRES_PORT"]
missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars : {', '.join(missing)}")
GSERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_CRED")  # JSON string (from GitHub Secret)
TMP_GOOGLE_CREDS_PATH = os.path.join(BASE_DIR, "gsa.json")

# ================== CONNECTIONS ==================
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
        if not GSERVICE_ACCOUNT_JSON:
            raise RuntimeError("Missing GSERVICE_ACCOUNT_JSON env var.")
        # Write creds to a temp file (do not commit)
        with open(TMP_GOOGLE_CREDS_PATH, "w", encoding="utf-8") as f:
            f.write(GSERVICE_ACCOUNT_JSON)

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(TMP_GOOGLE_CREDS_PATH, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Connected to Google Sheets")
        return client
    except Exception as e:
        logging.error(f"❌ Failed to connect to Google Sheets: {e}")
        raise

# ================== HELPERS ==================
def ensure_row_hash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a deterministic _row_hash column exists (used when 'cdn' uniqueness is absent).
    The hash is computed from the entire row content.
    """
    if "_row_hash" in df.columns:
        return df

    def row_to_hash(row: pd.Series) -> str:
        # Convert row to a sorted JSON string for stability
        # Use strings for all values to avoid dtype issues
        payload = {str(k): ("" if pd.isna(v) else str(v)) for k, v in row.to_dict().items()}
        blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    df["_row_hash"] = df.apply(row_to_hash, axis=1)
    return df

def create_table_if_not_exists(cursor, table_name: str, df: pd.DataFrame):
    """
    Create table if not exists, with inferred columns from df.
    Adds a _row_hash column for universal dedupe when 'cdn' is absent.
    """
    cols_sql: List[str] = []
    existing_cols = [c for c in df.columns if c != "_row_hash"]

    for col in existing_cols:
        # Basic inference (customize as needed)
        if col.lower() in ("submitted at", "submitted_at", "timestamp", "created_at"):
            col_type = "TIMESTAMP"
        else:
            col_type = "TEXT"
        cols_sql.append(f"\"{col}\" {col_type}")

    # add _row_hash
    cols_sql.append("\"_row_hash\" TEXT")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {", ".join(cols_sql)}
    );
    """
    cursor.execute(ddl)

    # Unique on cdn if exists; otherwise on _row_hash
    lowered = [c.lower() for c in existing_cols]
    if "cdn" in lowered:
        cursor.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_cdn_uniq" ON "{table_name}" ("cdn")'
        )
    cursor.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS "{table_name}_rowhash_uniq" ON "{table_name}" ("_row_hash")'
    )

def coerce_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for cand in ["Submitted At", "submitted at", "submitted_at", "Timestamp", "timestamp", "created_at"]:
        if cand in df.columns:
            df[cand] = pd.to_datetime(df[cand], errors="coerce")
    return df

def insert_new_rows(conn, table_name: str, df: pd.DataFrame):
    """
    Insert rows with deduplication:
    - If cdn present -> ON CONFLICT (cdn) DO NOTHING
    - Otherwise -> ON CONFLICT (_row_hash) DO NOTHING
    """
    if df.empty:
        logging.info(f"⚠ No data to insert for {table_name}")
        return

    cursor = conn.cursor()
    df = coerce_timestamps(df)
    df = ensure_row_hash(df)

    create_table_if_not_exists(cursor, table_name, df)

    cols = [c for c in df.columns]  # include _row_hash
    values = [tuple(None if (pd.isna(x) or x == "NaT") else x for x in row) for row in df.to_numpy()]
    cols_sql = ",".join([f"\"{c}\"" for c in cols])

    # Prefer cdn if present, else fallback to _row_hash
    conflict_target = "cdn" if "cdn" in [c.lower() for c in cols] else "_row_hash"

    sql = f"""
        INSERT INTO "{table_name}" ({cols_sql})
        VALUES %s
        ON CONFLICT ("{conflict_target}") DO NOTHING
    """

    try:
        execute_values(cursor, sql, values)
        conn.commit()
        logging.info(f"✅ Upserted {len(values)} rows into {table_name} (conflict on {conflict_target})")
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Failed to insert rows into {table_name}: {e}")
        raise

# ================== MAIN ==================
def main():
    logging.info("🚀 Starting ETL Job")

    conn = None
    try:
        conn = connect_postgres()
        gclient = connect_gsheet()

        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            sheet_cfg: Dict[str, Any] = json.load(f)
        logging.info("✅ Loaded config.json")

        # Iterate groups (e.g., Mobility, Home, Travel, Devices, Nissan)
        for _, cfg in sheet_cfg.items():
            file_name = cfg["file_name"]
            sheet_map = cfg.get("sheet_file", {})
            logging.info(f"📂 Processing file: {file_name}")

            try:
                gfile = gclient.open(file_name)
            except Exception as e:
                logging.error(f"❌ Could not open spreadsheet '{file_name}': {e}")
                continue

            for ws_name, table_name in sheet_map.items():
                try:
                    logging.info(f"➡ Loading sheet '{ws_name}' → table '{table_name}'")
                    ws = gfile.worksheet(ws_name)
                    data = ws.get_all_records()  # returns list[dict]
                    df = pd.DataFrame(data)

                    if df.empty:
                        logging.info(f"⚠ Sheet {ws_name} is empty, skipping.")
                        continue

                    insert_new_rows(conn, table_name, df)

                except Exception as e:
                    logging.error(f"❌ Failed loading sheet {ws_name} into {table_name}: {e}")

    except Exception as e:
        logging.error(f"❌ ETL job failed: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()
            logging.info("🔒 PostgreSQL connection closed")

    logging.info("✅ ETL Job Finished")

if __name__ == "__main__":
    main()




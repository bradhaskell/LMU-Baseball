import os
import io
import sys
import json
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_TAB_NAME = os.getenv("GSHEET_TAB_NAME", "Cauldron_Backend")
GSHEET_GID = os.getenv("GSHEET_GID")  # used for CSV export method
RAW_TABLE = os.getenv("RAW_TABLE", "raw_cauldron_scoreboard")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

# -----------------------
# Extraction methods
# -----------------------

def fetch_sheet_via_csv(gsheet_id: str, gid: str | None) -> pd.DataFrame:
    """
    Quick method: works if the sheet/tab is public to "Anyone with the link".
    """
    if gid:
        csv_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid={gid}"
    else:
        # If you only know the tab name and not gid, you can still export the first tab via 'gid=0'
        csv_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid=0"
    return pd.read_csv(csv_url)

def fetch_sheet_via_gspread(gsheet_id: str, tab_name: str, creds_json: str) -> pd.DataFrame:
    """
    Secure method for private sheets: uses a Google Service Account.
    Steps:
    1) Create a service account in Google Cloud (enable Sheets API).
    2) Add service account email as Viewer of the Sheet.
    3) Store JSON creds securely (locally as a file; in GitHub Actions as a secret).
    """
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    if creds_json.strip().startswith("{"):
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        # fallback: path to a JSON file on disk
        creds = Credentials.from_service_account_file(creds_json, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(gsheet_id)
    ws = sh.worksheet(tab_name)
    data = ws.get_all_records()  # returns list[dict]
    return pd.DataFrame(data)

# -----------------------
# Load to Postgres
# -----------------------

def ensure_table(engine):
    with engine.begin() as conn:
        # Execute schema if not present
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.execute(text(f.read()))

def load_dataframe(df: pd.DataFrame, engine, table: str):
    """
    Append to raw table. You can switch to an 'upsert' using row_hash if preferred.
    """
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)

def coerce_and_align_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light cleaning to match the raw schema. Adjust as needed.
    """
    # Strip leading/trailing spaces from columns
    df.columns = [c.strip() for c in df.columns]

    # Normalize obvious fields
    if "Year" in df.columns:
        df["year"] = pd.to_numeric(df["Year"], errors="coerce")
    if "Week" in df.columns:
        df["week"] = pd.to_numeric(df["Week"], errors="coerce")

    # Keep original column names but also alias a few for SQL (optional)
    # Reorder columns so important ones come first
    preferred_order = [c for c in ["Name","Year","Season","Team","Captain","Week"] if c in df.columns]
    rest = [c for c in df.columns if c not in preferred_order]
    df = df[preferred_order + rest]

    # Replace NaNs with None for SQL
    return df.where(pd.notnull(df), None)

def main():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # 1) Ensure table exists
    ensure_table(engine)

    # 2) Extract
    if GOOGLE_CREDS_JSON:
        df = fetch_sheet_via_gspread(GSHEET_ID, GSHEET_TAB_NAME, GOOGLE_CREDS_JSON)
    else:
        if not GSHEET_GID:
            print("WARNING: Using CSV export without GSHEET_GID may fetch the first tab only.", file=sys.stderr)
        df = fetch_sheet_via_csv(GSHEET_ID, GSHEET_GID)

    if df.empty:
        print("No rows returned from Google Sheets. Exiting.")
        return

    # 3) Basic cleaning / alignment
    df = coerce_and_align_columns(df)

    # Optional: create a stable hash for idempotent loads
    try:
        from utils import compute_row_hash
        cols_for_hash = [c for c in df.columns if c not in {"ingested_at"}]
        df["_row_hash"] = compute_row_hash(df, cols_for_hash)
    except Exception as e:
        print(f"Row hash not computed: {e}", file=sys.stderr)

    # 4) Load
    load_dataframe(df, engine, RAW_TABLE)
    print(f"Loaded {len(df)} rows into {RAW_TABLE}.")

if __name__ == "__main__":
    main()

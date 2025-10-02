# etl/google_sheets_extract_load_raw.py
import os, json, sys, pathlib, pandas as pd
from dotenv import load_dotenv

load_dotenv()

GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_TAB_NAME = os.getenv("GSHEET_TAB_NAME", "Cauldron_Backend")
GSHEET_GID = os.getenv("GSHEET_GID")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_cauldron_scoreboard")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")

def fetch_sheet_csv(gsheet_id: str, gid: str | None) -> pd.DataFrame:
    if gid:
        url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid={gid}"
    else:
        url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid=0"
    return pd.read_csv(url)

def fetch_sheet_gspread(gsheet_id: str, tab_name: str, creds_json: str) -> pd.DataFrame:
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
        creds = Credentials.from_service_account_file(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(gsheet_id).worksheet(tab_name)
    return pd.DataFrame(ws.get_all_records())

def coerce_and_align(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    if "Year" in df.columns: df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    if "Week" in df.columns: df["Week"] = pd.to_numeric(df["Week"], errors="coerce")
    return df

def write_extract(df: pd.DataFrame) -> str:
    out_dir = pathlib.Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d_%H%M%S") \
        .__add__("_cauldron_backend.csv")
    df.to_csv(out_path, index=False)
    return str(out_path)

def load_to_db(df: pd.DataFrame, table: str, db_url: str):
    from sqlalchemy import create_engine
    engine = create_engine(db_url, pool_pre_ping=True)
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)

def main():
    if not GSHEET_ID:
        print("GSHEET_ID missing; set it in .env or GitHub Secrets.", file=sys.stderr)
        sys.exit(2)

    # Prefer private auth when creds provided; else fall back to CSV export
    if GOOGLE_CREDS_JSON:
        df = fetch_sheet_gspread(GSHEET_ID, GSHEET_TAB_NAME, GOOGLE_CREDS_JSON)
    else:
        df = fetch_sheet_csv(GSHEET_ID, GSHEET_GID)

    if df.empty:
        print("No rows from Google Sheets.")
        return

    df = coerce_and_align(df)

    if not DATABASE_URL:
        # extract-only mode
        out = write_extract(df)
        print(f"[DRY RUN] Saved extract to {out}")
        return

    load_to_db(df, RAW_TABLE, DATABASE_URL)
    print(f"Loaded {len(df)} rows into {RAW_TABLE}.")

if __name__ == "__main__":
    main()


# etl/google_sheets_extract_load_raw.py
import os, json, sys, pathlib, pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL  # <-- important for safe DB URL building
import pymysql  # required by SQLAlchemy's mysql+pymysql dialect

load_dotenv()

# ---------- ENVIRONMENT VARIABLES ----------
GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_TAB_NAME = os.getenv("GSHEET_TAB_NAME", "Cauldron_Backend")
GSHEET_GID = os.getenv("GSHEET_GID")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_cauldron_scoreboard")

# MySQL credentials
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# Build SQLAlchemy URL safely (handles special chars in password)
DATABASE_URL = URL.create(
    "mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,  # do NOT URL-encode; URL.create handles it
    host=MYSQL_HOST,
    port=3306,
    database=MYSQL_DB,
)

# Google credentials (path or JSON string)
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", "")


# ---------- EXTRACT FUNCTIONS ----------
def fetch_sheet_csv(gsheet_id: str, gid: str | None) -> pd.DataFrame:
    """Public-sheet fallback (CSV export)."""
    if gid:
        url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid={gid}"
    else:
        url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid=0"
    return pd.read_csv(url)


def fetch_sheet_gspread(gsheet_id: str, tab_name: str, creds_json: str) -> pd.DataFrame:
    """Private-sheet auth using a Google service account."""
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    # Accept either file path or inline JSON
    if creds_json.strip().startswith("{"):
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(creds_json, scopes=scopes)

    gc = gspread.authorize(creds)
    ws = gc.open_by_key(gsheet_id).worksheet(tab_name)
    return pd.DataFrame(ws.get_all_records())


def fetch_drive_file_as_dataframe(file_id: str, creds_json: str, sheet_name: str | None = None) -> pd.DataFrame:
    """
    Fallback for non-Google Sheets files stored in Drive (e.g., .xlsx).
    Downloads the binary and reads with pandas.
    """
    import io
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    # Accept path or inline JSON
    if creds_json.strip().startswith("{"):
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(creds_json, scopes=scopes)

    drive = build("drive", "v3", credentials=creds)

    # Download file bytes
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    buf.seek(0)

    # Try Excel first; if that fails, try CSV
    try:
        return pd.read_excel(buf, sheet_name=sheet_name)  # needs openpyxl
    except Exception:
        buf.seek(0)
        return pd.read_csv(buf)


# ---------- TRANSFORM ----------
def coerce_and_align(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleanup and column normalization."""
    df.columns = [c.strip() for c in df.columns]
    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    if "Week" in df.columns:
        df["Week"] = pd.to_numeric(df["Week"], errors="coerce")
    return df


# ---------- LOAD HELPERS ----------
def ensure_table(engine, table_name: str):
    """Create target table if it doesn't exist."""
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        name VARCHAR(255),
        year INT,
        season VARCHAR(50),
        team VARCHAR(100),
        captain VARCHAR(100),
        week INT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_stmt))


def load_to_mysql(df: pd.DataFrame, table: str, db_url):
    """Append data to MySQL."""
    # If your RDS enforces SSL, add: connect_args={"ssl": {}}
    engine = create_engine(db_url, pool_pre_ping=True)
    ensure_table(engine, table)
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)


def write_extract(df: pd.DataFrame) -> str:
    """Save extracted data locally when DB not provided."""
    out_dir = pathlib.Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d_%H%M%S')}_cauldron_backend.csv"
    df.to_csv(out_path, index=False)
    return str(out_path)


def normalize_sheet_id(val: str) -> str:
    val = (val or "").strip()
    if val.startswith("http"):
        parts = val.split("/d/")
        if len(parts) >= 2:
            return parts[1].split("/")[0]
        raise ValueError("Provided URL does not look like a Google Sheets URL with /d/<id>/")
    return val


# ---------- MAIN ----------
def main():
    if not GSHEET_ID:
        print("❌ GSHEET_ID missing; set it in .env or GitHub Secrets.", file=sys.stderr)
        sys.exit(2)

    sheet_id = normalize_sheet_id(GSHEET_ID)
    human_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    print(f"🔎 Opening: {human_url}")
    print(f"📄 Tab name: '{GSHEET_TAB_NAME}'")

    # Extract
    if GOOGLE_CREDS_JSON:
        print("🔐 Using private access with service account...")
        try:
            # Try native Google Sheets first
            df = fetch_sheet_gspread(sheet_id, GSHEET_TAB_NAME, GOOGLE_CREDS_JSON)
        except Exception as e:
            from gspread.exceptions import APIError
            if isinstance(e, APIError) and "not supported for this document" in str(e):
                print("↪️ Detected non-Google Sheet in Drive; falling back to Drive download...")
                df = fetch_drive_file_as_dataframe(sheet_id, GOOGLE_CREDS_JSON, sheet_name=GSHEET_TAB_NAME)
            else:
                raise
    else:
        print("🌐 Using public CSV export (requires 'Anyone with link: Viewer')...")
        df = fetch_sheet_csv(sheet_id, GSHEET_GID)

    if df.empty:
        print("⚠️ No rows returned from the source.")
        return

    df = coerce_and_align(df)
    print(f"✅ Extracted {len(df)} rows from '{GSHEET_TAB_NAME}'.")

    # Load (or dry run)
    if not MYSQL_HOST or not MYSQL_USER or not MYSQL_DB:
        out = write_extract(df)
        print(f"💾 [DRY RUN] Saved extract locally to {out}")
        return

    print(f"📤 Loading {len(df)} rows into MySQL table '{RAW_TABLE}' on {MYSQL_HOST}...")
    load_to_mysql(df, RAW_TABLE, DATABASE_URL)
    print(f"🏁 Successfully loaded {len(df)} rows into '{RAW_TABLE}'.")


if __name__ == "__main__":
    main()


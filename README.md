# LMU Baseball – Google Sheets → Postgres EL

This pipeline extracts the `Cauldron_Backend` tab from a Google Sheet and appends it into a Postgres `raw_cauldron_scoreboard` table.

## Setup

1. `python -m venv .venv && source .venv/bin/activate`
2. `pip install -r requirements.txt`
3. Copy `.env.example` → `.env` and fill in values.
4. Create raw table: `psql "$DATABASE_URL" -f elt/schema.sql`
5. Run locally: `python -m elt.google_sheets_extract_load_raw`

## Deploy on GitHub Actions
- Add repository secrets:
  - `DATABASE_URL`, `GSHEET_ID`, `GSHEET_TAB_NAME`, `RAW_TABLE`
  - Either `GSHEET_GID` (if using CSV export) **or** `GOOGLE_APPLICATION_CREDENTIALS_JSON` (for private sheet).
- Trigger `Run workflow` or wait for the schedule.

## Notes
- Keep the raw table schema permissive (mostly TEXT). Transform downstream for typing/validation.
- If you need idempotency, switch to an upsert keyed by `_row_hash` instead of blind append.

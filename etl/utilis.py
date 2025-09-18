import hashlib
import pandas as pd

def compute_row_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Stable hash per row across selected columns for idempotent upserts."""
    def _hash_row(row):
        s = "||".join("" if pd.isna(row[c]) else str(row[c]) for c in cols)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()
    return df.apply(_hash_row, axis=1)

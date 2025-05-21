# -------------------------------
# data_cleaning.py  (cleaning layer)
# -------------------------------
"""Clean raw user data to the required dimensional‑model spec."""
from __future__ import annotations

import re
import pandas as pd


class DataCleaning:
    """Apply cleaning rules so that dim_users has high integrity."""

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            raise ValueError("Received empty DataFrame for cleaning.")

        cleaned = df.copy()

        # 1. standardise column names → snake_case
        cleaned.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in cleaned.columns]

        # 2. remove totally‑empty rows
        cleaned.dropna(how="all", inplace=True)

        # 3. deduplicate
        cleaned.drop_duplicates(inplace=True)

        # 4. parse all date/datetime‑like columns
        date_cols = [c for c in cleaned.columns if any(k in c for k in ("date", "dob", "joined"))]
        for col in date_cols:
            cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce")

        # 5. normalise telephone numbers → digits only strings
        phone_cols = [c for c in cleaned.columns if c.startswith("phone") or c.endswith("phone") or "mobile" in c]
        for col in phone_cols:
            cleaned[col] = cleaned[col].astype(str).str.replace(r"\D", "", regex=True)

        # 6. trim whitespace for object columns
        str_cols = cleaned.select_dtypes(include="object").columns
        cleaned[str_cols] = cleaned[str_cols].apply(lambda s: s.str.strip())

        # 7. fill NA in categoricals with "Unknown"
        cat_cols = cleaned.select_dtypes(include="object").columns
        cleaned[cat_cols] = cleaned[cat_cols].fillna("Unknown")

        # 8. sanity check row count
        expected = 15_284
        if len(cleaned) != expected:
            print(
                f"[WARNING] Expected {expected} rows after cleaning, got {len(cleaned)}. "
                "Investigate upstream raw data or adjust rules if spec changes."
            )
        return cleaned
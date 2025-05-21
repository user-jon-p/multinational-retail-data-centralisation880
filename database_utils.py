# -------------------------------
# data_utilis.py  (utility layer)
# -------------------------------
"""Utility helpers for database connectivity and generic I/O."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


class DatabaseConnector:
    """Handle RDS credential loading, engine creation, table listing & uploads."""

    def __init__(self, creds_path: str | Path):
        self.creds_path = Path(creds_path)
        self.creds: Dict[str, Any] = self.read_db_creds()
        self.engine: Engine = self.init_db_engine()

    # -------------------------------------------------------------
    # Step‑2  – read the YAML credential file
    # -------------------------------------------------------------
    def read_db_creds(self) -> Dict[str, Any]:
        if not self.creds_path.exists():
            raise FileNotFoundError(f"Credential file not found: {self.creds_path}")

        with self.creds_path.open("r", encoding="utf-8") as fh:
            creds = yaml.safe_load(fh)

        required = {"RDS_HOST", "RDS_PORT", "RDS_DATABASE", "RDS_USER", "RDS_PASSWORD"}
        missing = required - creds.keys()
        if missing:
            raise KeyError(f"Missing credential keys in YAML: {', '.join(missing)}")
        return creds

    # -------------------------------------------------------------
    # Step‑3  – build a SQLAlchemy engine
    # -------------------------------------------------------------
    def init_db_engine(self) -> Engine:
        c = self.creds
        uri = (
            f"postgresql://{c['RDS_USER']}:{c['RDS_PASSWORD']}@{c['RDS_HOST']}:{c['RDS_PORT']}/{c['RDS_DATABASE']}"
        )
        return create_engine(uri)

    # -------------------------------------------------------------
    # Step‑4  – list tables
    # -------------------------------------------------------------
    def list_db_tables(self) -> List[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    # -------------------------------------------------------------
    # Step‑7  – upload any dataframe
    # -------------------------------------------------------------
    def upload_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        chunksize: int | None = 10_000,
    ) -> None:
        if df.empty:
            raise ValueError("Attempting to upload an empty DataFrame.")
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunksize,
        )
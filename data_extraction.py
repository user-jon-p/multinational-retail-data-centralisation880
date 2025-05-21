# -------------------------------
# data_extraction.py  (ETL layer)
# -------------------------------
"""Extract tables from the raw sales database."""
from __future__ import annotations

import pandas as pd

# local import (relative)
from data_utilis import DatabaseConnector


class DataExtractor:
    """Provide methods for pulling tables into pandas DataFrames."""

    def read_rds_table(self, db_conn: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """Return the specified table as a DataFrame."""
        if table_name not in db_conn.list_db_tables():
            raise ValueError(f"Table '{table_name}' does not exist in the database.")
        return pd.read_sql_table(table_name, con=db_conn.engine)

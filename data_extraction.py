import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect

class DataExtractor:
    def __init__(self):
        pass

    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, creds):
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

    def read_rds_table(self, table_name):
        engine = self.init_db_engine()
        df = pd.read_sql_table(table_name, engine)
        return df

     

    
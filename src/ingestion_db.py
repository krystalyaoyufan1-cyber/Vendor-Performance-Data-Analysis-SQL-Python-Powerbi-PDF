import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
import re

# --- Logging ---
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --- DB engine ---
engine = create_engine('sqlite:///inventory.db')

# --- Helpers ---
def safe_table_name(filename: str) -> str:
    """Turn CSV filename into a safe SQLite table name."""
    name = filename[:-4]  # remove .csv
    name = re.sub(r'[\s\-]+', '_', name)      # spaces/dashes -> underscore
    name = re.sub(r'[^0-9a-zA-Z_]', '', name) # remove weird chars
    if name and name[0].isdigit():            # can't start with digit
        name = "t_" + name
    return name.lower()

def ingest_db(df, table_name, engine):
    """Ingest DataFrame into SQLite (replace table if exists)."""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    """Load all CSVs in /data and ingest them into SQLite DB."""
    start = time.time()
    ingested = []  # keep track for summary

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            table_name = safe_table_name(file)
            logging.info(f'Ingesting {file} -> table {table_name} (rows={len(df)}, cols={df.shape[1]})')
            ingest_db(df, table_name, engine)
            ingested.append((file, table_name, df.shape))

    end = time.time()
    total_time = (end - start) / 60
    logging.info('----------- Ingestion Complete -----------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

    # --- Print summary in notebook ---
    print("Ingestion Summary:")
    for file, table_name, shape in ingested:
        print(f" - {file} → table `{table_name}`: {shape[0]} rows × {shape[1]} cols")
    print(f"\nTotal time taken: {total_time:.2f} minutes")

if __name__ == '__main__':
    load_raw_data()

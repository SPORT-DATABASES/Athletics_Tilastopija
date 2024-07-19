from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

logger.info(f"Connecting to database at {host}:{port}, database: {database}, user: {user}")

# Load DataFrame from Parquet or CSV file
results_df = pd.read_parquet('results_df.parquet')

# Database engine setup for deletion
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', 
                       connect_args={'ssl': {'ca': ca_cert_path}})

min_date = results_df['Start_Date'].min()
logger.info(f"Minimum Start_Date in results_df: {min_date}")

min_date_str = min_date.strftime('%Y-%m-%d')
logger.info(f"Formatted min_date: {min_date_str}")

try:
    with engine.connect() as conn:
        # Turn off SQL safe updates
        conn.execute(text("SET SQL_SAFE_UPDATES = 0;"))

        # Get total rows before deletion
        total_rows_before = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database before deletion: {total_rows_before}")

        # Delete rows with Start_Date >= min_date
        delete_query = text("DELETE FROM Tilastopija_results WHERE Start_Date >= :min_date")
        result = conn.execute(delete_query, {'min_date': min_date_str})
        rows_deleted = result.rowcount
        logger.info(f"Rows deleted from database: {rows_deleted}")

        # Commit the deletion
        conn.commit()

        # Turn on SQL safe updates
        conn.execute(text("SET SQL_SAFE_UPDATES = 1;"))

        # Get total rows after deletion
        total_rows_after = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database after deletion: {total_rows_after}")

except SQLAlchemyError as e:
    logger.error(f"Error processing database operations: {e}")
finally:
    engine.dispose()

# Database engine setup for insertion
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', 
                       connect_args={'ssl': {'ca': ca_cert_path}})

try:
    total_rows = len(results_df)
    chunk_size = 1000
    rows_inserted = 0

    with engine.connect() as conn:
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            batch = results_df.iloc[start:end]
            batch.to_sql(name='Tilastopija_results', con=conn, if_exists='append', index=False)
            logger.info(f"Rows {start + 1} to {end} inserted into database")
            rows_inserted += len(batch)

        # Commit the insertion
        conn.commit()

        total_rows_after_insertion = conn.execute(text("SELECT COUNT(*) FROM Tilastopija_results")).scalar()
        logger.info(f"Total rows in database after insertion: {total_rows_after_insertion}")

    logger.info(f"Total rows inserted into database: {rows_inserted}")
    rows_added = rows_inserted - rows_deleted
    logger.info(f"Total new rows added to database: {rows_added}")

except SQLAlchemyError as e:
    logger.error(f"Error processing database operations: {e}")
finally:
    engine.dispose()

# Remove temporary file if it exists
if os.path.exists('results_df.parquet'):
    os.remove('results_df.parquet')

#!/usr/bin/env python
"""
Bronze Layer ETL for Tabby DWH project
Extracts Data from postgreSQL and loads it into the Bronze layer
"""

import os
import pandas as pd 
from sqlalchemy import create_engine, text
import duckdb
from datetime import datetime 
import logging


#set up logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s- %(levelname)s - %(message)s'
)

logger = logging.getLogger('bronze-etl')


PG_URI = "postgresql://postgres:taiwo@localhost:5432/tabby_source"

DUCKDB_PATH = os.path.expanduser("~/tabby-dwh/data/tabby_dwh.duckdb")

#define tables to extract

TABLES = [
    'customers',
    'merchants',
    'transactions',
    'payment_plans',
    'installments'
]

# User events table - treated as coming from a different source
USER_EVENTS_PATH = "data/raw/user_events.csv"

def ensure_data_directory():
    """Make sure the data directory exists"""
    data_dir = os.path.dirname(DUCKDB_PATH)
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Ensured data directory exists at {data_dir}")

def ensure_bronze_schema():
    conn = duckdb.connect(DUCKDB_PATH)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("Bronze schema created or already exists")
    except Exception as e:
        logger.error(f"Error creating bronze schema: {e}")
        raise
    finally:
        conn.close()

def extract_from_postgres(table_name):
    logger.info(f"Extracting {table_name} from postgreSQL")

    try:
        pg_engine = create_engine(PG_URI)

        # Query data
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, pg_engine)
        
        # Add metadata columns
        df['_etl_extracted_at'] = datetime.now()
        df['_etl_source'] = f"postgres.{table_name}"
        
        logger.info(f"Extracted {len(df)} records from {table_name}")
        return df
    
    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        raise
    finally:
        pg_engine.dispose()


def extract_user_events():
    """Extract user events from CSV"""
    logger.info("Extracting user events from CSV")
    
    try:
        # Read CSV
        df = pd.read_csv(USER_EVENTS_PATH)
        
        # Convert date strings to datetime
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        
        # Add metadata columns
        df['_etl_extracted_at'] = datetime.now()
        df['_etl_source'] = "file.user_events"
        
        logger.info(f"Extracted {len(df)} records from user_events")
        return df
    
    except Exception as e:
        logger.error(f"Error extracting user events: {e}")
        raise       


def load_to_bronze(df, table_name):
    """Load data to bronze layer in DuckDB"""
    logger.info(f"Loading {table_name} to bronze layer")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        
        # Define bronze table
        bronze_table = f"bronze.{table_name}"
        
        # Check if table exists
        table_exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{table_name}'").fetchone()[0] > 0
        
        if table_exists:
            # If table exists, truncate it (delete all rows)
            logger.info(f"Truncating existing table {bronze_table}")
            conn.execute(f"DELETE FROM {bronze_table} WHERE 1=1")
        else:
            # Create table if it doesn't exist
            logger.info(f"Creating new table {bronze_table}")
            conn.execute(f"CREATE TABLE {bronze_table} AS SELECT * FROM df LIMIT 0")
        
        # Insert new data
        conn.execute(f"INSERT INTO {bronze_table} SELECT * FROM df")
        
        # Count records for verification
        result = conn.execute(f"SELECT COUNT(*) FROM {bronze_table}").fetchone()
        
        logger.info(f"Loaded {result[0]} records into {bronze_table}")
    
    except Exception as e:
        logger.error(f"Error loading data to {bronze_table}: {e}")
        raise
    finally:
        conn.close()

def run_bronze_etl():
    """Run the Bronze layer ETL process"""
    logger.info("Starting Bronze layer ETL process")
    
    # Ensure data directory exists
    ensure_data_directory()
    
    # Ensure bronze schema exists
    ensure_bronze_schema()
    
    # Extract and load each table
    for table in TABLES:
        try:
            # Extract from source
            df = extract_from_postgres(table)
            
            # Load to bronze
            load_to_bronze(df, table)
        
        except Exception as e:
            logger.error(f"Failed to process {table}: {e}")
    
    # Process user events separately
    try:
        # Extract user events
        events_df = extract_user_events()
        
        # Load to bronze
        load_to_bronze(events_df, "user_events")
    
    except Exception as e:
        logger.error(f"Failed to process user_events: {e}")
    
    logger.info("Bronze layer ETL process completed")

if __name__ == "__main__":
    run_bronze_etl()
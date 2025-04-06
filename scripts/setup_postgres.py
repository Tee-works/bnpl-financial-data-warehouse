#!/usr/bin/env python
""" 
Set up Postgres database with sample data for Tabby DWH project
"""

import pandas as pd
from sqlalchemy import create_engine, text


# create postgres connection string
DB_URI = "postgresql://postgres:taiwo@localhost:5432/tabby_source"

def create_database():
    # Connect to the postgres database first
    engine = create_engine("postgresql://postgres:taiwo@localhost:5432/postgres")
    conn = engine.connect()

    # commit is required
    conn.execution_options(isolation_level="AUTOCOMMIT")

    try:
        conn.execute(text("CREATE DATABASE tabby_source"))
        print("Database created successfully")
    except Exception as e:
        print(f"Database may already exist: {e}")

    conn.close()
    engine.dispose()


def drop_tables():
    """Drop tables in the source database with CASCADE"""
    # Connect to the tabby_source database
    engine = create_engine(DB_URI)
    
    # Drop tables with CASCADE (this will drop dependent foreign key constraints)
    drop_tables = [
        "DROP TABLE IF EXISTS installments CASCADE;",
        "DROP TABLE IF EXISTS payment_plans CASCADE;",
        "DROP TABLE IF EXISTS transactions CASCADE;",
        "DROP TABLE IF EXISTS merchants CASCADE;",
        "DROP TABLE IF EXISTS customers CASCADE;"
    ]
    
    # Execute the DROP TABLE statements with CASCADE
    with engine.connect() as conn:
        for statement in drop_tables:
            conn.execute(text(statement))
        conn.commit()

    engine.dispose()
    print("Tables dropped successfully")


def load_data():
    """Load sample data into the database"""
    # Connect to the database
    engine = create_engine(DB_URI)
    
    # Load data from CSV files
    customers_df = pd.read_csv('data/raw/customers.csv')
    merchants_df = pd.read_csv('data/raw/merchants.csv')
    transactions_df = pd.read_csv('data/raw/transactions.csv')
    payment_plans_df = pd.read_csv('data/raw/payment_plans.csv')
    installments_df = pd.read_csv('data/raw/installments.csv')
    
    # Convert date strings to datetime
    for df in [customers_df, merchants_df, transactions_df, payment_plans_df, installments_df]:
        for col in df.columns:
            if 'date' in col.lower():
                df[col] = pd.to_datetime(df[col])
    
    # Load data into tables (replace existing data if necessary)
    customers_df.to_sql('customers', engine, if_exists='replace', index=False)
    merchants_df.to_sql('merchants', engine, if_exists='replace', index=False)
    transactions_df.to_sql('transactions', engine, if_exists='replace', index=False)
    payment_plans_df.to_sql('payment_plans', engine, if_exists='replace', index=False)
    installments_df.to_sql('installments', engine, if_exists='replace', index=False)
    
    print("Data loaded successfully")
    engine.dispose()


def main():
    """Main function to set up PostgreSQL database"""
    print("Setting up PostgreSQL database for Tabby DWH project...")
    
    create_database()
    drop_tables()
    load_data()
    
    print("PostgreSQL database setup complete!")


if __name__ == "__main__":
    main()

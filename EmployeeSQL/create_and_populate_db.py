import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# Database connection parameters
DB_NAME = "employees_db"
DB_USER = "postgres"
DB_PASSWORD = "bmws4"  # Change this to your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

def create_database():
    # Connect to default postgres database first
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Drop database if exists and create new one
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    
    cur.close()
    conn.close()

def create_schema():
    # Read the SQL file
    with open('sql_erd_import.sql', 'r') as file:
        sql_schema = file.read()
    
    # Connect to the new database and create schema
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Execute schema creation
    cur.execute(sql_schema)
    
    cur.close()
    conn.close()

def import_csv_data():
    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Define CSV files and their corresponding tables
    csv_files = {
        'titles': '../data/titles.csv',
        'departments': '../data/departments.csv',
        'employees': '../data/employees.csv',
        'dept_emp': '../data/dept_emp.csv',
        'dept_manager': '../data/dept_manager.csv',
        'salaries': '../data/salaries.csv'
    }
    
    # Import each CSV file
    for table, file_path in csv_files.items():
        try:
            df = pd.read_csv(file_path)
            df.to_sql(table, engine, if_exists='append', index=False)
            print(f"Successfully imported {file_path}")
        except Exception as e:
            print(f"Error importing {file_path}: {str(e)}")

def main():
    print("Creating database...")
    create_database()
    
    print("Creating schema...")
    create_schema()
    
    print("Importing CSV data...")
    import_csv_data()
    
    print("Database setup complete!")

if __name__ == "__main__":
    main()

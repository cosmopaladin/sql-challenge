import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Create database connection
engine = create_engine('postgresql://postgres:postgres@localhost:5432/employees_db')
Base = declarative_base()

# Define the tables
class Title(Base):
    __tablename__ = 'titles'
    title_id = Column(String, primary_key=True)
    title = Column(String)

class Employee(Base):
    __tablename__ = 'employees'
    emp_no = Column(Integer, primary_key=True)
    emp_title_id = Column(String, ForeignKey('titles.title_id'))
    birth_date = Column(Date)
    first_name = Column(String)
    last_name = Column(String)
    sex = Column(String)
    hire_date = Column(Date)

class Department(Base):
    __tablename__ = 'departments'
    dept_no = Column(String, primary_key=True)
    dept_name = Column(String)

class DeptEmployee(Base):
    __tablename__ = 'dept_emp'
    emp_no = Column(Integer, primary_key=True)
    dept_no = Column(String, ForeignKey('departments.dept_no'))

class DeptManager(Base):
    __tablename__ = 'dept_manager'
    emp_no = Column(Integer, primary_key=True)
    dept_no = Column(String, ForeignKey('departments.dept_no'))

class Salary(Base):
    __tablename__ = 'salaries'
    index = Column(Integer, primary_key=True)
    emp_no = Column(Integer, ForeignKey('employees.emp_no'))
    salary = Column(Integer)

# Create the tables
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Dictionary of CSV files and their corresponding table names
csv_files = {
    'titles': '../data/titles.csv',
    'departments': '../data/departments.csv',
    'employees': '../data/employees.csv',
    'dept_emp': '../data/dept_emp.csv',
    'dept_manager': '../data/dept_manager.csv',
    'salaries': '../data/salaries.csv'
}

# Import each CSV file into PostgreSQL
for table_name, file_path in csv_files.items():
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Convert date columns for employees table
        if table_name == 'employees':
            df['birth_date'] = pd.to_datetime(df['birth_date'])
            df['hire_date'] = pd.to_datetime(df['hire_date'])
        
        # Import to PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False
        )
        print(f"Successfully imported {table_name}")
    except Exception as e:
        print(f"Error importing {table_name}: {str(e)}")

print("Import complete!")

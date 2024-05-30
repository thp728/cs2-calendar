import sqlite3
from contextlib import closing
import pandas as pd
import os
from datetime import datetime,timedelta

class Database:
    def __init__(self, database_name: str = ":memory:"):
        self.database_name = database_name

    def create_table(self, table_name: str, columns: str):
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Table '{table_name}' created successfully.")

    

    def create_table_from_dataframe(self, table_name: str, dataframe: pd.DataFrame):
        columns = []
        for column, dtype in dataframe.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "REAL"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            columns.append(f"{column} {sql_type}")

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)

            # Get the column names
            column_names = dataframe.columns.tolist()

            # Construct the INSERT query with placeholders for values
            insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['?'] * len(column_names))});"

            # Extract values from DataFrame and convert to list of tuples
            values = dataframe.to_records(index=False)

            # Execute the INSERT query with all records
            cursor.executemany(insert_query, values)

            conn.commit()
            print(f"Table '{table_name}' created successfully and records inserted from the DataFrame.")


    def delete_database(self):
        if self.database_name == ":memory:":
            print("In-memory database will be reset upon reconnecting.")
        else:
            try:
                os.remove(self.database_name)
                print(f"Database '{self.database_name}' deleted successfully.")
            except FileNotFoundError:
                print(f"Database '{self.database_name}' does not exist.")
            except Exception as e:
                print(f"An error occurred while deleting the database: {e}")

  
    def describe_table(self, table_name: str):
        # Construct the SQL query to describe the table
        query = f"PRAGMA table_info({table_name});"

        # Connect to the database and execute the query
        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch all rows from the cursor
            rows = cursor.fetchall()

            # Print the table description
            print("Field\t\tType\t\tNull\tKey\tDefault\tExtra")
            for row in rows:
                print("\t".join(str(item) for item in row))
                
                
    def read_data(self, table_name: str, fields: list = None):
        # If no specific fields are provided, select all fields
        if fields is None:
            fields_clause = '*'
        else:
            # Convert the list of fields to a comma-separated string
            fields_clause = ', '.join(fields)

        # Construct the SQL query
        query = f"SELECT {fields_clause} FROM {table_name};"

        # Connect to the database and execute the query
        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch all rows from the cursor
            rows = cursor.fetchall()

            # Print the rows (you can modify this part to return the data as needed)
            for row in rows:
                print(row)
                

    def delete_past_dates(self, table_name: str, date_column: str):
        # Get the current date and time
        current_datetime = datetime.now()

        # Construct the SQL query to delete rows with date in the past
        query = f"DELETE FROM {table_name} WHERE {date_column} < ?;"

        # Connect to the database and execute the query
        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (current_datetime,))
            conn.commit()
            print(f"Rows with date in the past deleted successfully.")
    
    

    def clear_table(self, table_name: str):
        # Construct the SQL query to clear the table
        query = f"DELETE FROM {table_name};"

        # Connect to the database and execute the query
        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print(f"All rows deleted from the '{table_name}' table.")
    
    
    def delete(self, table_name: str, condition: str = None):
        # Construct the SQL query to delete rows
        if condition:
            query = f"DELETE FROM {table_name} WHERE {condition};"
        else:
            query = f"DELETE FROM {table_name};"

        # Connect to the database and execute the query
        with closing(sqlite3.connect(self.database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print("Rows deleted successfully.")    
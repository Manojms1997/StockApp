import pyodbc
import os
import pandas as pd
from datetime import datetime

server = '(LocalDB)\\MSSQLLocalDB'  # Your server name or IP address
database = 'stockapp'  # Your database name
username = 'YourUsername'  # Your SQL Server username
password = 'YourPassword'  # Your SQL Server password
driver = 'ODBC Driver 17 for SQL Server'  # ODBC Driver
folder_path = 'D:/STEM-OPT/Address-Change/data/daily/us/nasdaq stocks/1'

# If exception: returning true to force delete table 
def table_exists(table_name: str) -> bool:
    try:
        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        )
        cursor = connection.cursor()
        check_query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?;
        """
        cursor.execute(check_query, (table_name,))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        connection.close()

        return exists
    
    except Exception as e:
        print("Error checking if table exists:", e)
        return True 
    
def drop_table(table_name) -> None:
    try:
        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        )
        cursor = connection.cursor()
        query = f"DROP TABLE {table_name};"
        cursor.execute(query)
        cursor.close()
        connection.commit()
        connection.close()
        return
    
    except Exception as e:
        print("Error Dropping Table:", e)
        return

def create_table(name: str) -> bool:
    try:
        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        )
        print("Create-Table: Connection successful!")

        cursor = connection.cursor()

        query = """
            CREATE TABLE """ + name + """ (
                ID INT PRIMARY KEY,
                SDate DATE NOT NULL,
                SOpen FLOAT,
                SClose FLOAT,
                SHigh FLOAT,
                SLow FLOAT,
                SVol INT,
                SOpenInt INT
            );
            """
        
        cursor.execute(query)
        cursor.close()
        connection.commit()
        connection.close()

        print("Successfully Created Table.", name)
        return True
    
    except Exception as e:
        print("Error Creating Table:", e)
        return False

def insert(table_name: str, id: int, date, _open: float, _close: float, high: float, low: float, vol: int, open_int: int ) -> bool:
    try:
        connection = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
        )

        cursor = connection.cursor()

        query =f"""
            INSERT INTO {table_name} (ID, SDate, SOpen, SClose, SHigh, SLow, SVol, SOpenInt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        
        cursor.execute(query, (id, date, _open, _close, high, low, vol, open_int))
        cursor.close()
        connection.commit()
        connection.close()
        return True
    
    except Exception as e:
        print("Error inserting into table:", e)
        return False
    

def main():
    for filename in os.listdir(folder_path):
        if not filename.endswith('.txt'):
            print(filename, " is not a valid text file")
            continue

        file_path = os.path.join(folder_path, filename)
        stock_name = filename.replace(".us.txt", "")

        if table_exists(stock_name):
            drop_table(stock_name)

        if not create_table(stock_name):
            continue
        
        try:
            df = pd.read_csv(file_path)
            inserted_rows = 0
            for i in range(len(df)):
                id = i + 1
                date = datetime.strptime(str(df.iloc[i]["<DATE>"]), "%Y%m%d").date()
                _open = float(df.iloc[i]["<OPEN>"])
                _close = float(df.iloc[i]["<CLOSE>"])
                high = float(df.iloc[i]["<HIGH>"])
                low = float(df.iloc[i]["<LOW>"])
                vol = int(df.iloc[i]["<VOL>"])
                open_int = int(df.iloc[i]["<OPENINT>"])
                
                if insert(stock_name, id, date, _open, _close, high, low, vol, open_int):
                    inserted_rows += 1

            print("========== Inserted", inserted_rows, "rows into", stock_name, "table ===========")            
            
        except Exception as e:
            print(f"Error reading {filename}: {e}")

main()
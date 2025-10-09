
import pandas as pd
import pyodbc

def create_sql_connection_string(server, database, username, password, driver='{SQL Server}'):
    """
    สร้าง connection string สำหรับเชื่อมต่อกับ SQL Server

    :param server: ชื่อเซิร์ฟเวอร์
    :param database: ชื่อฐานข้อมูล
    :param username: ชื่อผู้ใช้
    :param password: รหัสผ่าน
    :return: Connection string สำหรับเชื่อมต่อกับ SQL Server
    """
    #ODBC Driver 17 for SQL Server
    return f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"



def create_sql_table_script(df, table_name, primary_keys=None, use_column_names=True):
    """
    สร้าง SQL script สำหรับการสร้างตารางใน SQL Server จาก DataFrame

    :param df: Pandas DataFrame ที่ต้องการแปลงเป็น SQL table
    :param table_name: ชื่อของตารางใน SQL
    :param primary_keys: รายชื่อคอลัมน์ที่ต้องการตั้งเป็น primary key (ถ้ามี)
    :param use_column_names: ใช้ชื่อคอลัมน์จาก DataFrame หรือไม่ (True/False)
    :return: SQL script สำหรับการสร้างตาราง
    """
    
    # กำหนด data types สำหรับ SQL Server
    sql_data_types = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'NVARCHAR(MAX)',#'TEXT',
        'bool': 'BIT',
        'datetime64[ns]': 'DATETIME',
        'datetime64[ns, Asia/Bangkok]': 'DATETIME',
        'str' : 'VARCHAR(255)'
    }

    # เริ่มต้นสร้าง SQL script
    sql_script = f"CREATE TABLE {table_name} (\n"

    # สร้างรายการคอลัมน์และ data types
    # for column in df.columns:
    #     col_name = column if use_column_names else f"[Column_{column}]"
    #     data_type = sql_data_types.get(str(df[column].dtype), 'NVARCHAR(MAX)')  # กำหนดค่า default เป็น NVARCHAR(MAX)
    #     sql_script += f"    {col_name} {data_type},\n"

    for i, column in enumerate(df.columns):
        if use_column_names and column and not str.__contains__(column,'Unnamed:'):  # ถ้ามีชื่อคอลัมน์
            col_name =f'[{column}]'
        else:  # ถ้าไม่มีชื่อคอลัมน์
            col_name = f'[col{i+1:03}]'  # col001, col002, ...
        
        dtype = str(df[column].dtype)
        sql_type = sql_data_types.get(dtype, 'TEXT')  # ใช้ TEXT ถ้าไม่พบชนิดข้อมูล
        col_name = col_name.replace(" ", "")
        sql_script += f"    {col_name} {sql_type},\n"# columns.append(f"    {col_name} {sql_type}")

    # หากมีการระบุ primary keys ให้เพิ่มไปยัง script
    if primary_keys:
        primary_keys_str = ", ".join(primary_keys)
        sql_script += f"    PRIMARY KEY ({primary_keys_str})\n"
    
    # ปิดวงเล็บ
    sql_script += ");"

    return sql_script

""" 
# ตัวอย่างการใช้งาน
data = {
    'CustomerID': [1001, 1002, 1003],
    'Name': ['Customer A', 'Customer B', 'Customer C'],
    'Age': [25, 30, 22],
}

df = pd.DataFrame(data)

# สร้าง SQL script
script = create_sql_table_script(df, "Customers", primary_keys=['CustomerID'], use_column_names=True)
print(script)
 """




def import_excel_to_sql_server(file_path, table_name,sqlConnection, start_row=0):
    """
    อ่านไฟล์ Excel และนำข้อมูลไป import ลงใน SQL Server

    :param file_path: ตำแหน่งที่ตั้งของไฟล์ Excel
    :param table_name: ชื่อของตารางใน SQL Server
    :param start_row: แถวแรกที่ต้องการ import (เริ่มนับจาก 0)
    """
    try:
        # อ่านไฟล์ Excel เริ่มจากแถวที่กำหนด
        df = pd.read_excel(file_path, header=None, skiprows=start_row)

        # สร้างชื่อคอลัมน์ใหม่
        df.columns = [f'col{i+1:03}' for i in range(df.shape[1])]

        # เชื่อมต่อกับ SQL Server
        # conn_str = 'DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password'
        # sqlConnection = pyodbc.connect(conn_str)
        cursor = sqlConnection.cursor()

        # สร้างตารางใน SQL Server
        create_table_query = f"CREATE TABLE {table_name} (\n"
        create_table_query += ",\n".join([f"{col} NVARCHAR(500)" for col in df.columns])
        create_table_query += "\n);"
        
        cursor.execute(create_table_query)
        sqlConnection.commit()

        # นำข้อมูลเข้าไปในตาราง
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in row])})"
            cursor.execute(insert_query, tuple(row))
        
        sqlConnection.commit()
        print(f"Data imported successfully into table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        cursor.close()
        sqlConnection.close()
""" 
# ตัวอย่างการใช้งาน
file_path = 'path/to/your/excel_file.xlsx'  # เปลี่ยนเป็น path ที่ต้องการ
table_name = 'YourTableName'  # ตั้งชื่อของตาราง
import_excel_to_sql_server(file_path, table_name, start_row=1)  # ระบุแถวแรกที่ต้องการ import
 """



def insert_data_to_sql_table(connection_string, table_name, data_dict):

    """
    Insert data into a SQL table using keys from a dictionary as column names.

    :param connection_string: Connection string to connect to the SQL Server
    :param table_name: Name of the table where data will be inserted
    :param data_dict: Dictionary containing data to be inserted
    """
    try:
        # เชื่อมต่อกับ SQL Server
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # สร้างชื่อคอลัมน์และค่าที่จะ insert
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['?' for _ in data_dict])
        values = list(data_dict.values())

        # สร้างคำสั่ง SQL INSERT
        sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insert command
        cursor.execute(sql_insert, values)
        conn.commit()

        print(f"Data inserted successfully into table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        cursor.close()
        conn.close()

# ตัวอย่างการใช้งาน
# connection_string = 'DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password'
# table_name = 'YourTableName'
# data_dict = {
#     'col001': 'Value1',
#     'col002': 'Value2',
#     'col003': 'Value3'
# }

# insert_data_to_sql_table(connection_string, table_name, data_dict)

def import_df_to_sql_server(df, table_name, sqlConnection, primary_keys=None, use_column_names=True):
    """
    Import a Pandas DataFrame into a SQL Server table.

    :param df: Pandas DataFrame to be imported
    :param table_name: Name of the table in SQL Server
    :param primary_keys: List of primary key columns
    :param use_column_names: Use column names from the DataFrame
    """
    try:
        # Connect to SQL Server
        # conn_str = 'DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password'
        # sqlConnection = pyodbc.connect(conn_str)
        cursor = sqlConnection.cursor()

        # Create SQL table script
        create_table_query = create_sql_table_script(df, table_name, primary_keys, use_column_names)
        cursor.execute(create_table_query)
        sqlConnection.commit()

        # Insert data into the table
        for index, row in df.iterrows():
            columns = ', '.join([f'[{col}]' for col in df.columns])
            placeholders = ', '.join(['?' for _ in df.columns])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
        
        sqlConnection.commit()
        print(f"Data imported successfully into table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    # finally:
        # cursor.close()
        # sqlConnection.close()
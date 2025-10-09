import pandas as pd

def add_index_as_first_column(df):
    """Add the index of the DataFrame as the first column."""
    # รีเซ็ต index และเก็บ index เป็นคอลัมน์ใหม่
    df_reset = df.reset_index()
    
    # เปลี่ยนชื่อคอลัมน์ index ถ้าต้องการ
    df_reset.rename(columns={'index': 'Index'}, inplace=True)
    
    return df_reset

# def add_column_to_dataframe(df, column_position, column_name, default_value):
#     """
#     เพิ่มคอลัมน์ใหม่เข้าไปใน DataFrame

#     Parameters:
#     df (pd.DataFrame): DataFrame ที่จะเพิ่มคอลัมน์
#     column_position (int): ตำแหน่งที่ต้องการเพิ่มคอลัมน์ (0 สำหรับคอลัมน์แรก)
#     column_name (str): ชื่อของคอลัมน์ใหม่
#     default_value: ค่าเริ่มต้นที่จะใช้ในคอลัมน์ใหม่

#     Returns:
#     pd.DataFrame: DataFrame ที่มีการเพิ่มคอลัมน์ใหม่
#     """
#     # เพิ่มคอลัมน์ใหม่ที่ตำแหน่งที่ระบุ
#     df.insert(column_position, column_name, default_value)
    
#     return df

def add_column_to_dataframe(df, position, column_name, default_value, data_type=object):
    """
    เพิ่มคอลัมน์ใหม่เข้าไปใน DataFrame

    Parameters:
    df (pd.DataFrame): DataFrame ที่จะเพิ่มคอลัมน์
    position (int): ตำแหน่งที่จะเพิ่มคอลัมน์ (0 สำหรับคอลัมน์แรก)
    column_name (str): ชื่อของคอลัมน์ใหม่
    default_value: ค่าเริ่มต้นสำหรับคอลัมน์ใหม่
    data_type: ชนิดข้อมูลของคอลัมน์ใหม่ (เช่น 'int', 'float', 'str')

    Returns:
    pd.DataFrame: DataFrame ที่มีการเพิ่มคอลัมน์ใหม่
    """
    
    # สร้าง Series ใหม่ด้วยค่าเริ่มต้นและชนิดข้อมูลที่ระบุ
    new_column = pd.Series([default_value] * len(df), dtype=data_type)
    
    # ใช้ insert เพื่อเพิ่มคอลัมน์ใหม่ที่ตำแหน่งที่ระบุ
    df.insert(position, column_name, new_column)
    
    return df

def read_csv_to_dataframe(file_path, delimiter=','):
    """
    อ่านไฟล์ CSV หรือ Text และแปลงให้เป็น DataFrame โดยสามารถระบุ delimiter ได้

    :param file_path: ตำแหน่งที่ตั้งของไฟล์
    :param delimiter: ตัวแบ่งคอลัมน์ (default คือ ',')
    :return: DataFrame ที่เก็บข้อมูลจากไฟล์
    """
    try:
        # อ่านไฟล์และแปลงเป็น DataFrame
        df = pd.read_csv(file_path, delimiter=delimiter)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None 
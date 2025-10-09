import pandas as pd
# import xlrd 

def read_excel_to_dataframe(file_path, sheet_name=0):
    """
    อ่านไฟล์ Excel และแปลงเป็น DataFrame

    :param file_path: ตำแหน่งที่ตั้งของไฟล์ Excel
    :param sheet_name: ชื่อหรือดัชนีของชีตที่ต้องการอ่าน (ค่าเริ่มต้นคือชีตแรก)
    :return: DataFrame ที่เก็บข้อมูลจากไฟล์ Excel
    """
    try:
        # อ่านไฟล์ Excel และแปลงเป็น DataFrame
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except Exception as e:
        return f"An error occurred: {e}"

# ตัวอย่างการใช้งาน
file_path = 'path/to/your/excel_file.xlsx'  # เปลี่ยนเป็น path ที่ต้องการ
dataframe = read_excel_to_dataframe(file_path)
print(dataframe)

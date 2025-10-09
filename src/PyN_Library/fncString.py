import re

def clean_string(input_string):
    """
    ล้างอักขระพิเศษใน string

    Parameters:
    input_string (str): สตริงที่ต้องการล้างอักขระพิเศษ

    Returns:
    str: สตริงที่ไม่มีอักขระพิเศษ
    """
    # ใช้ regular expression เพื่อแทนที่อักขระพิเศษด้วยค่าว่าง
    cleaned_string = re.sub(r'[^a-zA-Z0-9\s]', '', input_string)
    
    return cleaned_string


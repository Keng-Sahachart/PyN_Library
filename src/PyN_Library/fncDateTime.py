#จัดการเรื่อง วันที่และเวลา
from datetime import datetime, timedelta




def convert_UnixTimestamp_to_DateTime(UnixTimestamp):
    #แปลง Unix Timestamp เป็น DateTime
    return datetime.fromtimestamp(UnixTimestamp)

def convert_DateTime_to_UnixTimestamp(date_time):
    #แปลง DateTime เป็น Unix Timestamp
    return int(date_time.timestamp())

def add_days_to_date(date_time, days):
    #เพิ่มวันให้กับ DateTime
    return date_time + timedelta(days=days)

def subtract_days_from_date(date_time, days):
    #ลบวันจาก DateTime
    return date_time - timedelta(days=days)
def format_date(date_time, format_string="%Y-%m-%d %H:%M:%S"):
    #ฟอร์แมต DateTime เป็นสตริง
    return date_time.strftime(format_string)
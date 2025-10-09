"""function for manage file folder"""


def create_folders(path):
    """
    ตรวจสอบและสร้างโฟลเดอร์ตาม path ที่ระบุ

    :param path: ตำแหน่งที่ต้องการสร้างโฟลเดอร์
    """
    import os
    if not os.path.exists(path):
        os.makedirs(path)
        return f"Directory '{path}' created successfully."
    return f"Directory '{path}' already exists."



def get_files_in_folder(folder_path, file_extension=None):
    """
    ดึงชื่อไฟล์ทั้งหมดในโฟลเดอร์ที่ระบุ โดยสามารถระบุประเภทของไฟล์ได้

    :param folder_path: ตำแหน่งที่ตั้งของโฟลเดอร์
    :param file_extension: ประเภทของไฟล์ (เช่น '.txt', '.jpg') หากไม่ระบุ จะดึงไฟล์ทั้งหมด
    :return: รายชื่อไฟล์ในรูปแบบ string
    """
    import os
    import fnmatch
    try:
        # ตรวจสอบว่าโฟลเดอร์มีอยู่จริงหรือไม่
        if not os.path.exists(folder_path):
            return f"Directory '{folder_path}' does not exist."

        # สร้างรายการเพื่อเก็บชื่อไฟล์
        files = []

        # ดึงชื่อไฟล์ทั้งหมดในโฟลเดอร์
        for filename in os.listdir(folder_path):
            # ตรวจสอบว่าเป็นไฟล์หรือไม่
            if os.path.isfile(os.path.join(folder_path, filename)):
                # หากมีการระบุประเภทของไฟล์ ให้ทำการตรวจสอบ
                if file_extension:
                    if fnmatch.fnmatch(filename, f'*{file_extension}'):
                        files.append(filename)
                else:
                    files.append(filename)

        # แปลงรายการเป็น string และส่งกลับ
        return ', '.join(files)

    except Exception as e:
        return str(e)

# # ตัวอย่างการใช้งาน
# folder_path = 'path/to/your/folder'  # เปลี่ยนเป็น path ที่ต้องการ
# result = get_files_in_folder(folder_path, '.txt')  # ระบุประเภทของไฟล์ที่ต้องการ
# print(result)



def delete_folder(folder_path):
    """
    ลบโฟลเดอร์ทั้งหมด (รวมถึงไฟล์และโฟลเดอร์ย่อย) หากพบโฟลเดอร์นั้น

    :param folder_path: ตำแหน่งที่ตั้งของโฟลเดอร์ที่ต้องการลบ
    :return: ข้อความยืนยันผลการลบ
    """
    import os
    import shutil
    try:
        # ตรวจสอบว่าโฟลเดอร์มีอยู่จริงหรือไม่
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
            return True #f"Directory '{folder_path}' has been deleted successfully."
        else:
            print(f"Directory '{folder_path}' does not exist.")
            return False 
    except Exception as e:
        print(f"An error occurred: {e}")
        return False 

# # ตัวอย่างการใช้งาน
# folder_path = 'path/to/your/folder'  # เปลี่ยนเป็น path ที่ต้องการ
# result = delete_folder(folder_path)
# print(result)

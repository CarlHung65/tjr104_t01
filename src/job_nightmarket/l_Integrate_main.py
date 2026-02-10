import os
import shutil
import t_clean_schema
import l_SQL_to_GCP

def remove_pycache():
    for root, dirs, files in os.walk(".", topdown=False):
        for d in dirs:
            if d == "__pycache__":
                folder_path = os.path.join(root, d)
                shutil.rmtree(folder_path)
                print(f"已刪除：{folder_path}")

if __name__ == "__main__":
    t_clean_schema.main()
    l_SQL_to_GCP.main()
    remove_pycache()
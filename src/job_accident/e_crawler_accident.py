import requests  # 負責：發送網路請求，把網頁內容和檔案「搬」回來
from bs4 import BeautifulSoup  # 負責：解析 HTML 原始碼，像濾網一樣過濾出我們要的資訊
import os  # 負責：跟你的作業系統溝通，建立資料夾、串接路徑、檢查檔案
import urllib3  # 負責：底層網路設定，這裡用來關閉 SSL 安全檢查警告
import zipfile  # 負責：處理壓縮檔，讀取裡面的 CSV 內容
import pandas as pd  # 負責：資料處理，把 CSV 轉成表格，進行切分與清洗
import re #因為近年資料A1,A2的title分別為"CSV下載檔案"以及"ZIP下載檔案",所以用Regular Expression只抓取"下載檔案"
from datetime import datetime
from create_accident_table import (HEADERS,
                    SAVE_OLD_DATA_DIR,
                    SAVE_NEW_DATA_DIR,
                    RECENT_PAGE_A1_URL,
                    RECENT_PAGE_A2_URL) 


# 關閉「不安全請求」的警告，因為我們使用了 verify=False (跳過憑證檢查)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# ------------------------------------------------------------------
# 爬蟲與下載 (Extract) .zip
# ------------------------------------------------------------------
def auto_scrape_and_download_old_data(page_url)->list:
    # 建立存檔資料夾，exist_ok=True 代表若資料夾已存在就不會報錯
    os.makedirs(SAVE_OLD_DATA_DIR, exist_ok=True)
     
    
    extracted_data_12mon = []# 用來存讀取出來的 12個月的DataFrame
    print(f"正在分析網頁結構: {page_url}")
    # verify=False 解決政府網站常見的 SSL 憑證驗證失敗問題
    res = requests.get(page_url, headers=HEADERS, verify=False)
    #這一步是 Python 幫你對伺服器發出「敲門」請求，伺服器會把整個網頁的 HTML 原始碼（就像一堆混亂的標籤）丟回來給 res.text

    # 使用 BeautifulSoup 解析 HTML，'html.parser' 是 Python 內建的解析器
    #這是一個「整理工具」。它把那一堆亂七八糟的 HTML 原始碼變成一個「樹狀結構」，讓你可以搜尋
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # find() 找尋第一個符合條件的標籤，我們利用 title 屬性精準定位下載按鈕
    #這就是你的眼睛。它在 HTML 樹裡找 <a>（超連結標籤），而且精確鎖定那個 title 叫做「CSV 下載檔案」的按鈕
    csv_link_tag = soup.find('a', title="CSV下載檔案")

    if not csv_link_tag:
        print("錯誤：找不到下載標籤！")
        return False
    #找到按鈕後，這一步是拿到按鈕背後隱藏的 真正的下載網址 (FILE_URL)
    FILE_URL = csv_link_tag.get('href') # 取得 <a> 標籤中的 href 屬性 (即檔案網址)
    print(f"成功鎖定檔案位址: {FILE_URL}")
    
    try:
    # stream=True 啟用串流模式，檔案會一塊一塊下載，不會一次塞爆記憶體
            with requests.get(FILE_URL, headers=HEADERS, stream=True, timeout=60, verify=False) as r:
                r.raise_for_status() # 檢查連線狀態，若非 200 (成功) 則觸發異常
                raw_filename =  FILE_URL.split('/')[-1]#這邊的的raw_filename 是一個接收FILE_URL被split('/')<--最後一個'/' 的物件
                if not raw_filename.endswith('.zip'): #如果沒有抓到上面的規則,就抓網頁的url的倒數'/'當檔名
                    file_id = page_url.split('/')[-1]
                    raw_filename = f"accident_{file_id}.zip"
                #最終路徑組合
                final_path = os.path.join(SAVE_OLD_DATA_DIR, raw_filename)
                print(f"正在下載至: {final_path}")

                with open(final_path, 'wb') as f:
                    # iter_content() 每次讀取 1MB 的資料，並寫入硬碟
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                         f.write(chunk)
            print(f"【下載成功】檔案存於: {final_path}")

            print(f"--- 解壓縮 ---")
            with zipfile.ZipFile(final_path, 'r') as z:
                for info in z.infolist():
                    #raw_n = info.filename
            # 處理檔名亂碼 (cp437 -> cp950)
                    try:
                        real_name = info.filename.encode('cp437').decode('cp950')
                    except:
                        real_name = info.filename
                    #print(f"DEBUG: 原始檔名=[{raw_n}] -> 解碼後=[{real_name}]")
            # 只抓 A1、A2 的 CSV
                    if ("A1" in real_name or "A2" in real_name) and real_name.endswith('.csv'):
                        with z.open(info.filename) as f:
                        # 讀取並存入 list 中
                            df = pd.read_csv(f, encoding='utf-8-sig')
                            print(f"已讀取檔案: {real_name}")
                            extracted_data_12mon.append(df)
        
    except Exception as e:
        print(f"下載失敗: {e}")
    return extracted_data_12mon

# ------------------------------------------------------------------
# 爬蟲與下載近期 (Extract) .csv/.zip
# ------------------------------------------------------------------
def auto_scrape_recent_data()->list:
    # 1. 建立資料夾
    os.makedirs(SAVE_NEW_DATA_DIR, exist_ok=True)

    
    A1A2_List=[]
    # 2. 發送請求並放入清單 (正確的 append 或直接宣告)
    ra1 = requests.get(RECENT_PAGE_A1_URL, headers=HEADERS, verify=False)
    ra2 = requests.get(RECENT_PAGE_A2_URL, headers=HEADERS, verify=False)
    recentA1A2 = [ra1, ra2] 
    
    # 為了區分檔名，我們加一個簡單的計數器
    count = 1

    for r1r2 in recentA1A2:
        soup = BeautifulSoup(r1r2.text, 'html.parser')
        csv_link_tag = soup.find('a', title=re.compile("下載檔案"))
       
        if not csv_link_tag:
            print(f"錯誤：第 {count} 個頁面找不到下載標籤！")
            count += 1
            continue # 找不到就換下一個，不要直接 return False

        FILE_URL = csv_link_tag.get('href')
       
        print(f"成功鎖定檔案位址: {FILE_URL}")
    
        try:
            with requests.get(FILE_URL, headers=HEADERS, stream=True, timeout=60, verify=False) as r:
                r.raise_for_status()

                tag_title = csv_link_tag.get('title', '')
                if "ZIP" in tag_title:
                    file_name = f"recent_A{count}.zip"
                elif "CSV" in tag_title:
                    file_name = f"recent_A{count}.csv"

                final_save_path = os.path.join(SAVE_NEW_DATA_DIR, file_name)

                print(f"正在下載至: {final_save_path}")


                with open(final_save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        #每次從網路接收 1MB 的數據就立刻寫入硬碟，然後再接下一個 1MB。
                        if chunk:
                            f.write(chunk)

                if ".zip" in file_name:
                    with zipfile.ZipFile(final_save_path, 'r') as z:
                        for info in z.infolist():
                        # 處理檔名亂碼 (cp437 -> cp950)
                            try:
                                real_name = info.filename.encode('cp437').decode('cp950')
                            except:
                                real_name = info.filename
            
                    # 只抓 A1、A2 的 CSV
                        if ("A1" in real_name or "A2" in real_name) and real_name.endswith('.csv'):
                            with z.open(info.filename) as f:
                        # 讀取並存入 list 中
                                df = pd.read_csv(f, encoding='utf-8-sig')
                        print(f"已讀取檔案: {real_name}")
                elif ".csv" in file_name: 
                    df = pd.read_csv(final_save_path,encoding='utf-8-sig')


                #print(df.head())
                A1A2_List.append(df)
                print(f"【下載成功】檔案存於: {final_save_path}")
                count += 1 # 檔名序號加一
     

        except Exception as e:
            print(f"下載失敗: {e}")
            
    return A1A2_List



# ------------------------------------------------------------------
# 資料夾舊檔案判定,讀舊的csv,並存入df
# ------------------------------------------------------------------
def read_old_data_to_dataframe(full_path)->list:
    old_compare_list=[]
    if ".csv" in full_path:
        df=pd.read_csv(full_path,encoding="utf-8-sig")
    elif ".zip" in full_path:
        with zipfile.ZipFile(full_path,'r') as z:
           for info in z.infolist():
                try:
                    real_name = info.filename.encode('cp437').decode('cp950')
                except:
                    real_name = info.filename
                if ("A1" in real_name or "A2" in real_name) and real_name.endswith('.csv'):
                    with z.open(info.filename) as f:
                        df = pd.read_csv(f, encoding='utf-8-sig')
                        print(f"已讀取檔案: {real_name}")
                else:
                    continue      
           
                if df is not None: #檢查機制,避免資料夾裡有不適zip,csv類型的檔案
                    old_compare_list.append(df)
            
    return old_compare_list
    

# ------------------------------------------------------------------
# log dataframe比較(新舊list),最後產出log
# -----------------------------------------------------------------
def compare_and_log(old_df_list, new_df_list):
    Logs_dir = os.path.join(SAVE_NEW_DATA_DIR,"Logs")
    Log_file = os.path.join(Logs_dir,"update_log.txt")
    os.makedirs(Logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(Log_file, "a", encoding="utf-8") as log:
        log.write(f"\n{'='*50}\n")#印分隔線
        log.write(f"檢查時間: {timestamp}\n")#印當下時間
        for i in range(len(old_df_list)):
            if old_df_list is None:
                log.write("紀錄: 第一次抓取資料。\n")
                log.write(f"初始資料量: {len(new_df_list[i])} 筆, 欄位數: {len(new_df_list[i].columns)}\n")
            else:
                # 檢查筆數差異
                if len(new_df_list[i])!=len(old_df_list[i]):
                    diff_count = len(new_df_list[i]) - len(old_df_list[i])
                    log.write(f"舊資料筆數: {len(old_df_list[i])}\n")
                    log.write(f"新資料筆數: {len(new_df_list[i])}\n")
                    log.write(f"新增筆數: {diff_count}\n")
                
                # 找出真正「內容不一樣」的資料
                # 假設 '序號' 或某個組合鍵是唯一的
                # 這裡用一個簡單的方法：找出 new_df 中不在 old_df 的部分
                new_records = pd.concat([old_df_list[i], new_df_list[i]]).drop_duplicates(keep=False)
                
                if not new_records.empty:
                    log.write(f"具體差異內容摘要:\n{new_records.head().to_string()}\n")
                else:
                    log.write("結果: 資料內容無變化。\n")
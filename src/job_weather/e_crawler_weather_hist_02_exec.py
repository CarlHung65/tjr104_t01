# 引入資料缺失的條件conditions_to_crawling，這些是待爬資料。
from e_crawler_weather_hist_01_checkmiss import conditions_to_crawling

# 引入瀏覽器驅動程式主控制器webdriver
from selenium import webdriver
# ChromeDriver服務管理器
from selenium.webdriver.chrome.service import Service
# 自動下載與 Chrome 版本匹配的 ChromeDriver
from webdriver_manager.chrome import ChromeDriverManager
# Chrome 瀏覽器啟動參數設定
from selenium.webdriver.chrome.options import Options
# 網頁元素定位
from selenium.webdriver.common.by import By
# 等待元素出現/可點擊
from selenium.webdriver.support.ui import WebDriverWait
# 等待條件判斷
from selenium.webdriver.support import expected_conditions as EC

import time  # 延遲執行，確保網頁載入與元素穩定
from pathlib import Path  # 設定檔案路徑為Path物件
import shutil  # 整理下載下來的csv之存放資料夾


def get_CODis_to_open_stn_list(driver: webdriver, wait: WebDriverWait) -> list:
    try:
        # Step 4-1 or 6-1: 進入網站
        driver.get("https://codis.cwa.gov.tw/StationData")

        # Step 4-2 or 6-2: 勾選自動雨量站
        stationC1_input = wait.until(
            EC.element_to_be_clickable((By.ID, "auto_C1")))
        driver.execute_script("arguments[0].click();", stationC1_input)

        # Step 4-3 or 6-3: 勾選自動氣象站
        stationC2_input = driver.find_element(By.ID, "auto_C0")
        stationC2_input.click()

        # Step 4-4 or 6-4: 勾選農業站
        stationC2_input = driver.find_element(By.ID, "agr")
        stationC2_input.click()

        # Step 4-5 or 6-5: 點開測站清單
        station_lst_btn = driver.find_element(
            By.CSS_SELECTOR, "#switch_display > button:nth-child(2)")
        station_lst_btn.click()
        time.sleep(4)
        return None
    except Exception as e:
        print(f"Error發生: {e}")
        return None


def crawler_CODis_existing_stn_list(driver: webdriver, wait: WebDriverWait) -> list:
    try:
        # Step 4-1 ~ 4-5: 造訪網站直到打開測站清單
        get_CODis_to_open_stn_list(driver, wait)

        # Step 4-6: 尋找有開放載點的測站。
        divs = driver.find_elements(By.XPATH,
                                    '//*[@id="station_table"]/table/tbody/tr/td[2]/div')
        stn_lst = [div.text for div in divs]
        return stn_lst

    except Exception as e:
        print(f"Error發生: {e}")
        return None


def crawler_CODis_to_dowload_data(driver: webdriver, wait: WebDriverWait, station_id: str,
                                  target_year: int, target_month: int, target_monthday: int,
                                  max_retry_time=3) -> None | str:
    """This function featuring crawlering from the website CODis by the 
    following actions: Get in the web page, 
    select the default observation station classes(自動雨量站、自動氣象站、署屬有人站、農業站), 
    find and download the weather data of certain station ID and datetime that should be given
    by the arguments. 
    If any exception is raised during the process, you can choose the maximal
    retry times. Otherwise, the default is THREE times."""

    print(
        f"---正在找尋並下載站號{station_id}: {target_year}-{target_month:02}-{target_monthday:02}的天氣資料---")
    for attempt in range(max_retry_time):
        print(f"\t第{attempt+1}次嘗試中.....")
        try:
            # Step 6-1 ~ 6-5: 造訪網站直到打開測站清單
            get_CODis_to_open_stn_list(driver, wait)

            # Step 6-6: 找到測站坐落在哪一個資料列，如果找到了，用Selenium driver定位該列最右側趨勢圖icon，並點擊
            xpath = f"//tr[.//div[contains(text(), '{station_id}')]]//div[i[contains(@class, 'fa-chart-line')]]"
            chartbtn = wait.until(
                EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", chartbtn)
            print("\t點擊chart鈕成功")
            time.sleep(2)

            # Step 6-7: 點開日期選單
            date_input_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[1]/label/div/div[2]/div[1]/input')))
            driver.execute_script("arguments[0].click()", date_input_btn)
            print("\t成功點開日期選單")

            # Step 6-8: 點開年份下拉式選單
            y_menu_xpath = "//div[contains(@class, 'vdatetime-popup__year')]"
            y_menu_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, y_menu_xpath)))
            driver.execute_script("arguments[0].click()", y_menu_btn)
            print("\t成功點開年份下拉式選單")

            # Step 6-9: 選擇年份
            y_xpath = f"//div[contains(@class, 'vdatetime-year-picker__item') and contains(text(), '{target_year}')]"
            year_select = wait.until(
                EC.element_to_be_clickable((By.XPATH, y_xpath)))
            driver.execute_script("arguments[0].click()", year_select)
            print(f"\t成功選到{target_year}年")

            # Step 6-10: 點開月日下拉式選單
            # 以下兩種xpath都定位得到。
            # submenu_xpath = '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[1]/label/div/div[2]/div[1]/div/div[2]/div[1]/div[2]'
            month_date_xpath = "//div[contains(@class, 'vdatetime-popup__date')]"
            month_date_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, month_date_xpath)))
            driver.execute_script("arguments[0].click()", month_date_btn)
            print(f"\t成功點開月日下拉式選單")

            # Step 6-11: 選擇月份
            m_xpath = f"//div[contains(@class, 'vdatetime-month-picker__item') and contains(text(), '{target_month}月')]"
            month_select = wait.until(
                EC.element_to_be_clickable((By.XPATH, m_xpath)))
            driver.execute_script("arguments[0].click()", month_select)
            print(f"\t成功選到{target_month}月")

            # Step 6-12: 選擇日期
            md_xpath = f"//div[contains(@class, 'vdatetime-calendar__month__day')]//span[contains(text(), '{target_monthday}')]"
            monthday_select = wait.until(
                EC.element_to_be_clickable((By.XPATH, md_xpath)))
            driver.execute_script("arguments[0].click()", monthday_select)
            print(f"\t成功選到{target_monthday}日")
            time.sleep(1)

            # step 6-13: 觸發下載
            csv_btn_xpath = '//*[@id="main_content"]/section[2]/div/div/section/div[5]/div[1]/div[2]/div'
            csv_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, csv_btn_xpath)))
            time.sleep(1)
            driver.execute_script("arguments[0].click()", csv_btn)
            print(f"\t下載成功!")

            return None

        except Exception as e:
            print(f"Error發生: {e}")
            print(
                f"\t下載失敗，無法下載到{target_year}-{target_month:02}-{target_monthday:02}。")
            if attempt == max_retry_time - 1:
                print(f"---嘗試{max_retry_time}次仍下載失敗，請做問題排解。---")
                error_log = f"{station_id}{target_year}-{target_month:02}-{target_monthday:02}"
                return error_log
            else:
                print(f"\t將嘗試第{attempt+2}次.......")


def e_crawler_weather_hist_02_exec(conditions_to_crawling: list,
                                   service, options, save_path: str | Path) -> None:

    # 呼叫盤點結果，確認conditions_to_crawling set是否非空集合。
    if conditions_to_crawling:
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        wait = WebDriverWait(driver, 10)

        count = 0
        error_log = []
        for condition in conditions_to_crawling:
            stn_id, yr, m, md = condition[0], condition[1], condition[2], condition[3]
            file_name_pattern = f"{stn_id}-{yr}-{m:02}-{md:02}*.csv"

            if list(save_path.rglob(file_name_pattern)):
                count += 1
                print(f"同名檔案已經存在，無需下載。目前累計找到{count}筆同名檔案")
            else:
                # 執行爬蟲任務
                result = crawler_CODis_to_dowload_data(
                    driver, wait, stn_id, yr, m, md, 1)
                if result:  # 如果下載成功會回傳None、反之回傳error string
                    error_log.append(result)
        print(
            f"任務結束，本次總共下載了{len(conditions_to_crawling) - len(error_log)}個檔案。")
        print(error_log)
        driver.close()
    else:
        print("無下載任何氣象觀測檔。")

    return None


def classify_and_move_csvfile_path(orign_dir: str | Path, dest_dir: str | Path) -> None:
    """""""""
    Classify and organize csv files into date-based directories by parsing filenames.
    Automatically creates date-structured subdirectories (dt=YYYY-MM-DD format)
    and copies observation station CSV files based on embedded station ID and
    date information in the filename pattern: "*?-YYYY-MM-DD*.csv"."""

    for csv_file in orign_dir.glob("*?-????-?*-?*.csv"):
        file_name = csv_file.name
        station_id, y, m, md = file_name.replace(
            ".csv", "").split("-")  # C0K400  2024 04  2 22.22
        md = md.split()[0]
        dest_sub_dir = dest_dir/f"dt={y}-{m:02}-{md:02}"
        dest_sub_dir.mkdir(parents=True, exist_ok=True)
        new_file_name = dest_sub_dir/file_name
        shutil.copy(csv_file, new_file_name)
        shutil.rmtree(str(orign_dir))  # 上雲時這裡不確定是否要改策略

    return None


if __name__ == '__main__':
    #  Step 1: 設定未來存檔資料夾路徑，上雲的時候這裡待更改，
    curr_dir = Path().resolve()
    save_path = curr_dir/"raw_csv"/"daily_weather_data"
    save_path.mkdir(parents=True, exist_ok=True)

    #  Step 2: 建立service & options物件
    service = Service(ChromeDriverManager().install())
    options = Options()

    #  Step 3: 指派step1下載路徑給options物件，上雲的時候這裡待更改，
    prefs = {"download.default_directory": str(save_path)}
    options.add_experimental_option("prefs", prefs)

    # Step 4: 主程式，執行爬蟲
    e_crawler_weather_hist_02_exec(
        conditions_to_crawling, service, options, save_path)

    # Step 5: 整理資料夾，依照日期分區dt=......，上雲時這裡要確認是否該更改
    source_dir = save_path
    destination_dir = curr_dir/"processed_csv"/"partitioned_weather_csv"
    classify_and_move_csvfile_path(source_dir, destination_dir)

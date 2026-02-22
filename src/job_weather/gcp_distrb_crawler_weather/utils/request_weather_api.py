import requests
from dotenv import load_dotenv
import os
from airflow.models import Variable
from airflow.exceptions import AirflowException


def request_weather_api(lats_str: str, lons_str: str,
                        start_date: str, end_date: str,
                        var_lst: list) -> dict | list | None:

    # url = "https://archive-api.open-meteo.com/v1/archive"
    # params = {
    #     "latitude": lat,
    #     "longitude": lon,
    #     "start_date": start_date,
    #     "end_date": end_date,
    #     "hourly": var_lst,
    #     "timezone": "Asia/Taipei"
    # }

    # 調用API
    print("Calling for API once getting api key......")

    # 呼叫API KEY
    load_dotenv()
    apikey = os.getenv("openmeteoapikey")
    url = "https://customer-archive-api.open-meteo.com/v1/archive"
    params = {"latitude": lats_str, "longitude": lons_str,
              "start_date": start_date, "end_date": end_date,
              "hourly": var_lst, "timezone": "Asia/Taipei",
              "apikey": apikey,
              }

    header = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"}
    try:
        print(f"發送請求中.......")
        print(f"Start date: {start_date}. End date: {end_date}")
        print(f"[緯度:{lats_str}, 經度{lons_str}].....")

        response = requests.get(
            url, params=params, timeout=120, headers=header)

        if response.status_code == 200:
            print(f"請求發送成功。")
            data = response.json()
            return data
        elif response.status_code == 429:
            print(f"Rate limit 429 reached at {lats_str}, {lons_str}")
            raise AirflowException("Rate limit hit 429!")

    except requests.exceptions.Timeout:
        print("response.status_code: ", response.status_code)
        raise AirflowException("Timeout!")
    except requests.exceptions.ConnectionError:
        print("response.status_code: ", response.status_code)
        raise AirflowException("Connection error!")
    except requests.exceptions.HTTPError as e:
        print("response.status_code: ", response.status_code)
        raise AirflowException("HTTP error!")
    except Exception as e:
        print("response.status_code: ", response.status_code)
        raise AirflowException("Unexpected error!")

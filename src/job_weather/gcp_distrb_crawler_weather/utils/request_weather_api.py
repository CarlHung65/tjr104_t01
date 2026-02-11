import requests
import pandas as pd
import time
from datetime import timedelta
from airflow.sdk import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def request_weather_api(lat: float | pd.Float64Dtype,
                        lon: float | pd.Float64Dtype,
                        start_date: str, end_date: str,
                        var_lst: list) -> dict | None:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": var_lst,
        "timezone": "Asia/Taipei"
    }
    header = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, params=params, timeout=30, headers=header)
        print(f"{lat}, {lon}發送ing。")
        if response.status_code == 200:
            print(f"{lat}, {lon}發送成功。{response.status_code}")
            return response.json()

        elif response.status_code == 429:
            print(f"Rate limit 429 reached at {lat}, {lon}")
            time.sleep(3600)
            return None  # raise AirflowException的話會殺掉整個迴圈，浪費重啟task retry額度。return None當作跳過這一輪
        else:
            print(f"HTTP {response.status_code} at {lat}, {lon}")
            return None  # 這區可能是來自API參數問題，也是直接跳過

    except requests.exceptions.Timeout:
        print(f"Timeout at {lat}, {lon}")
        return None
    # 其餘偏向連線問題則仰賴airflow retry機制，所以直接raise exception
    except requests.exceptions.ConnectionError:
        print(f"Connection error at {lat}, {lon}")
        raise AirflowException(f"Connection error at {lat},{lon}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error at {lat}, {lon}: {e}")
        raise AirflowException(f"HTTP error at {lat},{lon}: {e}")
    except Exception as e:
        print(f"Unexpected error at {lat}, {lon}: {e}")
        raise AirflowException(f"Unexpected error at {lat},{lon}: {str(e)}")

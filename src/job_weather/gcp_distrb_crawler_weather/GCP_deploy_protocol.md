## Flow chart
    ```
    VM1[11K] ──┐
    VM2[11K] ──┤ GCS (gs://your-bucket/weather/) (44K × 100KB Parquet)
    VM3[11K] ──┤
    VM4[11K] ──┘
        ↓
    VM5[合併] → VM6[MySQL Container, already exist]

    ```
1. GCP infrastructures (GCS、VM、Artifact registry、Service Account)

    1.1 Establish GCS bucket
    ``` 
        # 格式如下:
        gsutil mb gs://<Bucket_name>/

        # 範例如下:
        gsutil mb gs://tjr104-01_weather/
    ```

    1.2 (Optional) Establish Service Account(GCS + Artifact Registry + Compute Instance Admin(v1))
    ```
        # IAM → Service Accounts → Create
        # 為這個SA授予三權限角色：Storage Object Admin + Artifact Registry Reader + Compute Instance Admin (v1)

        # 下載 JSON，並重新命名，例如 tjr104-01_weather-user.json
        gcloud iam service-accounts create tjr104-01_weather-user
        gsutil iam ch serviceAccount:<SA name>@<project_id>.iam.gserviceaccount.com:objectAdmin gs://<bucket_name>/
    ```

    1.3 Create Five VM instances(或是到GCP console上創建5台)
    ```
        # 4台 Airflow VM + 1台合併VM
        for i in {1..5}; do
            gcloud compute instances create airflow3-vm$i \
                --machine-type=e2-medium \
                --zone=asia-east1-b \
                --image-project=ubuntu-os-cloud \
                --image-family=ubuntu-2510-minimal-amd64 \
                --tags=airflow3-vm \
                --service-account=<SA name>@<project_id>.iam.gserviceaccount.com
        done
    ```
    ** 備註: gcloud compute instances create 沒有指定 --boot-disk-type的話會
            自動使用 `pd-standard` (Standard Persistent Disk)

    1.4 進入VM
    ```
        gcloud compute ssh --zone "asia-east1-c" "airflow3-vm4" --project "watchful-net-484213-s5" --tunnel-through-iap
    ```
    
2. Install Docker
    ```
        # SSH 到每台 VM ，個別執行:
            sudo apt-get update 
            sudo apt-get install -y docker.io
            sudo systemctl start docker
            sudo systemctl enable docker
    ```
    ```
        # 確認docker安裝成功
            sudo usermod -aG docker lucky460721_gmail_com && \
            newgrp docker && \
            docker run hello-world
        # 再做一次
            docker run hello-world
    ```
3. Prepare several .py files where sharding the DAG runs to four VM instances VM1-VM5.

4. Push local Airflow3 image to GCP artifact registry
    ```
        # 準備Dockerfile: apache-airflow[google], google-cloud-storage, pyarrow, pandas[gcs], pymysql...

        # 準備.dockerignore: *.json, .env, __pycache__/, .git

        # 本地執行
            # 包成image，最後面的 空格. 不可忘(docker build -t <image名>:<image版號>)，範例：
            docker build -t airflow3-weather-etl-image:latest .
            # macbook用arm64的話最好用這個來包image
            docker build --platform linux/amd64 -t airflow3-weather-etl-image:latest .
            
            # 加上完整路徑改名 (docker tag <上一行取好的image名>:<image版號> <GCP artifact registry path>/<上一行取好的image名>:<image版號>)，範例：
            docker tag airflow3-weather-etl-image:latest asia-east1-docker.pkg.dev/watchful-net-484213-s5/tjr104-01-weather/airflow3-weather-etl-image:latest
            
            # 以SA或個人帳密登入gcloud
            gcloud config set account <xxxx@gmail.com>
            
            # 幫VM上的docker取得可以使用artifact registry的權限
            gcloud auth configure-docker asia-east1-docker.pkg.dev

            # 推送到artifact registry (docker push <GCP artifact registry path>/<上一行取好的image名>:<image版號>)，範例：
            docker push asia-east1-docker.pkg.dev/watchful-net-484213-s5/tjr104-01-weather/airflow3-weather-etl-image:latest         
    ```

5. Network Settings
    5.1 查詢各 VM IP
    ```
        gcloud compute instances list

        # 得到內部 IP（ETL 用）→ Airflow 連 MySQL
        # 範例輸出：
        # VM6-MySQL: 10.x.y.z
        # VM1-Airflow: 10.x.y.z
    
        # 也得到外部 IP（瀏覽器用）→ Airflow UI
        # 範例輸出：
        VM1: 34.123.x.x:8080
        VM2: 35.234.y.y:8080
        ...and so on.
    ```

6. Deploy MySQL in VM6 (already done)

7. 在VM1-VM5內部，各開立一份專案資料夾，所以要重複以下動作五次。
    ```
        # 流程說明：進入VM1/2/3/4/5 的終端機，逐行執行以下指令
        gcloud compute ssh指令 進入 VM/1/2/3/4/5

        # 創建資料夾，像跟地端的image一樣同步
        mkdir -p ~/airflow3-app/dags ~/airflow3-app/logs ~/airflow3-app/tasks ~/airflow3-app/utils ~/airflow3-app/data

        # 進入
        cd ~/airflow3-app
    ```
8. 在VM1-VM5的專案資料夾中，設立.env檔案各一份，來存放五台VM在運行自己docker containers時會使用到的相同資訊。所以要重複以下動作五次。
    ```
        # 流程說明：進入VM1/2/3/4/5 的終端機 -> 用 vim 指令編寫並存下.env檔案
        gcloud compute ssh指令 進入 VM1。
        cd airflow3-app
        輸入 vim .env。
        按下 i 進入編輯模式，把你在本機的.env內容複製並貼上。
        按 Esc，輸入 :wq 後按 Enter 存檔離開。

        # 檢查有存檔成功
        cat .env
    ```
    ```
        如果沒有vim指令，在vm的終端機中輸入
        sudo apt-get update && sudo apt-get install vim -y
    ```

9. Pull and start Airflow3 docker container in four VM instances (VM1-VM5)
    ```
        # 幫VM上的docker取得可以使用artifact registry的權限，
        # 格式如下：
        glcoud auth configure-docker <GCP artifact repository path>
        # 範例：
        gcloud auth configure-docker asia-east1-docker.pkg.dev

        # Docker pull
        # 格式如下：
        docker pull <GCP artifact registry路徑名>/<Image名稱>:<image版號>
        # 範例：
        docker pull asia-east1-docker.pkg.dev/watchful-net-484213-s5/tjr104-01-weather/airflow3-weather-etl-image:latest

        # Docker Run
        docker run -d \
            --name airflow-vm1 \ # 第二台就寫airflow-vm2、....以此類推
            --env-file .env \
            -p 8080:8080 \
            -v $PWD/dags:/opt/airflow/dags \
            -v $PWD/logs:/opt/airflow/logs \
            -v $PWD/tasks:/opt/airflow/tasks \
            -v $PWD/utils:/opt/airflow/utils \
            -v $PWD/data:/opt/airflow/data \ # 預備而已，基本上數據會直接流向GCS然後MySQL
            -e GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcs-key.json \ #如果是免金鑰模式，這行不用，一樣也能從docker container連到GCS。
            -e PYTHONPATH=/opt/airflow \ # 確保python腳本的根目錄等於opt/airflow，才不會importModuleError
            -e AIRFLOW__PROXIES__PASSWORD=<自定義固定密碼> \ #將airflow3 UI帳密固定
            -e _AIRFLOW_WWW_USER_PASSWORD=<自定義固定密碼> \ #將airflow3 UI帳密固定
            -e VM_ID=1 \ # 此時就會在.env中增加一個變數叫做VM_ID，值為1，實際上得根據VM編號不同來修改值。
            -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
            <GCP artifact registry路徑名>/<Image名稱>:<image版號> \
            airflow standalone
    ```
10. 進入docker container，測試 Container 內部是否真的能看到 GCS
    ```
        docker exec -it <VM名稱> airflow connections add 'google_cloud_default' \
        --conn-type 'google_cloud_platform'
        
        docker exec -it <VM名稱> python3 -c "from airflow.providers.google.cloud.hooks.gcs import GCSHook; print(GCSHook().list('<bucket_名稱>'))"
    ```

11. Firewall settings
    ```
        # 制定防火牆規則，5台vm只需一起在mac終端機執行一次就好:
        gcloud compute firewall-rules create allow-airflow-gui \
        --project=<專案id> \
        --direction=INGRESS \
        --priority=1000 \
        --network=default \
        --action=ALLOW \
        --rules=tcp:8080 \
        --source-ranges=0.0.0.0/0 \      
        --target-tags=airflow3-vm
    ```
    ```
        # 將這個防火牆設定個別在vm1(也是在mac終端機執行)，每台個別執行
        gcloud compute instances add-tags <VM名稱> \
        --tags=airflow3-vm \
        --zone=asia-east1-c \
        --project=watchful-net-484213-s5
    ```

12. 測試是否能進入airflow3 UI介面（登入後看到空的dag很正常）
    ```
        # 啟動airflow-vm1上的airflow3-tjr104 docker container後，查看airflow-vm1日誌：
            docker logs -f airflow-vm1

        # 如果看到 Airflow 成功啟動並顯示 Web UI 已就緒，就可以直接在瀏覽器輸入以下網址登入看結果了。
            http://[VM1_External_IP]:8080 

        # 尋找airflow UI登入密碼（帳號一概為admin），路徑一定是/opt/airflow/下面的。例如：
            docker exec -it <VM名稱> cat /opt/airflow/standalone_admin_password.txt
            或
            docker exec -it <VM名稱> cat /opt/airflow/simple_auth_manager_passwords.json.generated
            若找不到，確認是否已經進入了docker container還是是退出狀態，已經進入的話就只需要寫cat＋後面的路徑檔字串。如果還是找不到則
                cd /opt/airflow/
                ls
                ....找看看可能在哪邊。
    ```
13. 在vs code或mac terminal進入專案資料夾，開始把開發期間.py檔搬運到vm1的專案資料夾，然後因為前面docker run -v的關係，搬進去的專案資料夾又會投影到/opt/airflow/下的同名資料夾。
    ```
        # 一次把dags、tasks、utils三個資料夾同步過去
        # 格式如下：
        gcloud compute scp --recurse ./dags ./tasks ./utils <個人gcp_username>@<VM名稱>:~/<VM中專案資料夾名稱>/ --project=<專案id> --zone=<zone名稱> --tunnel-through-iap

        # 範例如下：
        gcloud compute scp --recurse ./dags ./tasks ./utils lucky460721@airflow3-vm1:~/airflow3-app/ --project=watchful-net-484213-s5 --zone=asia-east1-c --tunnel-through-iap
    ```


14. 刷新airflow UI介面，確保airflow scheduler有成功地自動掃描 /opt/airflow/dags。
    ```
        # 進入vm
        gcloud compute scp --recurse ./dags ./tasks ./utils <個人gcp_username>@<VM名稱>:~/<VM中專案資料夾名稱>/ --project=<專案id> --zone=<zone名稱> --tunnel-through-iap
        
        # 測試是否importError
        docker exec -it <容器名稱> airflow dags reserialize
    ```
    ```
        sudo chmod -R 777 ~/airflow3-app/dags
        sudo chmod -R 777 ~/airflow3-app/tasks
        sudo chmod -R 777 ~/airflow3-app/utils
        sudo chmod -R 777 ~/airflow3-app/logs
    ```

15. 測試能否從container連進Mysql container
    ```
        nc -zv <mysql container internal ip> 3306
    ```

16. ui介面啟動dag run

17. 如果中途需要用vs code改程式碼、debug
    17.1 地端搬到vm磁碟機(見第13點)
        ```
            gcloud compute scp --recurse ./dags ./tasks ./utils lucky460721@airflow3-vm1:~/airflow3-app/ --project=watchful-net-484213-s5 --zone=asia-east1-c --tunnel-through-iap
        ```

    17.2 vm磁碟機搬到docker container內部
        ```
            # 同步整個 dags 資料夾
            docker cp ~/airflow3-app/dags/. airflow-vm1:/opt/airflow/dags/

            # 同步整個 tasks 資料夾
            docker cp ~/airflow3-app/tasks/. airflow-vm1:/opt/airflow/tasks/
            
            # 強制airflow重新掃描dags
            /opt/airflow$ airflow dags reserialize
        ```
    17.3 去ui刷新網路頁面

## Supplementary
1. 讓airflow與GCS連結

    1.1  GCSHook 是 Apache Airflow 中用來操作 Google Cloud Storage (GCS) 的標準 Hook，主要透過 Airflow Connection 管理 GCP 認證。主要用途：GCSHook 提供 GCS bucket 的完整 CRUD 操作，包括上傳、下載、列出物件和生成簽名 URL。它使用  gcp_conn_id  參數連結到 Airflow 的 Google Cloud Connection，無需在程式碼中硬編碼服務帳戶金鑰。
    ```
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        # 初始化 Hook (使用預設 google_cloud_default connection)
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

        # 上傳本地檔案到 GCS
        gcs_hook.upload(
            bucket_name='my-data-bucket',
            object_name='etl/output.parquet', # GCS 中的目標位置
            filename='/local/path/data.parquet' # 本地來源檔案
        )

        # 下載 GCS 檔案
        gcs_hook.download(
            bucket_name='my-data-bucket', 
            object_name='raw/input.csv', # GCS 中的來源檔案
            filename='/local/download/input.csv' # 本地儲存位置
        )

        # 列出 bucket 中的檔案
        files = gcs_hook.list(
            bucket_name='my-data-bucket',
            prefix='etl/2026/'  # 只列出特定前綴
        )

    ```
    1.2 若整合到task中
    ```
        from airflow.sdk import task
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        @task
        def process_and_upload(**context):
            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            撰寫ETL 邏輯...
            gcs_hook.upload(
                bucket_name='{{ params.bucket }}',
                object_name='processed/{{ ds }}/data.parquet',
                filename='/tmp/processed.parquet'
            )
    ```

2. 本專案VM上的airflow container 啟動容器 - Docker run範例:
```
    docker run -d \
    --name airflow-vm4 \
    --env-file .env \
    -p 8080:8080 \
    -v $PWD/dags:/opt/airflow/dags \
    -v $PWD/logs:/opt/airflow/logs \
    -v $PWD/tasks:/opt/airflow/tasks \
    -v $PWD/utils:/opt/airflow/utils \
    -v $PWD/data:/opt/airflow/data \
    -e PYTHONPATH=/opt/airflow \
    -e AIRFLOW__PROXIES__PASSWORD=<自定義密碼> \
    -e _AIRFLOW_WWW_USER_PASSWORD=<自定義密碼> \
    -e VM_ID=4 \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
    asia-east1-docker.pkg.dev/watchful-net-484213-s5/tjr104-01-weather/airflow3-weather-etl-image:latest \
    airflow standalone
```
# Flow chart
- Prequisities [Chechk which gcloud accounts being activated](#0-check-config-of-currently-activated-gcloud-accounts-and-projects-in-your-local-site)
- Step 1. [Establish GCS bucket on GCP](#1-establish-gcs-bucket-on-gcp)
- Step 2. [Establish your own artifact registry](#2-establish-your-own-artifact-registry)
- Step 3. [Establish  Service Account for bucket and artifact registry](#3-establish-service-accountto-grant-with-gcs--artifact-registry)
- Step 4. [Create a VM instance](#4-create-a-vm-instance)
- Step 5. [Initiate new firewall rule which uses IAP](#5-setting-of-firewall-rule-using-iap)
- Step 6. [Test the connection with VM (not visit airflow docker container yet)](#6-enter-the-created-vm-instance-through-iap-tunnel-not-try-to-visit-airflow-ui-yet)
- Step 7. [Install docker in VM](#7-install-docker-in-vm)
- Step 8. [Drafting the dags in .py scripts for your ETL logics](#8-on-the-local-end-prepare-several-py-files-describing-your-dags-and-tasks-as-per-your-scope-of-data-pipeline)
- Step 9. [(Optional, suitable for Production) Push a customized airflow3 image from local to GCP artifact registry](#9-optional-suitable-for-production-push-customized-airflow3-image-from-local-to-gcp-artifact-registry)
- Step 10. [Create new project folders in VM](#10-create-project-folders-inside-vm)
- Step 11. [Create .env file in VM](#11-initiate-env-file-in-vm)
- Step 12. [(Optional, suitable for Production) Pull Airflow3 docker container in VM](#12-optional-suitable-for-production-pull-airflow3-docker-container-in-vm)
- Step 13. [Start an airflow3 docker container](#13-start-an-airflow3-docker-container)
- Step 14. [Visit airflow UI](#14-enter-vm-and-try-to-visit-airflow-ui-after-start-airflow-docker-container)
- Step 15. [Create connection between airflow container and GCS bucket](#15-create-and-test-connection-btw-the-container-in-the-vm-and-the-gcs-bucket)
- Step 16. [(Optional, suitable for Development) Debugging,adding/updating existing dags](#16-optional-suitable-for-development-debuggingaddingupdating-existing-dags)
- Step 17. [Check dag is serialized correctly](#17-check-dag-serialized-correctly)
- Step 18. [Test connection bewtween airflow conatiner and mysql container](#18-check-airflow-container-can-read-mysql-database-in-another-vmcontainer-in-the-same-gcp-project)
- Step 19. [Trigger a new dag run on airflow3 UI](#19-trigger-a-new-dag-run-on-ui)
- Step 20. [(Optional) Debugging if airflow container is running.](#20-optional-revise-codes-debug-when-needed)

# 0. Check config of currently activated gcloud accounts and projects in your local site.
- 在本地terminal中確認目前active帳號是否有切為專案相關人士:
    ```
        gcloud auth list # 會跳出這台電腦上目前有哪一個帳號是被gcloud記住且active中
        
        # 如果該帳號不是屬於專案相關人士，就執行下一行來切換
        gcloud config set account <帳號@gmail_com>

    ```
# 1. Establish GCS bucket on GCP
- 若在GCP console上操作: 進入cloud storage -> create new bucket
- 若在本地端terminal操作:
    ```
        # 格式如下:
        gsutil mb gs://<Bucket_name>/
        # 範例如下:
        gsutil mb gs://tjr104-01_weather/
    ```
# 2. Establish your own artifact registry
- GCP console -> artifact registry -> ....

# 3. Establish Service Account (to grant the access to GCS and Artifact Registry)
- 專案層範圍的設定: 在GCP console上操作: 進入IAM → Service Accounts → Create → `為這個SA授予權限Artifact Registry Reader
- (不使用GCP VM，而是本機連GCS才需要做這步)承上，建立SA後，下載JSON，並重新命名，例如 tjr104-01_weather-user.json，此檔絕對不可以外流出去。

- bucket層的設定: 設定這個SA能對哪個`特定`GS bucket有讀取、上傳物件、列出bucket內容的權限。
    ```
        # 格式如下：
        gsutil iam ch serviceAccount:<SA name>@<project_id>.iam.gserviceaccount.com:objectCreator gs://<Step 1新增的bucket_name>/

        gsutil iam ch serviceAccount:<SA name>@<project_id>.iam.gserviceaccount.com:objectAdmin gs://<Step 1新增的bucket_name>/

        gsutil iam ch serviceAccount:<SA name>@<project_id>.iam.gserviceaccount.com:LegacyBucketReader gs://<Step 1新增的bucket_name>/

        # 範例如下：
        gsutil iam ch serviceAccount:practice-etl-by-airflow@causal-inquiry-12345678.x9.iam.gserviceaccount.com:objectViewer gs://practice-airflow3-0219
    ```
    ** 如果是在IAM那裡一口氣設定了Storage Object Viewer+Storage Legacy Bucket Reader+Storage Object Creater+Artifact Registry Reader四個權限，那權限範圍會變成專案層級，使得此帳號會有權create、view所屬專案下的`所有`bucket、而非單一bucket，並且專案層的設定會覆蓋bucket層的設定。

# 4. Create a VM instance
- 可在GCP console或terminal中操作，擇一執行。
- 在GCP console上操作: 進入Computer Engine -> VM instances -> Create instance -> 設定instance name、region、zone、machine-type、disk、os、network tags、service account
- 在本機terminal上操作:
    ```
        # 格式如下:
            gcloud compute instances create <VM名稱> \
                --machine-type=<CPU規格>\
                --zone=<機房位置> \
                --image-project=ubuntu-os-cloud \
                --image-family=<作業系統規格> \
                --tags=<自定義VM的網路標記> \
                --service-account=<SA_name>@<gcp_project_id>.iam.gserviceaccount.com

        # 範例如下:
            gcloud compute instances create airflow3-vm$i \
                --machine-type=e2-medium \
                --zone=asia-east1-b \
                --image-project=ubuntu-os-cloud \
                --image-family=ubuntu-2510-minimal-amd64 \
                --tags=airflow3-vm \
                --service-account=<SA name>@<your_gcp_project_id>.iam.gserviceaccount.com
    ```
    ** 備註: gcloud compute instances create 沒有指定 --boot-disk-type的話會
            自動使用 `pd-standard (Standard Persistent Disk)`

# 5. Setting of firewall rule (Using IAP)
- 可在GCP console或terminal中操作，擇一執行。
- 在GCP console中執行: firwall -> create firewall rule -> 開啟tcp port 22(SSH埠)、使用35.235.240.0/20 (Google IAP專用IP段)、入站(ingress)、此規則要套在哪種VM網路標記。

- 在本地terminal執行:
    ```
        gcloud compute firewall-rules create allow-iap-ssh2 \
        --project=<專案ID> \
        --direction=INGRESS \
        --priority=1000 \
        --network=default \
        --action=ALLOW \
        --rules=tcp:22 \
        --source-ranges=35.235.240.0/20 \
        --target-tags=<VM的網路標記>
    ```
    **VM網路標記可參照 [Step 4. Create VM instances](#4-create-vm-instances)
    ** 如果VM instance在生成時忘記加上網路標記，需追加執行這步: 新增VM的網路標記(在本地端終端機執行即可)
    ```
        gcloud compute instances add-tags <VM名稱> \
        --tags=<防火牆規則中的target-tags內容> \
        --zone=<機房名稱> \
        --project=<專案id>
    ```
- `務必執行`：將其他開放tcp port 22 的防火牆規則刪掉或停用，例如: 0.0.0.0/0這個網段。否則你的VM外部ip還是會被在公網看到而潛藏ssh被暴力破解的風險。
- `務必執行`：確定你登入用的gmail帳號持有IAP-secured Tunnel User權限

# 6. Enter the created VM instance through IAP tunnel (not try to visit airflow UI yet)
- 在本地termimal中執行以下，若卡住沒反應代表防火牆設定成功(只能走IAP tunnel)。
    ```
        # 格式如下:
        gcloud compute ssh --zone "<機房位置>" "<VM名稱>" --project "<專案ID>"

        # 範例如下:
        gcloud compute ssh --zone "asia-east1-c" "vm_with_airflow_container" --project "causal-inquiry-12345678-x9"
    ```
- 合法地在本地terminal中執行，
    ```
        # 格式如下:
            gcloud compute ssh --zone "<機房位置>" "<VM名稱>" --project "<專案ID>" --tunnel-through-iap

        # 範例如下:
            gcloud compute ssh --zone "asia-east1-c" "practice-gcp-airflow-0219" --project "causal-inquiry-484423-e7" --tunnel-through-iap
    ```
- 輸入密碼，此時透過該密碼，ssh私鑰會被自動用於個人身份認證，認證成功後就會VM內(terminal游標會發生變化)。認證成功的前提是你的gmail帳號在gcloud compute ssh前，這個專案持有`compute.osLogin`或更高權限(在IAM可查看到)`。並且enable-oslogin=TRUE(在Metadata可查看到)。如果沒有oslogin=True，可能是create VM instance的時候access scope只選到Allow default access。
    
# 7. Install Docker in VM
-   ssh+iap進入VM內，執行docker安裝程序:
    ```
        sudo apt-get update 
        sudo apt-get install -y docker.io
        sudo systemctl start docker
        sudo systemctl enable docker
    ```
- 緊接著確認docker有安裝成功
    ```
        sudo usermod -aG docker lucky460721_gmail_com && \
        newgrp docker && \
        docker run hello-world
    ```
- 通常做完上一步的第三句以後會靜悄悄，故再執行一次以下這句。
    ```
        docker run hello-world
    ```
# 8. On the local end, prepare several .py files describing your dags and tasks as per your scope of data pipeline.

# 9. (Optional, Suitable for Production) Push customized Airflow3 image from local to GCP artifact registry
- Draft Dockerfile, .dockerignore(to prevent pushing *.json, .env, __pycache__/, .git onto registry from credential information leakage)
- Dockerfile範例
    ```
        FROM apache/airflow:slim-3.1.7-python3.13
        USER root
        RUN apt-get update \
            && apt-get install -y --no-install-recommends git \
            && apt-get autoremove -yqq --purge \
            && apt-get clean \
            && rm -rf /var/lib/apt/lists/*
        ENV TZ=Asia/Taipei \
            AIRFLOW__CORE__LOAD_EXAMPLES=False \
            PYTHONPATH=/opt/airflow
        USER airflow
        COPY requirements.txt /
        RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
        COPY --chown=airflow:root dags/ /opt/airflow/dags
        COPY --chown=airflow:root tasks/ /opt/airflow/tasks
        COPY --chown=airflow:root utils/ /opt/airflow/utils/
        CMD ["airflow", "standalone"]
    ```

- 本地端`專案資料夾下的terminal`執行:
    ```
        # 包成image，最後面的 空格. 不可忘:
            docker build -f Dockerfile -t <image名>:<image版號>。
            # 範例：
            docker build -f Dockerfile -t airflow3-weather-etl-image:latest .

            # macbook用arm64的話最好用這個來包image，確保使用windows系統的團隊也可以正常速度啟動容器。
            docker build --platform linux/amd64 -f Dockerfile -t airflow3-weather-etl-image:latest .
        
        # 加上完整路徑改名:
            docker tag <上一行取好的image名>:<image版號> <GCP artifact registry path>/<上一行取好的image名>:<image版號>
            # 範例：
            docker tag airflow3-weather-etl-image:latest asia-east1-docker.pkg.dev/watchful-net-12345678-x9/tjr104-01-weather/airflow3-weather-etl-image:latest
        
        # 確認本機目前要執行的GCP專案是預期的專案
            gcloud config set project <專案ID>
        
        # 幫本機的docker取得: 可以上傳、下載artifact registry上的image之權限
            gcloud auth configure-docker <registry路徑>
            # 範例
            gcloud auth configure-docker asia-east1-docker.pkg.dev

        # 推送到artifact registry 
            docker push <GCP artifact registry path>/<image名>:<image版號>
            # 範例：
            docker push asia-east1-docker.pkg.dev/watchful-net-12345678-x9/tjr104-01-weather/airflow3-weather-etl-image:latest         
    ```


# 10. Create project folders in VM
- SSH進入VM terimnal (as per [step 6](#6-enter-the-created-vm-instance-through-iap-tunnel-not-try-to-visit-airflow-ui-yet))
- Create folders
    ```
        # 創建資料夾，最好跟地端一樣長相。
        mkdir -p ~/airflow3-app/dags ~/airflow3-app/logs ~/airflow3-app/tasks ~/airflow3-app/utils ~/airflow3-app/data

        # 進入
        cd ~/airflow3-app
    ```
# 11. Initiate .env file in VM.
- Enter the project folder. E.g, `airflow3-app` after step 9.
    ```
        cd <專案資料夾名稱>
    ```
- Enter `vim .env`
    ```
        # 如果沒有vim指令，在vm的終端機中輸入
        sudo apt-get update && sudo apt-get install vim -y
    ```
- Press `i`
- Typing/copy-paste the credential info during dag runs in .env file. It is not necessary to include some credentials related with google cloud services (e.g. GCS) in .env file because the ADC mechanism behind Google SDK will be able to check the where the credentials are by asking the service account granted to VM, and the service account has described what google cloud services can be accessed by this VM.

- Press `Esc` -> Enter `:wq` -> Press `Enter`
- Make sure .env has been created and saved.
    ```
        cat .env
    ```

# 12. (Optional, suitable for Production) Pull Airflow3 docker container in VM
- 在VM的terimal中執行:
    ```
        # 幫VM上的docker取得可以使用artifact registry的權限
            # 格式如下：
            glcoud auth configure-docker <GCP artifact repository path>
            # 範例：
            gcloud auth configure-docker asia-east1-docker.pkg.dev

        # Docker pull image
            # 格式如下：
            docker pull <GCP artifact registry路徑名>/<Image名稱>:<image版號>
            # 範例：
            docker pull asia-east1-docker.pkg.dev/watchful-net-12345678-x9/tjr104-01-weather/airflow3-weather-etl-image:latest

    ```
# 13. Start an airflow3 docker container
- 可以執行的指令大致上長這樣
    ```
    docker run -d \
    --name <容器名稱> \
    --env-file .env \
    -p 8080:8080 \
    -v $PWD/dags:/opt/airflow/dags \
    -v $PWD/logs:/opt/airflow/logs \
    -v $PWD/tasks:/opt/airflow/tasks \
    -v $PWD/utils:/opt/airflow/utils \
    -v $PWD/data:/opt/airflow/data \ # 預備而已，基本上數據會直接流向GCS
    -e GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcs-key.json \ #如果是免金鑰模式，這行不用，一樣也能從docker container連到GCS。
    -e PYTHONPATH=/opt/airflow \ # 確保python腳本的根目錄等於opt/airflow，才不會importModuleError
    -e AIRFLOW__PROXIES__PASSWORD=<自定義固定密碼> \ #將airflow3 UI帳密固定
    -e _AIRFLOW_WWW_USER_PASSWORD=<自定義固定密碼> \ #將airflow3 UI帳密固定
    -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
    <GCP artifact registry路徑名>/<Image名稱>:<image版號> \
    airflow standalone
    ```
- 如果`有`執行過Step 9 & Step 12
    ```
        # 範例:
        docker run -d \
        --name airflow-vm4 \
        --env-file .env \
        -p 8080:8080 \
        asia-east1-docker.pkg.dev/watchful-net-12345678-x9/tjr104-01-weather/airflow3-weather-etl-image:latest \
        airflow standalone
    ```
- 如果`沒有`執行過Step 9 & Step 12
     ```
        # 範例:
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
        -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
        apache/airflow:slim-3.1.7-python3.13 \
        airflow standalone
    ```

# 14. Enter VM and try to visit airflow UI after start airflow docker container
- ctrl + D to log out VM if you are still inside VM. 
- Enter VM again but using IAP tunnel + port forwarding
    ```
        # 格式
        gcloud compute ssh --zone "<zone名>" "<VM名>" --project"<專案ID>" --tunnel-through-iap --ssh-flag="-L 8080:localhost:8080
        # 範例
        gcloud compute ssh --zone "asia-east1-c" "practice-gcp-airflow-0219" --project "causal-inquiry-12345678-x9" --tunnel-through-iap --ssh-flag="-L 8080:localhost:8080"
    ```
- 瀏覽器打開，輸入http://localhost:8080，成功進入登入頁面，代表Web UI就緒且連線成功！
- 尋找airflow UI登入帳號與密碼
    ```
        # 帳密路徑一定是/opt/airflow/下面的。例如：
        docker exec -it <容器名稱> cat /opt/airflow/standalone_admin_password.txt
        或
        docker exec -it <容器名稱> cat /opt/airflow/simple_auth_manager_passwords.json.generated
        若找不到，確認是否已經進入了docker container還是是退出狀態，已經進入的話就只需要寫cat＋後面的路徑檔字串。如果還是找不到則
            cd /opt/airflow/
            ls
            ....找看看可能在哪邊。
    ```
- 取得帳密後即可登入UI

# 15. Create and test connection btw the container in the VM and the GCS bucket
- 安裝airflow-providers套件
    ```
        docker exec -it <容器名稱> /bin/bash
        pip install "apache-airflow-providers-google>=10.1.0"
    ```
- 重啟容器
    ```
        docker restart <容器名稱>
    ```
- 設定airflow connections參數 (此舉也可以在airflow UI上設定): 
    ```
        docker exec -it <容器名稱> airflow connections add 'google_cloud_default' \
        --conn-type 'google_cloud_platform'
    ```    
- 測試與GCS連線成功，有權讀到bucket中的內容物，如果bucket下方沒有檔案，會回傳[]:
    ```
        docker exec -it <容器名稱> python3 -c "from airflow.providers.google.cloud.hooks.gcs import GCSHook; print(GCSHook().list('<bucket_名稱>'))"
    ```

# 16. (Optional, suitable for Development) Debugging,adding/updating existing dags
- 進入本地端terminal專案資料夾，開始把開發期間.py檔搬運到vm的專案資料夾，然後因為前面docker run -v的關係，搬進去的專案資料夾又會投影到/opt/airflow/下的同名資料夾。
    ```
        # 一次把dags、tasks、utils三個資料夾同步過去
        # 格式如下：
        gcloud compute scp --recurse ./dags ./tasks ./utils <個人gcp_username>@<VM名稱>:~/<VM中專案資料夾名稱>/ --project=<專案id> --zone=<zone名稱> --tunnel-through-iap

        # 範例如下：
        gcloud compute scp --recurse ./dags ./tasks ./utils lucky460721@airflow3-vm1:~/airflow3-app/ --project=causal-inquiry-123455678-x9 --zone=asia-east1-c --tunnel-through-iap
    ```
        
# 17. Check DAG serialized correctly.
- IAP tunnel + forwarding port進入VM terminal
- 序列化
    ``` 
        # 測試是否importError
        docker exec -it <容器名稱> airflow dags reserialize
    ```
    ```
        # 如果權限不足，可使用以下，但production不建議
        sudo chmod -R 777 ~/airflow3-app/dags
        sudo chmod -R 777 ~/airflow3-app/tasks
        sudo chmod -R 777 ~/airflow3-app/utils
        sudo chmod -R 777 ~/airflow3-app/logs
    ```
- 刷新airflow UI介面，確保airflow scheduler有成功地自動掃描 /opt/airflow/dags。

# 18. Check airflow container can read MySQL database in another VM/container in the same GCP project.
    ```
        nc -zv <mysql container internal ip> 3306
    ```

# 19. Trigger a new dag run on UI.

# 20. (Optional) Revise codes, debug when needed
- 地端搬到vm磁碟機
    ```
        # 整個資料夾複製
        gcloud compute scp --recurse ./dags ./tasks ./utils lucky460721@airflow3-vm1:~/airflow3-app/ --project=watchful-net-484213-s5 --zone=asia-east1-c --tunnel-through-iap

        # 只複製一個檔案
        gcloud compute scp <本地端檔案路徑> lucky460721@<vm名稱>:<vm資料夾路徑> --project=watchful-net-484213-s5 --zone=asia-east1-c --tunnel-through-iap
        範例：
        gcloud compute scp ./utils/request_weather_api.py lucky460721@airflow3-vm1:~/airflow3-app/utils/ --project=watchful-net-484213-s5 --zone=asia-east1-c --tunnel-through-iap
        ```

- VM磁碟機搬到docker container內部
    ```
        docker cp <vm bind路徑> <airflow-container名稱>:<airflow內mount路徑> 
        # 範例：同步整個 dags 資料夾
        docker cp ~/airflow3-app/dags/. airflow-vm1:/opt/airflow/dags/

        # 範例：同步整個 tasks 資料夾
        docker cp ~/airflow3-app/tasks/. airflow-vm1:/opt/airflow/tasks/
                    
    ```
- 清除快取
    ```
        docker exec -it <airflow-container名稱> find /opt/airflow/<資料夾名> -name "*.pyc" -delete
    ```
- Refresh UI

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
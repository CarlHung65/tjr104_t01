## 目錄
1. [Project Description](#1-project-description-專案簡述)
2. [Architecture](#2-architecture-專案架構)
3. [Tech. stack](#3-tech-stack技術堆疊)
4. [Key overcomse](#4-key-overcomes關鍵突破)
5. [DEMO](#5-demo)
6. [Setup](#6-setup-commands)


## 1. Project Description 專案簡述
A data pipeline that integrates traffic accidents, weather data, and risk index (PDI) to analyze and identify pedestrian risk hotspots around night markets in Taiwan. 
透過整合全台夜市地理資訊、歷年車禍事故數據與即時天氣觀測三地異質資料源，建立一套用數據驅動的交通風險量化評估系統 (PDI)，協助政府與大眾探查與識別「臺灣行人地獄」中的高風險熱區。

## 2. Architecture 專案架構
- Step 1 (Extraction): 
    - Web scraping from the four data sources: Wikipedia, Google Maps Place API, 政府資料開放平臺(https://data.gov.tw) and OpenMeteo API. Saving the sourced data in STAGING layers.  
    從Wikipedi爬取夜市名單、Google Maps Place API獲取夜市座標、政府資料開放平臺(https://data.gov.tw)取得事故紀錄、Open-Meteo API請求天氣觀測資料。
- Step 2 (Transformation): 
    - Using Pandas to clean/transform the 1.5 million ~ 1 billion data rows. Among them, filtering and keeping the practical attributes/parameters by general knowledge so as to alleviate the burden on disk/RAM/processor.  
    使用 Pandas 處理百萬級～億級的資料清洗，善用現實世界嘗試篩選有意義的特徵資料，避免一味放入資料空耗硬體資源。
- Step 3 (Loading): 
    - Streamlining the cleaned data from STAGING LAYERS (bronze) to DATA LAYER (silver) by using RDBMS/MySQL. The schema of tables in MySQL server is predetermined by the concept of ER models (Link to Database model: https://dbdiagram.io/d/traffic_accidents-6979e356bd82f5fce2e07c36).  
    梳理清洗完成後的數據，並存入關聯式資料庫MySQL，正式轉為DATA LAYER。資料庫中的資料模型採用ER models的概念 (資料庫模型連結: https://dbdiagram.io/d/traffic_accidents-6979e356bd82f5fce2e07c36)
- Step 4 (Serving):
    - Querying data in MySQL server and then calculating them to find the spatial relations between traffic accidents and certain night markets. The resulted tables after data aggregation are considered MART LAYER and saved in MySQL or Redis (another NoSQL DBMS).  
    查詢MySQL資料庫，執行空間聚合、統計運算得到夜市與車禍關聯性，取得之分析成果轉為MART層實體資料表存放於MySQL或Redis。
    - The tables are converted to graphs by Tableau, Streamlit/Altair and Plotly. Users could navigate the dashboards in the web-pages based on Streamlit.  
    前端以 Streamlit 呈現，並串接 Tableau 進行深度分析。
- Step 5 (Dockerize and cloud deploy):
    - Deploying the service in a virtual machine on GCP (google cloud platform) with dockerization.  
    將上述服務部署至雲端GCP虛擬機，搭配容器化措施簡化啟動流程。

## 3. Tech. Stacks 技術堆疊
- Environment: Docker, GCP Compute Engine
- Processing: Python (Pandas, Streamlit)
- Orchestration: Airflow 3
- Storage: GCS, MySQL, Redis
- Dashboard: Tableau
- Code management: SSH key + GitHub

## 4. Key overcomes 關鍵突破
- Risk of deadlock when loading > 4 GB, 1 billion data rows in a single server.
    - Q: 1 billion rows of weather data led to huge I/O burden and the concern about deadlock when query/join.
    - A: Corrected the business requirements (true user demands), and simplified the number of locations really needed. Splitting TL tasks to multiple batches to mitigate the I/O rate in each connection.
        
- *>>* 1000 traffic accident spots made slow reaction in web service.
    - Q: There were > 1.5 million raw spots being shown in the map, causing OOM.
    - A: Reduced the precision of geometry and used heatmap/MarkerCluster rather than markers. After that, only 10 thousand were processed.

- Process of spatial calculation delayed the response of Map.
    - Q: Spatial calculation in real time led to slower response to frontend.
    - A: Added NoSQL as CACHE layer, compressed the pre-processed table in binary type. Used TTL to protect the data from being obsolete.

## 5. Demo
- Demo video (https://www.youtube.com/watch?v=FF9iNqWO9xw)
- ![Example of pages](/images/streamlit_pages_example.png)

## 6. Setup commands
- After git clone, initiate new  `.env` file in the project root directory. Fill in your own credential information after  `=` .
    ```
        # ===== MySQL =====
        MYSQL_ROOT_PASSWORD=
        MYSQL_DATABASE=
        MYSQL_USER=
        MYSQL_PASSWORD=

        # ===== Airflow Admin =====
        AIRFLOW_ADMIN_USER=
        AIRFLOW_ADMIN_PASSWORD=
        AIRFLOW_ADMIN_EMAIL=
        AIRFLOW_SECRET_KEY=

        # ===== Airflow =====
        AIRFLOW_UID=50000
        AIRFLOW__CORE__EXECUTOR=LocalExecutor
        AIRFLOW__CORE__LOAD_EXAMPLES=False
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://
        AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
        AIRFLOW__DAG_PROCESSOR_MANAGER__DAILY_MAX_RUNS=1
        AIRFLOW__CORE__PARALLELISM=8
        AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
        AIRFLOW__CORE__DAG_CONCURRENCY=4
        AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=16
        AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
        AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE=1800

        # ===== Redis =====
        REDIS_HOST=localhost
        REDIS_PORT=6379
        REDIS_PASSWORD=
    ```
    
- Start the docker containers
    ```
        docker compose up -d # start the containers, MySQL, AirFlow, Redis

        docker exec -it /bin/bash cat simple_auth_manager_passwords.json.generated # then you will get the accounts and passwords

        # visit and loging http://localhost:18080

        # trigger the DAGs to initiate the ETL to write the data to MySQL server container.
    ```
- Follow the [setup.sh](tjr104_t01/setup.sh) to initiate the env. for frontend. Perform this only once.
- Active the env.
    ```
        source ~/.bashrc 
        
        # if not work on AppleMac, try:
        source ~/.zshr
    ```
- then start up the web service.
    ```
        st-start
    ```
- If visit the web page next time, just do ```st-start``` again.



*README.md authored by Jessie-Lin-810630, Mar. 30, 2026*
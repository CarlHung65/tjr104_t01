FROM apache/airflow:slim-3.1.7-python3.13
USER root
RUN apt-get update \
    && apt-get install -y zsh \
    && echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" \
    && apt-get install -y --no-install-recommends vim git curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Taipei \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    PYTHONPATH=/opt/airflow

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
COPY --chown=airflow:root dags/d_weatherapi_keep_refresh_this_year.py /opt/airflow/dags
COPY --chown=airflow:root dags/d_weatherapi_trace_historical_year.py /opt/airflow/dags
COPY --chown=airflow:root tasks/__init__.py /opt/airflow/tasks/
COPY --chown=airflow:root tasks/e_crawler_weather_gcp_refactor.py /opt/airflow/tasks/
COPY --chown=airflow:root tasks/l_load_to_mysql_gcp.py /opt/airflow/tasks/
COPY --chown=airflow:root utils/ /opt/airflow/utils/
CMD ["airflow", "standalone"]
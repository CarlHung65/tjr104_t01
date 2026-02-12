FROM apache/airflow:3.0.3-python3.11


USER root
RUN apt-get update -y
RUN apt-get install curl vim wget procps git -y
RUN apt-get install -y zsh \
    && echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

USER airflow
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
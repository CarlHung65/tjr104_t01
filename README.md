## Environment Variables

1. You need to create `.env` file in the project root directory.
2. Fill in the required values after `=`.
```env
# ===== MySQL =====
MYSQL_ROOT_PASSWORD=
MYSQL_DATABASE=
MYSQL_USER=
MYSQL_PASSWORD=

# ===== Airflow Admin =====
AIRFLOW_ADMIN_USER=
AIRFLOW_ADMIN_PASSWORD=
AIRFLOW_ADMIN_EMAIL=

# ===== Airflow =====
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

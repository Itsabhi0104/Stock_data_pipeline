# Stock-pipeline (Airflow + Postgres)

## Quick start
1. Build image:
   docker compose build

2. Initialize Airflow DB and create admin user:
   docker compose up airflow-init

3. Start services:
   docker compose up -d

4. Open Airflow UI: http://localhost:8080 (admin/admin)

5. Trigger DAG `daily_stock_prices` and check Postgres:
   psql -h localhost -U postgres -d stocksdb -c "select * from stock_price;"

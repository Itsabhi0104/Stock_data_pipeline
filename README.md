# Stock Data Pipeline (Airflow + PostgreSQL)

A robust, automated stock data pipeline that fetches real-time stock prices and stores them in PostgreSQL using Apache Airflow for orchestration. The pipeline supports multiple data providers and includes comprehensive error handling and retry mechanisms.

## 🏗️ Architecture

- **Apache Airflow**: Orchestrates daily stock data fetching
- **PostgreSQL**: Stores current stock prices and historical data
- **Docker Compose**: Containerized deployment
- **Multiple Data Providers**: yfinance (default), Alpha Vantage, Yahoo Finance Direct

## 📋 Features

- **Multi-provider support**: Automatic fallback between data sources
- **Robust error handling**: Retry logic with exponential backoff
- **Rate limiting**: Configurable delays to respect API limits
- **Historical tracking**: Maintains both current snapshots and historical records
- **Configurable symbols**: Easy to modify which stocks to track
- **Health checks**: Built-in monitoring for all services

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 2GB RAM available

### 1. Clone and Setup
```bash
git clone <repository-url>
cd stock-pipeline
```

### 2. Configure Environment
Create a `.env` file in the root directory:
```bash
# Database Configuration
DB_NAME=stocksdb
DB_USER=postgres
DB_PASS=postgres
DB_HOST=postgres
DB_PORT=5432

# Airflow Configuration
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Stock Configuration
SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA
MARKET_PROVIDER=yfinance

# Rate Limiting
REQUEST_DELAY_SECONDS=5.0
MAX_ATTEMPTS=3
BATCH_SIZE=1

# Alerts
ALERT_EMAIL=admin@example.com
```

### 3. Build and Initialize
```bash
# Build the Docker image
docker compose build

# Initialize Airflow database and create admin user
docker compose up airflow-init

# Start all services
docker compose up -d
```

### 4. Access Services
- **Airflow Web UI**: http://localhost:8080 (admin/admin)
- **PostgreSQL**: localhost:5432 (postgres/postgres)

### 5. Run the Pipeline
1. Open Airflow UI at http://localhost:8080
2. Find the `daily_stock_prices` DAG
3. Toggle it ON and trigger a manual run
4. Monitor the execution in the Airflow UI

### 6. Verify Data
```bash
# Connect to PostgreSQL and check data
psql -h localhost -U postgres -d stocksdb -c "SELECT * FROM stock_price;"

# Check historical data
psql -h localhost -U postgres -d stocksdb -c "SELECT * FROM stock_price_history ORDER BY fetched_at DESC LIMIT 10;"
```

## 📊 Database Schema

### `stock_price` (Current Snapshot)
```sql
symbol          TEXT PRIMARY KEY
price           DOUBLE PRECISION
change          DOUBLE PRECISION
percent_change  DOUBLE PRECISION
volume          BIGINT
fetched_at      TIMESTAMPTZ
```

### `stock_price_history` (Historical Records)
```sql
id              SERIAL PRIMARY KEY
symbol          TEXT NOT NULL
price           DOUBLE PRECISION
change          DOUBLE PRECISION
percent_change  DOUBLE PRECISION
volume          BIGINT
fetched_at      TIMESTAMPTZ
```
### Stock Symbols
```bash
SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA,META,NFLX,NVDA
```
### Custom DAG Schedule
Edit `dags/stock_price_dag.py`:
```python
schedule_interval="@daily",  # Change to "0 9 * * 1-5" for weekdays at 9 AM
```
## 🐛 Troubleshooting

### Common Issues

#### Services Won't Start
```bash
# Check logs
docker compose logs -f

# Restart services
docker compose down
docker compose up -d
```

#### Database Connection Issues
```bash
# Check PostgreSQL health
docker compose exec postgres pg_isready -U postgres

# Reset database
docker compose down -v
docker compose up -d
```

#### No Data Being Fetched
1. Check Airflow logs in the UI
2. Verify API keys and environment variables
3. Test manually:
```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/fetch_and_upsert.py
```
### Debugging

#### View Airflow Logs
```bash
# Follow scheduler logs
docker compose logs -f airflow-scheduler

# Follow webserver logs
docker compose logs -f airflow-webserver
```

#### Access Container
```bash
# Access Airflow container
docker compose exec airflow-webserver bash

# Run script manually
python /opt/airflow/scripts/fetch_and_upsert.py
```

#### Database Queries
```sql
-- Check recent fetches
SELECT symbol, price, fetched_at 
FROM stock_price 
ORDER BY fetched_at DESC;

-- Count historical records by symbol
SELECT symbol, COUNT(*) 
FROM stock_price_history 
GROUP BY symbol 
ORDER BY COUNT(*) DESC;

-- Price changes over time
SELECT symbol, price, change, percent_change, fetched_at
FROM stock_price_history 
WHERE symbol = 'AAPL'
ORDER BY fetched_at DESC 
LIMIT 10;
``

## 📝 File Structure

```
stock-pipeline/
├── .env                     # Environment configuration
├── .gitignore              # Git ignore rules
├── README.md               # This file
├── docker-compose.yml      # Docker services definition
├── requirements.txt        # Python dependencies
├── dags/
│   └── stock_price_dag.py  # Airflow DAG definition
├── docker/
│   └── Dockerfile          # Custom Airflow image
├── scripts/
│   ├── fetch_and_upsert.py # Main data fetching logic
│   └── utils.py            # Utility functions
└── sql/
    └── create_tables.sql   # Database schema
```



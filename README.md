# ETL Pipeline with Apache Airflow

**Apache Airflow implementation for automated daily sales data processing with DAG**

## Overview

This ETL pipeline automates the extraction, transformation, and loading of retail sales data from multiple sources using Apache Airflow. The system processes data from both online sales databases and in-store CSV files, transforming them into aggregated product sales summaries for business intelligence analysis.

## Architecture

**Data Sources:**
- PostgreSQL database (online sales transactions)
- CSV files (in-store sales records)

**Processing:**
- Data extraction from heterogeneous sources
- Transformation and aggregation by product ID
- Data quality validation and cleansing

**Target:**
- MySQL data warehouse (product sales summary)

## Pipeline Workflow

```
Extract (PostgreSQL) ──┐
                       ├── Transform & Aggregate ──> Load (MySQL)
Extract (CSV) ─────────┘
```

**DAG: `retail_sales_etl`**
- Schedule: Daily execution
- Tasks: `extract_postgres` → `extract_csv` → `transform_data` → `load_mysql`
- Dependencies: Parallel extraction, sequential transformation and loading

## Quick Start

### Prerequisites
- Docker & Docker Compose
- PostgreSQL (source database)
- MySQL (data warehouse)

### Setup
1. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit database credentials
   ```

2. **Initialize databases:**
   ```bash
   python setup_databases.py
   ```

3. **Deploy Airflow:**
   ```bash
   echo "AIRFLOW_UID=$(id -u)" >> .env
   docker-compose up -d
   ```

4. **Access Airflow UI:**
   - URL: `http://localhost:8080`
   - Credentials: `airflow/airflow`

## Data Schema

### Source Tables
**online_sales** (PostgreSQL):
```sql
sale_id      SERIAL PRIMARY KEY
product_id   INT
quantity     INT
sale_amount  DECIMAL(10,2)
sale_date    DATE
```

**in_store_sales** (CSV):
```
sale_id,product_id,quantity,sale_amount,sale_date
```

### Target Table
**product_sales_summary** (MySQL):
```sql
product_id         INT PRIMARY KEY
total_quantity     INT
total_sale_amount  DECIMAL(10,2)
last_updated       TIMESTAMP
```

## Configuration

### Airflow Connections
Configure these connections in Airflow UI:
- `postgres_default`: Source database connection
- `mysql_default`: Data warehouse connection

### Environment Variables
```bash
POSTGRES_HOST=localhost
POSTGRES_DB=sales_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

MYSQL_HOST=localhost
MYSQL_DB=sales_warehouse
MYSQL_USER=root
MYSQL_PASSWORD=your_password
```

## Data Processing Logic

1. **Extraction**: Parallel retrieval from PostgreSQL and CSV sources
2. **Transformation**: 
   - Data type validation and casting
   - Null value removal
   - Product-level aggregation (sum quantities and amounts)
3. **Loading**: Upsert operations to MySQL with timestamp tracking

## Monitoring

- Task execution status via Airflow web interface
- Execution logs and error tracking
- Data quality metrics and validation results

## Development

### Local Testing
```bash
pip install -r requirements.txt
python setup_databases.py
python -m pytest tests/  # If tests are implemented
```

### Adding Data Sources
1. Implement extraction function
2. Create PythonOperator task
3. Update transformation logic
4. Modify task dependencies 

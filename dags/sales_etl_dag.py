from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'retail_sales_etl',
    default_args=default_args,
    description='ETL pipeline for retail sales data',
    schedule='@daily',
    catchup=False,
)

# Define the path to the CSV file
csv_file_path = 'in_store_sales.csv'

# Define extraction functions
def extract_from_postgres(**kwargs):
    """Extract online sales data from PostgreSQL database"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get execution date from context
    execution_date = kwargs.get('ds')
    
    try:
        # Extract data for the specified date
        sql_query = f"""
        SELECT product_id, quantity, sale_amount, sale_date 
        FROM online_sales 
        WHERE sale_date = '{execution_date}'
        """
        df = pg_hook.get_pandas_df(sql_query)
    except Exception as e:
        # If there's an error, try to get all data
        sql_query = """
        SELECT product_id, quantity, sale_amount, sale_date 
        FROM online_sales
        """
        df = pg_hook.get_pandas_df(sql_query)
    
    # Add source column for tracking
    df['source'] = 'online'
    
    # Push data to XCom
    kwargs['ti'].xcom_push(key='online_sales_data', value=df.to_json())
    return f"Extracted {len(df)} rows from PostgreSQL"

def extract_from_csv(**kwargs):
    """Extract in-store sales data from CSV file"""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        # Filter for the execution date
        execution_date = kwargs.get('ds')
        filtered_df = df[df['sale_date'] == execution_date]
        
        # If no data for the date, use all data
        if len(filtered_df) == 0:
            filtered_df = df
        
        # Add source column for tracking
        filtered_df['source'] = 'in-store'
        
        # Push data to XCom
        kwargs['ti'].xcom_push(key='in_store_sales_data', value=filtered_df.to_json())
        return f"Extracted {len(filtered_df)} rows from CSV"
    except Exception as e:
        return f"Error extracting data from CSV: {str(e)}"

def transform_data(**kwargs):
    """Transform the extracted data"""
    ti = kwargs['ti']
    
    # Get data from XComs
    online_data_json = ti.xcom_pull(task_ids='extract_postgres', key='online_sales_data')
    in_store_data_json = ti.xcom_pull(task_ids='extract_csv', key='in_store_sales_data')
    
    # Convert JSON to DataFrames
    online_df = pd.read_json(online_data_json)
    in_store_df = pd.read_json(in_store_data_json)
    
    # Combine data from both sources
    combined_df = pd.concat([online_df, in_store_df], ignore_index=True)
    
    # Data cleansing - Remove null or invalid entries
    combined_df = combined_df.dropna(subset=['product_id', 'quantity', 'sale_amount'])
    
    # Convert data types if needed
    combined_df['product_id'] = combined_df['product_id'].astype(int)
    combined_df['quantity'] = combined_df['quantity'].astype(int)
    combined_df['sale_amount'] = combined_df['sale_amount'].astype(float)
    
    # Aggregate data by product_id
    aggregated_df = combined_df.groupby('product_id').agg({'quantity': 'sum','sale_amount': 'sum'}).reset_index()
    
    #rename columns
    aggregated_df.columns = ['product_id', 'total_quantity', 'total_sale_amount']
    
    # Push the transformed data to XCom
    ti.xcom_push(key = 'transformed_data', value = aggregated_df.to_json())
    
    return f"Transformed data: {len(aggregated_df)} products aggregated"

def load_to_mysql(**kwargs):
    """Load the transformed data into MySQL"""
    ti = kwargs['ti']
    
    # Get transformed data from XCom
    transformed_data_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.read_json(transformed_data_json)
    
    # Connect to MySQL and load data
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # Create the table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS product_sales_summary (
        product_id INT PRIMARY KEY,
        total_quantity INT,
        total_sale_amount DECIMAL(10, 2),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
    mysql_hook.run(create_table_sql)
    
    # Prepare the data for insertion
    records = df.to_dict('records')
    
    # Insert data using REPLACE to handle existing records
    for record in records:
        insert_sql = f"""
        REPLACE INTO product_sales_summary 
        (product_id, total_quantity, total_sale_amount)
        VALUES
        ({record['product_id']}, {record['total_quantity']}, {record['total_sale_amount']})
        """
        mysql_hook.run(insert_sql)
    
    return f"Loaded {len(records)} rows into MySQL"

# Define the tasks
extract_postgres_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_from_postgres,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id = 'extract_csv',
    python_callable = extract_from_csv,
    dag = dag,
)

transform_task = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    dag = dag,
)

load_task = PythonOperator(
    task_id = 'load_mysql',
    python_callable = load_to_mysql,
    dag = dag,
)

# Set task dependencies
[extract_postgres_task, extract_csv_task] >> transform_task >> load_task 
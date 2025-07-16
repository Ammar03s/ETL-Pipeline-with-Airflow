import psycopg2
import mysql.connector
from datetime import datetime, timedelta

#PostgreSQL
pg_params = {
    'host': 'localhost',
    'database': 'sales_db',
    'user': 'postgres',
    'password': 'ammar2003'  #my pass
}

#MySQL
mysql_params = {
    'host': 'localhost',
    'database': 'sales_warehouse',
    'user': 'root',
    'password': 'Ammar2003'  #my pass
}

def setup_postgres():
    conn = None
    try:
        temp_params = pg_params.copy()
        temp_params['database'] = 'postgres'  #default DB
        
        temp_conn = psycopg2.connect(**temp_params)
        temp_conn.autocommit = True  
        temp_cursor = temp_conn.cursor()
        
        #ensure if db exists
        temp_cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg_params['database'],))
        exists = temp_cursor.fetchone()
        if not exists:
            temp_cursor.execute(f"CREATE DATABASE {pg_params['database']}")
            print(f"Created database {pg_params['database']}")
        temp_cursor.close()
        temp_conn.close()


        #connect to our new db
        conn = psycopg2.connect(**pg_params)
        cursor = conn.cursor()
        
        #create table (if doesn't exist)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS online_sales (
            sale_id SERIAL PRIMARY KEY,
            product_id INT,
            quantity INT,
            sale_amount DECIMAL(10, 2),
            sale_date DATE
        )
        """)
        
        #clear existing data testtt
        cursor.execute("TRUNCATE TABLE online_sales RESTART IDENTITY")
        
        #sample data
        sample_data = [
            (101, 2, 40.00, '2024-03-01'),
            (102, 1, 20.00, '2024-03-01'),
            (103, 3, 60.00, '2024-03-02'),
            (101, 1, 20.00, '2024-03-02'),
            (104, 2, 50.00, '2024-03-03'),
            (105, 1, 25.00, '2024-03-03'),
            (101, 3, 60.00, '2024-03-04'),
            (102, 2, 40.00, '2024-03-04'),
            (103, 1, 20.00, '2024-03-05')
        ]
        


        for data in sample_data:
            cursor.execute("""
            INSERT INTO online_sales (product_id, quantity, sale_amount, sale_date)
            VALUES (%s, %s, %s, %s)
            """, data)
        
        #commit the changes
        conn.commit()
        print("PostgreSQL database setup completed successfully!")



    except Exception as e:
        print(f"Error setting up PostgreSQL database: {str(e)}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def setup_mysql():
    conn = None
    try:
        temp_params = mysql_params.copy()
        temp_params.pop('database', None) 
        temp_conn = mysql.connector.connect(**temp_params)
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_params['database']}")
        temp_cursor.close()
        temp_conn.close()
        #new db
        conn = mysql.connector.connect(**mysql_params)
        cursor = conn.cursor()
        

        #table for the aggregated sales
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_sales_summary (
            product_id INT PRIMARY KEY,
            total_quantity INT,
            total_sale_amount DECIMAL(10, 2),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        print("MySQL database setup completed successfully!")
    except Exception as e:
        print(f"Error setting up MySQL database: {str(e)}")

    finally:
        if conn:
            cursor.close()
            conn.close()




if __name__ == "__main__":
    print("Setting up db-_-")
    setup_postgres()
    setup_mysql()
    print("setup done!") 
from clickhouse_driver import Client
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import logging
import pandas as pd

def ch_connection(conn_id='clickhouse'):
    """Создаю подключение к ClickHouse"""
    try:
        conn = BaseHook.get_connection(conn_id)
        client = Client(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema or 'default'
        )
        logging.info('Success connection to ClickHouse!')
        return client
    except Exception as e:
        logging.error(f"Failed to connect to Clickhouse: {e}")
        raise AirflowException(f'Error connection to Clickhouse: {e}')

def create_tables():
    """Создаю таблицы с движком TinyLog"""
    client = ch_connection()
    
    tables_sql = [
        """
        CREATE TABLE IF NOT EXISTS raw_impressions (
            req_id String,
            user_id String,
            campaign_id String,
            creative_id String,
            ip String,
            ua String,
            ts DateTime
        ) ENGINE = TinyLog
        """,
        
        """
        CREATE TABLE IF NOT EXISTS raw_clicks (
            req_id String,
            user_id String,
            campaign_id String,
            creative_id String,
            ip String,
            ua String,
            ts DateTime
        ) ENGINE = TinyLog
        """,
        
        """
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            ts DateTime,
            ip String,
            clicks UInt32,
            window_start DateTime,
            window_end DateTime
        ) ENGINE = TinyLog
        """
    ]
    
    try:
        for sql in tables_sql:
            client.execute(sql)
        logging.info("All tables created successfully!")
    except Exception as e:
        logging.error(f"Failed to create tables: {e}")
        raise AirflowException(f'Error creating tables: {e}')

    
def load_csv_to_clickhouse(table_name, csv_path, schema):
    """Загружаю CSV в ClickHouse с указанием схемы для преобразования типов"""
    client = ch_connection()
    
    try:
        df = pd.read_csv(csv_path)
        
        data = []
        for _, row in df.iterrows():
            processed_row = []
            for i, (col_name, value) in enumerate(row.items()):
                if col_name in schema:
                    col_type = schema[col_name]
                    if col_type == 'datetime':
                        processed_row.append(pd.to_datetime(value))
                    elif col_type == 'int':
                        processed_row.append(int(value) if pd.notna(value) else 0)
                    elif col_type == 'float':
                        processed_row.append(float(value) if pd.notna(value) else 0.0)
                    elif col_type == 'bool':
                        processed_row.append(bool(value) if pd.notna(value) else False)
                    else:
                        processed_row.append(str(value) if pd.notna(value) else '')
                else:
                    # Если тип не указан в схеме, использую строку
                    processed_row.append(str(value) if pd.notna(value) else '')
            data.append(processed_row)

        query = f"INSERT INTO {table_name} VALUES"
        client.execute(query, data)
        
        return f"Successfully loaded {len(data)} rows into {table_name}"
        
    except Exception as e:
        logging.error(f"Failed to load data to {table_name}: {e}")
        raise AirflowException(f'Error loading data to {table_name}: {e}')
    

def check_data_quality():
    """Проверка качества данных"""
    client = ch_connection()
    impressions_count = client.execute("SELECT count() FROM raw_impressions WHERE ts > now() - INTERVAL 2 MINUTE")[0][0]
    clicks_count = client.execute("SELECT count() FROM raw_clicks WHERE ts > now() - INTERVAL 2 MINUTE")[0][0]
    return impressions_count, clicks_count
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from plugins import data_generator, data_validator, clickhouse_loader, queries, fraud_detector
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def create_tables_task():
    """Создаем таблицы в ClickHouse"""
    clickhouse_loader.create_tables()
    logging.info("Tables created successfully!")

def generate_data_task():
    """Генерирует тестовые данные"""
    start_id, end_id = data_generator.generate_sample_data(
        output_path_impressions='/opt/airflow/data/impressions.csv',
        output_path_clicks='/opt/airflow/data/clicks.csv',
        num_rows=50
    )
    logging.info(f"Generated data with IDs from {start_id} to {end_id}")

def validate_impressions_task():
    """Валидируем и очищает данные impressions"""
    data_validator.validate_and_clean_impressions(
        '/opt/airflow/data/impressions.csv',
        '/opt/airflow/data/cleaned_impressions.csv'
    )

def validate_clicks_task():
    """Валидируем и очищает данные clicks"""
    data_validator.validate_and_clean_clicks(
        '/opt/airflow/data/clicks.csv',
        '/opt/airflow/data/cleaned_clicks.csv'
    )

def load_impressions_task():
    from plugins.clickhouse_loader import load_csv_to_clickhouse
    
    # Схема для raw_impressions
    impressions_schema = {
        'req_id': 'string',
        'user_id': 'string', 
        'campaign_id': 'string',
        'creative_id': 'string',
        'ip': 'string',
        'ua': 'string',
        'ts': 'datetime'
    }
    
    result = load_csv_to_clickhouse(
        'raw_impressions', 
        '/opt/airflow/data/cleaned_impressions.csv',
        impressions_schema
    )
    print(result)

def load_clicks_task():
    from plugins.clickhouse_loader import load_csv_to_clickhouse
    
    # Схема для raw_clicks
    clicks_schema = {
        'req_id': 'string',
        'user_id': 'string',
        'campaign_id': 'string',
        'creative_id': 'string', 
        'ip': 'string',
        'ua': 'string',
        'ts': 'datetime'
    }
    
    result = load_csv_to_clickhouse(
        'raw_clicks',
        '/opt/airflow/data/cleaned_clicks.csv',
        clicks_schema
    )
    logging.info(result)

def data_quality_check_task():
    """Проверяем, что данные загрузились"""
    impressions_count, clicks_count = clickhouse_loader.check_data_quality()
    logging.info(f"Impressions loaded: {impressions_count}, Clicks loaded: {clicks_count}")
    if impressions_count == 0 and clicks_count == 0:
        raise ValueError("No data loaded in the last 2 minutes")

def run_aggregations_task():
    """Выполняем агрегационные запросы"""
    results = queries.run_aggregation_queries(clickhouse_loader.ch_connection())
    for query_name, result in results.items():
        logging.info(f"Results for {query_name}: {result}")

def run_fraud_check_task():
    """Запускаем анти-фрод проверку"""
    result = fraud_detector.run_fraud_detection(clickhouse_loader.ch_connection())
    logging.info(result)

with DAG(
    'etl_ads',
    default_args=default_args,
    description='Минутный ETL для импрешенов и кликов',
    schedule_interval='*/1 * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['ads', 'clickhouse']
) as dag:
    
    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables_task
    )
    
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data_task
    )
    
    validate_impressions = PythonOperator(
        task_id='validate_impressions',
        python_callable=validate_impressions_task
    )
    
    validate_clicks = PythonOperator(
        task_id='validate_clicks',
        python_callable=validate_clicks_task
    )
    
    load_impressions = PythonOperator(
        task_id='load_impressions',
        python_callable=load_impressions_task
    )
    
    load_clicks = PythonOperator(
        task_id='load_clicks',
        python_callable=load_clicks_task
    )
    
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check_task
    )
    
    run_aggregations = PythonOperator(
        task_id='run_aggregations',
        python_callable=run_aggregations_task
    )
    
    run_fraud_check = PythonOperator(
        task_id='run_fraud_check',
        python_callable=run_fraud_check_task
    )

    create_tables >> generate_data >> [validate_impressions, validate_clicks]
    validate_impressions >> load_impressions
    validate_clicks >> load_clicks
    [load_impressions, load_clicks] >> data_quality_check >> [run_aggregations, run_fraud_check]
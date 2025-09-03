import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import logging

STATE_FILE_PATH = '/opt/airflow/data/last_id_state.json'

def get_last_id():
    """Читаем последний использованный ID из файла состояния"""
    try:
        if os.path.exists(STATE_FILE_PATH):
            with open(STATE_FILE_PATH, 'r') as f:
                state = json.load(f)
                return state.get('last_id', 0)
    except:
        pass
    return 0

def save_last_id(last_id):
    """Сохраняем последний использованный ID в файл состояния"""
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    state = {'last_id': last_id}
    with open(STATE_FILE_PATH, 'w') as f:
        json.dump(state, f)

def generate_sample_data(output_path_impressions, output_path_clicks, num_rows=100):
    """Генерируем тестовые данные с уникальными ID, продолжая с последнего номера"""
    
    last_id = get_last_id()
    start_id = last_id + 1
    end_id = last_id + num_rows
    
    logging.info(f"Generating data with IDs from {start_id} to {end_id}")
    
    impressions_data = {
        'req_id': [f'imp_{i}' for i in range(start_id, end_id + 1)],
        'user_id': [f'user_{i}' for i in range(start_id, end_id + 1)],
        'campaign_id': np.random.choice([1, 2, 3], num_rows),
        'creative_id': np.random.choice([1, 2], num_rows),
        'ip': [f'192.168.1.{i%254 + 1}' for i in range(start_id, end_id + 1)],
        'ua': ['Mozilla/5.0'] * num_rows,
        'ts': [datetime.now() - timedelta(seconds=i) for i in range(num_rows)]
    }
    
    num_clicks = int(num_rows * 0.3)
    click_ids = list(range(start_id, start_id + num_clicks))
    
    # клики для анти-фрода
    fraud_ip = '192.168.1.100'
    normal_ips = [f'192.168.1.{i%254 + 1}' for i in range(start_id, start_id + num_clicks)]
    
    # Заменяем некоторые IP на подозрительные
    fraud_clicks_count = np.random.randint(8, 13)
    fraud_indices = np.random.choice(range(num_clicks), size=min(fraud_clicks_count, num_clicks), replace=False)
    for idx in fraud_indices:
        normal_ips[idx] = fraud_ip
    
    clicks_data = {
        'req_id': [f'click_{i}' for i in click_ids],
        'user_id': [f'user_{i}' for i in click_ids],
        'campaign_id': np.random.choice([1, 2, 3], num_clicks),
        'creative_id': np.random.choice([1, 2], num_clicks),
        'ip': normal_ips,
        'ua': ['Mozilla/5.0'] * num_clicks,
        'ts': [datetime.now() - timedelta(seconds=i%15) for i in range(num_clicks)]
    }
    
    # Добавляем "грязь" - несколько пустых UA
    num_empty_ua = max(1, int(num_rows * 0.1))
    empty_ua_indices = np.random.choice(range(num_rows), size=num_empty_ua, replace=False)
    for idx in empty_ua_indices:
        impressions_data['ua'][idx] = None
    
    # Добавляем несколько дубликатов req_id в impressions
    if num_rows > 5:
        duplicate_idx = np.random.choice(range(2, num_rows-2), size=2, replace=False)
        impressions_data['req_id'][duplicate_idx[1]] = impressions_data['req_id'][duplicate_idx[0]]
    
    df_impressions = pd.DataFrame(impressions_data)
    df_clicks = pd.DataFrame(clicks_data)
    
    df_impressions.to_csv(output_path_impressions, index=False)
    df_clicks.to_csv(output_path_clicks, index=False)
    
    save_last_id(end_id)
    
    return start_id, end_id
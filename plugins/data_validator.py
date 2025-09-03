import pandas as pd

def validate_and_clean_impressions(input_path, output_path):
    """Валидирует и очищает данные impressions"""
    df = pd.read_csv(input_path)
    
    # Проверка обязательных колонок
    required_columns = {'req_id', 'user_id', 'campaign_id', 'creative_id', 'ip', 'ua', 'ts'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns in impressions: {missing}")
    
    # Очистка данных
    df_clean = df.drop_duplicates(subset=['req_id'], keep='first')
    df_clean['ua'] = df_clean['ua'].fillna('Unknown')
    df_clean['ts'] = pd.to_datetime(df_clean['ts'])
    
    df_clean.to_csv(output_path, index=False)
    return output_path

def validate_and_clean_clicks(input_path, output_path):
    """Валидирует и очищает данные clicks."""
    df = pd.read_csv(input_path)
    
    required_columns = {'req_id', 'user_id', 'campaign_id', 'creative_id', 'ip', 'ua', 'ts'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns in clicks: {missing}")
    
    df_clean = df.drop_duplicates(subset=['req_id'], keep='first')
    df_clean['ua'] = df_clean['ua'].fillna('Unknown')
    df_clean['ts'] = pd.to_datetime(df_clean['ts'])
    
    df_clean.to_csv(output_path, index=False)
    return output_path
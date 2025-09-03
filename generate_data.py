import csv
import random
from datetime import datetime, timedelta
import os

def generate_user_agents():
    """Генерация различных user agents"""
    agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15',
        'Mozilla/5.0 (Android 10; Mobile; rv:91.0) Gecko/91.0 Firefox/91.0',
        '',  # Пустой UA
        'Invalid User Agent String',
        'curl/7.68.0'
    ]
    return random.choice(agents)

def generate_ip():
    """Генерация IP адресов"""
    return f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"

def generate_timestamp(days_back=30):
    """Генерация временных меток"""
    now = datetime.now()
    random_days = random.randint(0, days_back)
    random_hours = random.randint(0, 23)
    random_minutes = random.randint(0, 59)
    random_seconds = random.randint(0, 59)
    
    ts = now - timedelta(days=random_days, hours=random_hours, 
                        minutes=random_minutes, seconds=random_seconds)
    return ts.strftime('%Y-%m-%d %H:%M:%S')

def generate_data():
    """Генерация данных для файлов"""
    
    # Создаем директорию data если её нет
    os.makedirs('data', exist_ok=True)
    
    # Генерируем базовые данные
    num_records = 1000
    campaigns = [f'campaign_{i}' for i in range(1, 11)]
    creatives = [f'creative_{i}' for i in range(1, 21)]
    users = [f'user_{i}' for i in range(1, 501)]
    
    # Создаем словарь для отслеживания req_id
    req_ids = set()
    
    # Генерируем impressions
    impressions_data = []
    for i in range(num_records):
        req_id = f'req_{i:04d}'
        req_ids.add(req_id)
        
        # С вероятностью 2% создаем дубликат req_id
        if random.random() < 0.02 and i > 0:
            req_id = impressions_data[random.randint(0, len(impressions_data)-1)]['req_id']
        
        impression = {
            'req_id': req_id,
            'user_id': random.choice(users),
            'campaign_id': random.choice(campaigns),
            'creative_id': random.choice(creatives),
            'ip': generate_ip(),
            'ua': generate_user_agents(),
            'ts': generate_timestamp()
        }
        impressions_data.append(impression)
    
    # Генерируем clicks
    clicks_data = []
    click_probability = 0.3  # Вероятность клика
    
    for impression in impressions_data:
        if random.random() < click_probability:
            click_ts = datetime.strptime(impression['ts'], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=random.randint(1, 60))
            
            click = {
                'req_id': impression['req_id'],
                'user_id': impression['user_id'],
                'campaign_id': impression['campaign_id'],
                'creative_id': impression['creative_id'],
                'ip': impression['ip'],
                'ua': impression['ua'],
                'ts': click_ts.strftime('%Y-%m-%d %H:%M:%S')
            }
            clicks_data.append(click)
    
    # Добавляем "грязные" клики (без соответствующих импрешенов)
    num_dirty_clicks = int(num_records * 0.05)  # 5% грязных кликов
    for i in range(num_dirty_clicks):
        dirty_req_id = f'dirty_req_{i:04d}'
        
        click = {
            'req_id': dirty_req_id,
            'user_id': random.choice(users),
            'campaign_id': random.choice(campaigns),
            'creative_id': random.choice(creatives),
            'ip': generate_ip(),
            'ua': generate_user_agents(),
            'ts': generate_timestamp()
        }
        clicks_data.append(click)
    
    # Записываем данные в CSV файлы
    write_csv('data/impressions.csv', impressions_data, ['req_id', 'user_id', 'campaign_id', 'creative_id', 'ip', 'ua', 'ts'])
    write_csv('data/clicks.csv', clicks_data, ['req_id', 'user_id', 'campaign_id', 'creative_id', 'ip', 'ua', 'ts'])
    
    print(f"Сгенерировано {len(impressions_data)} записей в impressions.csv")
    print(f"Сгенерировано {len(clicks_data)} записей в clicks.csv")
    print("Файлы сохранены в папке data/")

def write_csv(filename, data, fieldnames):
    """Запись данных в CSV файл"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    generate_data()
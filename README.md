# Airflow Test Work

Проект для обработки рекламных данных с использованием Apache Airflow и ClickHouse.

### Исходные таблицы:
- **raw_impressions** - данные о показах рекламы
- **raw_clicks** - данные о кликах по рекламе

### Результирующие таблицы:
- **fraud_alerts** - обнаруженные мошеннические активности

## 🚀 Быстрый старт

## Connections создаются сами, файлы данных генерируются в даге

### Запуск инфраструктуры:
```bash
docker compose up airflow-init
docker compose up -d
```

## Остановить контейнеры

```bash
docker compose down -v
```

## Доступ в airflow (логин: airflow, пароль: airflow)

http://localhost:8080

## Проверить что данные загружаются, можно подключиться к clickhouse, например через dbeaver

Хост: localhost

Схема: default

Порт: 8123

Пользователь: clickhouse_user

Пароль: clickhouse_password

![Таблица fraud_alerts](images/fraud_alerts.png)

```sql
WITH events AS (
    SELECT 
        campaign_id, 
        'impression' as event_type
    FROM raw_impressions 
    WHERE ts >= (now() - toIntervalMinute(30))
    UNION ALL
    SELECT 
        campaign_id, 
        'click' as event_type  
    FROM raw_clicks 
    WHERE ts >= (now() - toIntervalMinute(30))
)
SELECT 
    campaign_id,
    countIf(event_type = 'click') as total_clicks,
    countIf(event_type = 'impression') as total_impressions,
    if(total_impressions > 0, total_clicks / total_impressions, 0) as ctr
FROM events
GROUP BY campaign_id
HAVING total_impressions > 0
ORDER BY ctr DESC
LIMIT 5;
```

![Запрос Топ-5 кампаний по CTR за последние 30 минут](images/top5_campaigns.png)

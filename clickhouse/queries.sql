-- 1. Топ-5 кампаний по CTR за последние 30 минут
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

-- 2. IP из fraud_alerts за последние 24 часа
SELECT DISTINCT ip
FROM fraud_alerts 
WHERE ts >= (now() - toIntervalHour(24));
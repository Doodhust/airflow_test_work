-- 1. Топ-5 кампаний по CTR за последние 30 минут
SELECT 
    campaign_id,
    sum(clicks) as total_clicks,
    sum(impressions) as total_impressions,
    if(total_impressions > 0, total_clicks / total_impressions, 0) as ctr
FROM (
    SELECT 
        campaign_id,
        count() as impressions,
        0 as clicks
    FROM ads.raw_impressions 
    WHERE ts >= now() - INTERVAL 30 MINUTE
    GROUP BY campaign_id
    
    UNION ALL
    
    SELECT 
        campaign_id,
        0 as impressions,
        count() as clicks
    FROM ads.raw_clicks 
    WHERE ts >= now() - INTERVAL 30 MINUTE
    GROUP BY campaign_id
)
GROUP BY campaign_id
ORDER BY ctr DESC
LIMIT 5;

-- 2. IP из fraud_alerts за последние 24 часа
SELECT DISTINCT ip
FROM ads.fraud_alerts 
WHERE ts >= now() - INTERVAL 24 HOUR;
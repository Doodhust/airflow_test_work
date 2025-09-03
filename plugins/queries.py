AGGREGATION_QUERIES = {
    'top_campaigns_ctr': """
        SELECT 
            campaign_id,
            count() as impressions,
            sumIf(1, event_type = 'click') as clicks,
            clicks / impressions as ctr
        FROM (
            SELECT 
                campaign_id, 
                'impression' as event_type
            FROM raw_impressions 
            WHERE ts >= now() - INTERVAL 30 MINUTE
            UNION ALL
            SELECT 
                campaign_id, 
                'click' as event_type
            FROM raw_clicks 
            WHERE ts >= now() - INTERVAL 30 MINUTE
        )
        GROUP BY campaign_id
        HAVING impressions > 0
        ORDER BY ctr DESC
        LIMIT 5
    """,
    
    'fraud_ips': """
        SELECT DISTINCT ip 
        FROM fraud_alerts 
        WHERE ts >= now() - INTERVAL 24 HOUR
    """
}

def run_aggregation_queries(client):
    """Выполняет агрегационные запросы и возвращает результаты."""
    results = {}
    for name, query in AGGREGATION_QUERIES.items():
        result = client.execute(query)
        results[name] = result
    return results
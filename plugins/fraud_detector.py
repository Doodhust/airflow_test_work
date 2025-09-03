FRAUD_DETECTION_QUERY = """
    INSERT INTO fraud_alerts
    SELECT 
        now() AS ts,
        ip,
        count() AS clicks,
        toStartOfInterval(ts, toIntervalSecond(10)) AS window_start,
        window_start + toIntervalSecond(10) AS window_end
    FROM raw_clicks
    WHERE ts >= (now() - toIntervalMinute(2))
    GROUP BY 
        ip,
        window_start
    HAVING count() > 5
"""

def run_fraud_detection(client):
    """Запускаем анти-фрод правило"""
    result = client.execute(FRAUD_DETECTION_QUERY)
    return f"Fraud detection completed. Rows affected: {result}"
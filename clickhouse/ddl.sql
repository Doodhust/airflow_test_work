CREATE DATABASE IF NOT EXISTS layers;

CREATE TABLE IF NOT EXISTS layers.raw_impressions
(
    req_id String,
    user_id Int32,
    campaign_id Int32,
    creative_id Int32,
    ip String,
    ua String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (ts, req_id);

CREATE TABLE IF NOT EXISTS layers.raw_clicks
(
    req_id String,
    user_id Int32,
    campaign_id Int32,
    creative_id Int32,
    ip String,
    ua String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (ts, req_id);

-- Таблица фрод-алертов
CREATE TABLE IF NOT EXISTS layers.fraud_alerts
(
    ts DateTime,
    ip String,
    clicks Int32,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree()
ORDER BY (ts, ip);
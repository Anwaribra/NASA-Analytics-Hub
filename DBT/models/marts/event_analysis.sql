WITH fact_events AS (
    SELECT *
    FROM {{ ref('fact_eonet_events') }}
)

SELECT
    category_title,
    DATE_TRUNC('month', EVENT_TIME) as event_month,
    COUNT(DISTINCT EVENT_ID) as event_count,
    COUNT(CASE WHEN CLOSED THEN EVENT_ID END) as closed_events,
    AVG(longitude) as avg_longitude,
    AVG(latitude) as avg_latitude,
    MIN(EVENT_TIME) as first_event,
    MAX(EVENT_TIME) as last_event,
    MIN(INGESTED_AT) as first_ingestion,
    MAX(INGESTED_AT) as last_ingestion
FROM fact_events
WHERE category_title IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 1
WITH source AS (
    SELECT *
    FROM {{ ref('stg_eonet_events') }}
),

event_categories AS (
    SELECT
        EVENT_TIME,
        EVENT_ID,
        c.value:id::STRING as category_id,
        c.value:title::STRING as category_title
    FROM source,
    LATERAL FLATTEN(input => CATEGORIES) c
),

event_geometries AS (
    SELECT
        EVENT_TIME,
        EVENT_ID,
        g.value:type::STRING as geometry_type,
        g.value:coordinates[0]::FLOAT as longitude,
        g.value:coordinates[1]::FLOAT as latitude,
        g.value:date::TIMESTAMP as geometry_date
    FROM source,
    LATERAL FLATTEN(input => GEOMETRIES) g
)

SELECT
    s.EVENT_TIME,
    s.EVENT_ID,
    s.TITLE,
    s.DESCRIPTION,
    s.LINK,
    s.CLOSED,
    c.category_id,
    c.category_title,
    g.geometry_type,
    g.longitude,
    g.latitude,
    g.geometry_date,
    s.INGESTED_AT
FROM source s
LEFT JOIN event_categories c 
    ON s.EVENT_ID = c.EVENT_ID 
    AND s.EVENT_TIME = c.EVENT_TIME
LEFT JOIN event_geometries g 
    ON s.EVENT_ID = g.EVENT_ID 
    AND s.EVENT_TIME = g.EVENT_TIME
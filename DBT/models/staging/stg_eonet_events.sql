WITH source AS (
    SELECT *
    FROM {{ source('nasa', 'EONET_EVENTS') }}
)

SELECT
    EVENT_TIME,
    DATA:id::STRING as EVENT_ID,
    DATA:title::STRING as TITLE,
    DATA:description::STRING as DESCRIPTION,
    DATA:link::STRING as LINK,
    DATA:closed::BOOLEAN as CLOSED,
    DATA:categories::VARIANT as CATEGORIES,
    DATA:sources::VARIANT as SOURCES,
    DATA:geometry::VARIANT as GEOMETRIES,
    EVENT_TIME as INGESTED_AT
FROM source
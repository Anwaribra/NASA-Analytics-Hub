WITH source AS (
    SELECT *
    FROM {{ source('nasa', 'ISS_EVENTS') }}
),

parsed_data AS (
    SELECT
        EVENT_TIME,
        PARSE_JSON(DATA) as json_data
    FROM source
)

SELECT
    EVENT_TIME,
    json_data:iss_position:latitude::FLOAT as LATITUDE,
    json_data:iss_position:longitude::FLOAT as LONGITUDE,
    json_data:message::STRING as MESSAGE,
    EVENT_TIME as INGESTED_AT
FROM parsed_data
WHERE json_data:iss_position IS NOT NULL
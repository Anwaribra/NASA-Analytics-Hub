WITH source AS (
    SELECT *
    FROM {{ source('nasa', 'ISS_EVENTS') }}
)

SELECT
    EVENT_TIME,
    PARSE_JSON(DATA):iss_position:latitude::FLOAT as LATITUDE,
    PARSE_JSON(DATA):iss_position:longitude::FLOAT as LONGITUDE,
    PARSE_JSON(DATA):message::STRING as MESSAGE,
    EVENT_TIME as INGESTED_AT
FROM source
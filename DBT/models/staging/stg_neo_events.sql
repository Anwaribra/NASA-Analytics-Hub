WITH source AS (
    SELECT *
    FROM {{ source('nasa', 'NEO_EVENTS') }}
),

flattened_neos AS (
    SELECT
        EVENT_TIME,
        f.value as neo_data,
        EVENT_TIME as INGESTED_AT
    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(DATA)) f
)

SELECT
    EVENT_TIME,
    neo_data:id::STRING as NEO_ID,
    neo_data:name::STRING as NAME,
    neo_data:nasa_jpl_url::STRING as NASA_JPL_URL,
    neo_data:is_potentially_hazardous_asteroid::BOOLEAN as IS_POTENTIALLY_HAZARDOUS,
    neo_data:estimated_diameter::VARIANT as ESTIMATED_DIAMETER,
    neo_data:close_approach_data::VARIANT as CLOSE_APPROACH_DATA,
    INGESTED_AT
FROM flattened_neos
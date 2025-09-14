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
    json_data:orbital_elements:semi_major_axis::FLOAT as SEMI_MAJOR_AXIS,
    json_data:orbital_elements:eccentricity::FLOAT as ECCENTRICITY,
    json_data:orbital_elements:inclination::FLOAT as INCLINATION,
    json_data:orbital_elements:velocity::FLOAT as VELOCITY,
    json_data:orbital_elements:period::FLOAT as ORBITAL_PERIOD,
    json_data:orbital_elements:position_vector:x::FLOAT as POSITION_X,
    json_data:orbital_elements:position_vector:y::FLOAT as POSITION_Y,
    json_data:orbital_elements:position_vector:z::FLOAT as POSITION_Z,
    EVENT_TIME as INGESTED_AT
FROM parsed_data
WHERE json_data:iss_position IS NOT NULL
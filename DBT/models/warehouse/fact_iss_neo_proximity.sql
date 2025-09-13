WITH iss_positions AS (
    SELECT
        EVENT_TIME,
        LATITUDE,
        LONGITUDE,
        INGESTED_AT
    FROM {{ ref('stg_iss_events') }}
),

neo_approaches AS (
    SELECT
        EVENT_TIME,
        NEO_ID,
        NAME,
        IS_POTENTIALLY_HAZARDOUS,
        f.value:epoch_date_close_approach::TIMESTAMP as CLOSE_APPROACH_TIME,
        f.value:miss_distance::VARIANT as MISS_DISTANCE,
        f.value:relative_velocity::VARIANT as RELATIVE_VELOCITY,
        INGESTED_AT
    FROM {{ ref('stg_neo_events') }},
    LATERAL FLATTEN(input => CLOSE_APPROACH_DATA) f
)

SELECT
    i.EVENT_TIME,
    i.LATITUDE as ISS_LATITUDE,
    i.LONGITUDE as ISS_LONGITUDE,
    n.NEO_ID,
    n.NAME as NEO_NAME,
    n.IS_POTENTIALLY_HAZARDOUS,
    n.CLOSE_APPROACH_TIME,
    n.MISS_DISTANCE:kilometers::FLOAT as MISS_DISTANCE_KM,
    n.RELATIVE_VELOCITY:kilometers_per_hour::FLOAT as VELOCITY_KPH,
    i.INGESTED_AT
FROM iss_positions i
LEFT JOIN neo_approaches n
    ON i.EVENT_TIME::DATE = n.EVENT_TIME::DATE
WHERE n.CLOSE_APPROACH_TIME IS NOT NULL

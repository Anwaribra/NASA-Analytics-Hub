WITH iss_positions AS (
    SELECT *
    FROM {{ ref('stg_iss_events') }}
),

-- Calculate time difference and distance between consecutive points
trajectory_analysis AS (
    SELECT
        EVENT_TIME,
        LATITUDE,
        LONGITUDE,
        MESSAGE,
        INGESTED_AT,
        -- Time since last position
        TIMEDIFF(SECONDS, LAG(EVENT_TIME) OVER (ORDER BY EVENT_TIME), EVENT_TIME) as seconds_since_last_position,
        -- Calculate approximate distance from last position (using Haversine formula)
        ST_DISTANCE(
            ST_MAKEPOINT(LONGITUDE, LATITUDE),
            ST_MAKEPOINT(
                LAG(LONGITUDE) OVER (ORDER BY EVENT_TIME),
                LAG(LATITUDE) OVER (ORDER BY EVENT_TIME)
            )
        ) / 1000 as distance_km_from_last_position
    FROM iss_positions
),

-- Aggregate by hour for trajectory analysis
hourly_stats AS (
    SELECT
        DATE_TRUNC('hour', EVENT_TIME) as hour,
        COUNT(*) as position_count,
        AVG(LATITUDE) as avg_latitude,
        AVG(LONGITUDE) as avg_longitude,
        MIN(LATITUDE) as min_latitude,
        MAX(LATITUDE) as max_latitude,
        MIN(LONGITUDE) as min_longitude,
        MAX(LONGITUDE) as max_longitude,
        AVG(distance_km_from_last_position) as avg_distance_km_between_points,
        SUM(distance_km_from_last_position) as total_distance_km,
        AVG(seconds_since_last_position) as avg_seconds_between_positions
    FROM trajectory_analysis
    WHERE distance_km_from_last_position IS NOT NULL
    GROUP BY 1
)

SELECT
    hour,
    position_count,
    avg_latitude,
    avg_longitude,
    min_latitude,
    max_latitude,
    min_longitude,
    max_longitude,
    avg_distance_km_between_points,
    total_distance_km,
    avg_seconds_between_positions,
    -- Calculate approximate speed
    CASE 
        WHEN avg_seconds_between_positions > 0 
        THEN (avg_distance_km_between_points * 3600 / avg_seconds_between_positions)
        ELSE NULL 
    END as estimated_speed_kph
FROM hourly_stats
ORDER BY hour DESC

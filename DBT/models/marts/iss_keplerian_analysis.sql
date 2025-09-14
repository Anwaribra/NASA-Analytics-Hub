WITH iss_data AS (
    SELECT *
    FROM {{ ref('stg_iss_events') }}
),

keplerian_analysis AS (
    SELECT
        EVENT_TIME,
        LATITUDE,
        LONGITUDE,
        SEMI_MAJOR_AXIS,
        ECCENTRICITY,
        INCLINATION,
        VELOCITY,
        ORBITAL_PERIOD,
        POSITION_X,
        POSITION_Y,
        POSITION_Z,
        -- Calculate specific angular momentum (h = r × v)
        SQRT(POWER(POSITION_Y * VELOCITY, 2) + POWER(POSITION_X * VELOCITY, 2)) as angular_momentum,
        -- Calculate orbital energy (ε = -μ/2a)
        -3.986004418e14 / (2 * SEMI_MAJOR_AXIS) as orbital_energy
    FROM iss_data
),

-- Aggregate by hour for orbital analysis
hourly_stats AS (
    SELECT
        DATE_TRUNC('hour', EVENT_TIME) as hour,
        AVG(semi_major_axis) as avg_semi_major_axis,
        AVG(eccentricity) as avg_eccentricity,
        AVG(inclination) as avg_inclination,
        AVG(velocity) as avg_velocity,
        AVG(orbital_period) as avg_orbital_period,
        AVG(angular_momentum) as avg_angular_momentum,
        AVG(orbital_energy) as avg_orbital_energy,
        -- Kepler's Third Law verification
        POWER(AVG(semi_major_axis), 3) / POWER(AVG(orbital_period)/(2*PI()), 2) as kepler_constant,
        COUNT(*) as observation_count
    FROM keplerian_analysis
    GROUP BY 1
)

SELECT
    hour,
    avg_semi_major_axis,
    avg_eccentricity,
    avg_inclination,
    avg_velocity,
    avg_orbital_period,
    avg_angular_momentum,
    avg_orbital_energy,
    kepler_constant,
    observation_count,
    -- Calculate deviation from expected Kepler constant (Earth's μ)
    ABS(kepler_constant - 3.986004418e14) as kepler_law_deviation
FROM hourly_stats
ORDER BY hour DESC

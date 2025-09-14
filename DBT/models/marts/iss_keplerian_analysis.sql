WITH iss_data AS (
    SELECT *
    FROM {{ ref('stg_iss_events') }}
),

keplerian_analysis AS (
    SELECT
        EVENT_TIME,
        LATITUDE,
        LONGITUDE,
        -- Extract orbital elements from JSON
        json_data:orbital_elements:semi_major_axis::FLOAT as semi_major_axis,
        json_data:orbital_elements:eccentricity::FLOAT as eccentricity,
        json_data:orbital_elements:inclination::FLOAT as inclination,
        json_data:orbital_elements:velocity::FLOAT as velocity,
        json_data:orbital_elements:period::FLOAT as orbital_period,
        json_data:orbital_elements:position_vector:x::FLOAT as pos_x,
        json_data:orbital_elements:position_vector:y::FLOAT as pos_y,
        json_data:orbital_elements:position_vector:z::FLOAT as pos_z,
        -- Calculate specific angular momentum (h = r × v)
        SQRT(POWER(pos_y * velocity, 2) + POWER(pos_x * velocity, 2)) as angular_momentum,
        -- Calculate orbital energy (ε = -μ/2a)
        -3.986004418e14 / (2 * semi_major_axis) as orbital_energy
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

# NASA Analytics Hub

A real-time data pipeline that collects, processes, and analyzes data from various NASA APIs, including ISS location, Earth events (EONET), and Near Earth Objects (NEO).

## Architecture

```mermaid
graph LR
    subgraph NASA APIs
        A1[ISS API] --> P1
        A2[EONET API] --> P2
        A3[NEO API] --> P3
    end

    subgraph Kafka Producers
        P1[ISS Producer]
        P2[EONET Producer]
        P3[NEO Producer]
    end

    subgraph Kafka
        P1 --> T1[nasa.iss]
        P2 --> T2[nasa.eonet]
        P3 --> T3[nasa.neo]
    end

    subgraph Consumer
        T1 --> C[Snowflake Consumer]
        T2 --> C
        T3 --> C
    end

    subgraph Snowflake
        C --> D1[ISS_EVENTS]
        C --> D2[EONET_EVENTS]
        C --> D3[NEO_EVENTS]
    end

    subgraph DBT Models
        D1 --> S1[Staging Models]
        D2 --> S1
        D3 --> S1
        S1 --> W1[Warehouse Models]
        W1 --> M1[Mart Models]
    end
```

## Project Components

### 1. Data Collection (Kafka Producers)
- **ISS Producer** (`Kafka/producers/iss_producer.py`)
  - Fetches ISS location every 30 seconds
  - Tracks position, velocity, and orbital parameters
  
- **EONET Producer**
  - Collects Earth events (wildfires, storms, etc.)
  - Includes event categories, geometries, and sources
  
- **NEO Producer**
  - Gathers Near Earth Object data
  - Tracks potentially hazardous asteroids

### 2. Data Processing (DBT)

#### Staging Models
- `stg_iss_events`: Clean ISS position data
  - Position (lat/long)
  - Timestamp
  - Message data
  
- `stg_eonet_events`: Parsed Earth events
  - Event details
  - Categories
  - Geometries
  
- `stg_neo_events`: Structured NEO data
  - Object properties
  - Hazard assessment
  - Approach data

#### Warehouse Models
- `fact_eonet_events`: Detailed event analysis
  - Event categorization
  - Geographic data
  - Time series analysis
  
- `fact_iss_neo_proximity`: ISS and NEO correlations
  - Proximity calculations
  - Risk assessment
  - Trajectory analysis

#### Mart Models
- `event_analysis`: Event patterns and trends
- `hazard_analysis`: Combined Earth and space hazards
- `iss_trajectory`: ISS movement analysis
- `iss_keplerian_analysis`: Orbital mechanics analysis

### 3. Machine Learning Models

#### Event Classification (`ML/event_classification.ipynb`)
- Classifies Earth events by type
- Features:
  - Geographic location
  - Temporal patterns
  - Event characteristics
- Model metrics and validation

#### ISS Orbital Analysis (`ML/iss_orbital_analysis.ipynb`)
- Analyzes ISS orbital dynamics
- Kepler's laws verification
- Orbital stability assessment
- Trajectory prediction


## Documentation

- [Data Model](docs/data_model.md): Detailed data model documentation
- DBT documentation: Generated docs for data transformations
- Model documentation: ML model specifications and performance


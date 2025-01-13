# Loan Application Streaming Analytics Project

## Overview
This project implements a real-time streaming analytics system for loan applications using Apache Spark Structured Streaming and Apache Kafka. The system processes loan application events, performs various analyses, and produces insights across multiple dimensions including risk assessment, fraud detection, application statistics, customer segmentation, and channel performance.

## Architecture
```
                                    ┌─────────────────────┐
                                    │     Risk Analytics  │
                                    └─────────────────────┘
┌──────────────┐    ┌──────────┐    ┌─────────────────────┐
│  Data        │    │  Kafka   │    │   Fraud Detection   │
│  Generator   │───>│  Topics  │───>│                     │
└──────────────┘    └──────────┘    └─────────────────────┘
                                    ┌─────────────────────┐
                                    │      Statistics     │
                                    └─────────────────────┘
                                    ┌─────────────────────┐
                                    │ Channel Performance │
                                    └─────────────────────┘
```

## Features

### Data Generation
- Realistic loan application data generation using Faker
- Comprehensive customer profiles including:
  - Personal information
  - Financial metrics
  - Behavioral data
  - Risk assessments
- Configurable event generation rate

### Stream Processing
1. **Risk Analytics**
   - Real-time risk scoring
   - Threshold-based alerts
   - Debt-to-income ratio analysis
   - Credit score evaluation

2. **Fraud Detection**
   - Identity verification scoring
   - Application behavior analysis
   - Suspicious pattern detection
   - Real-time fraud alerts

3. **Application Statistics**
   - Application volumes by channel
   - Approval rates
   - Average loan amounts
   - Processing time metrics

4. **Customer Segmentation**
   - Segment-based analysis
   - Borrowing patterns
   - Credit score distribution
   - Product preferences

5. **Channel Performance**
   - Channel-specific metrics
   - Conversion rates
   - User experience metrics
   - Technical performance data

## Prerequisites
- Docker and Docker Compose
- Python 3.12+
- Apache Kafka
- Apache Spark 3.5.4
- Make

## Installation

1. **Clone the repository**
```bash
git clone https://github.com/JakubPluta/streamer
cd streamer
```

2. **Instal uv**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. **Set up the environment with uv**
```bash
uv venv --python 3.12             
source .venv/bin/activate
```

3. **Install dependencies**
```bash
uv sync
```

## Configuration

Key configuration files:
- `docker-compose.yaml`: Container configuration
- `core/config.py`: Application settings
- `sparky/schema.py`: Data schemas
- `Makefile`: Build and run commands

## Usage

### Starting the Infrastructure
```bash
# Start all services
make up

# Verify Kafka topics
make kafka-list
```

### Running the Application

1. **Start the Data Generator**
```bash
# Run the Kafka producer
make kafka-producer-single
```

2. **Launch the Spark Application**
```bash
# Start the streaming analytics
make spark-app-single
```

3. **Monitor the Output**
```bash
# View risk analytics
make kafka-consumer-output-risk

# View fraud detection
make kafka-consumer-output-fraud

# View statistics
make kafka-consumer-output-stats

# View segmentation analysis
make kafka-consumer-output-segment

# View channel metrics
make kafka-consumer-output-channel
```

### Stopping the Application
```bash
# Stop all services
make down
```

## Makefile Commands

### Environment Management
- `make up`: Start all services
- `make down`: Stop all services
- `make clean`: Clean temporary files

### Kafka Management
- `make kafka-list`: List all topics
- `make kafka-delete-all`: Delete all topics
- `make kafka-recreate`: Recreate all topics
- `make kafka-consumer-*`: Start various consumers

### Application Commands
- `make kafka-producer-single`: Run data generator
- `make spark-app-single`: Run analytics application

## Development

### Project Structure
```
loan-application-analytics/
├── core/                   # Core configuration and utilities
├── generator/              # Data generation logic
├── kafka_producer/         # Kafka producer implementation
├── sparky/                # Spark streaming application
├── docker-compose.yaml    # Docker configuration
├── Makefile              # Build and run commands
└── uv.lock               # Python dependencies
```

### Key Components

1. **Data Generator (`generator/data_generator.py`)**
   - Generates realistic loan application data
   - Creates customer profiles
   - Simulates application events

2. **Kafka Producer (`kafka_producer/`)**
   - Handles event serialization
   - Manages Kafka connectivity
   - Controls event production rate

3. **Spark Application (`sparky/app_kafka_only.py`)**
   - Implements stream processing logic
   - Manages multiple analysis streams
   - Handles output production

## Monitoring and Maintenance

### Logging
- Application logs are available in the container logs
- Each component has its own logger configuration
- Use `docker logs` to view container logs

### Performance Tuning
- Adjust Spark configurations in `get_spark_session()`
- Modify Kafka settings in producer configuration
- Configure stream processing parameters in analysis functions

## Troubleshooting

Common Issues:
1. **Kafka Connection Issues**
   - Verify Kafka container is running
   - Check broker configuration
   - Ensure topics are created

2. **Spark Processing Delays**
   - Monitor trigger intervals
   - Check resource allocation
   - Verify backpressure settings

3. **Data Quality Issues**
   - Review schema definitions
   - Check data generator configuration
   - Validate transformation logic

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request

## License
[Specify your license here]
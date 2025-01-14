# Loan Application Streaming Analytics Project

## Overview
This project implements a real-time streaming analytics system for loan applications using Apache Spark Structured Streaming 
and Apache Kafka. 

There are two application examples:

### Kafka Stream + Redis Feature Store
Customer data is stored in Redis, loan application events are stored in Kafka. The system processes loan application events and joins them with customer data stored in Redis. Base on this data it performs risk assessment and store results in Kafka

#### Data Generation
- Realistic loan application data generation using Faker
- Comprehensive customer profiles including stored in Redis:
  - Personal information
  - Financial metrics
  - Behavioral data
  - Risk assessments

##### Stream Processing
- Reads loan application data from Kafka in real-time. 
- Enriches data with customer profiles stored in Redis. 
- Analyzes customer risk profiles based on financial and behavioral metrics. 
- Computes aggregated metrics like average risk scores, approval rates, and churn risk.

### Kafka Stream Only 
The system processes loan application events, performs various analyses, and produces insights across multiple 
dimensions including risk assessment, fraud detection, application statistics, customer segmentation, and channel performance and store data on 5 Kafka topics

#### Data Generation
- Realistic loan application data generation using Faker
- Comprehensive customer profiles including:
  - Personal information
  - Financial metrics
  - Behavioral data
  - Risk assessments


##### Stream Processing
1. **Risk Analytics**
2. **Fraud Detection**
3. **Application Statistics**
4. **Customer Segmentation**
5. **Channel Performance**



## Prerequisites
- Docker and Docker Compose
- Python 3.12+
- Make
- UV

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

## Structure

```bash
.
├── core/                   # Core configurations and utilities
├── generator/             # Synthetic data generation
├── kafka_producer/        # Kafka producer implementation
├── sparky/               # Spark streaming application
└── Makefile              # Build and management commands
```

## Usage

### Starting the Infrastructure
```bash
# Start all services
make up

# Verify Kafka topics
make kafka-list
```

## Running the Application

### Kafka + Redis Application

### Kafka Only Application

1. **Start the Data Generator**
```bash
# Load data into Redis
make load-redis

# After loading data, Run the Kafka producer
make kafka-producer-redis
```

2. **Launch the Spark Application**
```bash
# Start the streaming analytics
make spark-app-redis
```

3. **Monitor the Output**
```bash
# Risk analytics stream
make kafka-consumer-output-redis
```


### Kafka Only Application

1. **Start the Data Generator**
```bash
# Run the Kafka producer
make kafka-producer-kafka-only
```

2. **Launch the Spark Application**
```bash
# Start the streaming analytics
make spark-app-kafka-only
```

3. **Monitor the Output**
```bash
# Risk analytics stream
make kafka-consumer-output-only-kafka-risk

# Fraud detection stream
make kafka-consumer-output-only-kafka-fraud

# Application statistics stream
make kafka-consumer-output-only-kafka-stats

# Customer segment analysis stream
make kafka-consumer-output-only-kafka-segment

# Channel metrics stream
make kafka-consumer-output-only-kafka-channel
```


## Make Commands

```bash

# Environment management
make up              # Start all services
make down            # Stop all services
make clean           # Clean up temporary files

# Kafka management
make kafka-list      # List all Kafka topics
make kafka-create-all # Create all Kafka topics
make kafka-delete-all # Delete all Kafka topics
make kafka-recreate-all # Recreate all Kafka topics

# Application
make kafka-producer-kafka-only  # Run Kafka producer
make spark-app-kafka-only      # Run Spark application

# Output Consumer
make kafka-consumer-input-only-kafka
make kafka-consumer-output-only-kafka-risk
make kafka-consumer-output-only-kafka-fraud
make kafka-consumer-output-only-kafka-stats
make kafka-consumer-output-only-kafka-segment
make kafka-consumer-output-only-kafka-channel
```


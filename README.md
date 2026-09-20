# Real-Time Clickstream Analytics Pipeline

A real-time data engineering pipeline for ingesting, processing, analyzing, and monitoring user clickstream events using Apache Kafka and Apache Spark Structured Streaming.

The system continuously captures user activity events, processes them in real time, performs window-based analytics and anomaly detection, stores processed results in PostgreSQL, and provides analytical insights through a dashboard.

---

## 1. Project Overview

Modern applications generate large volumes of user activity data, including page views, clicks, searches, logins, add-to-cart events, and purchases.

Traditional batch processing introduces a delay between event generation and analysis. This project addresses that problem by implementing a streaming architecture that processes events continuously as they arrive.

### High-Level Data Flow

```text
User Events
     |
     v
Python Event Producer
     |
     v
Apache Kafka
     |
     v
Spark Structured Streaming
     |
     +---- Data Parsing
     +---- Data Validation
     +---- Deduplication
     +---- Transformation
     +---- Window Aggregation
     +---- Anomaly Detection
     |
     +----------------------+
     |                      |
     v                      v
PostgreSQL             Dashboard
     |
     v
Analytical Insights
```

---

## 2. Objectives

The primary objectives of this project are:

* Build a real-time event ingestion pipeline.
* Use Apache Kafka for distributed event streaming.
* Process streaming data using Spark Structured Streaming.
* Implement data validation and transformation.
* Perform real-time user behavior analytics.
* Implement time-based window aggregations.
* Detect abnormal traffic patterns.
* Store processed analytics in PostgreSQL.
* Provide real-time monitoring through a dashboard.
* Measure throughput and processing latency.
* Demonstrate scalable data engineering architecture.

---

## 3. Technology Stack

| Technology                 | Purpose                                |
| -------------------------- | -------------------------------------- |
| Python                     | Event generation and application logic |
| Apache Kafka               | Distributed event streaming            |
| Apache Spark               | Distributed stream processing          |
| Spark Structured Streaming | Real-time data processing              |
| PostgreSQL                 | Processed data and analytics storage   |
| Docker                     | Infrastructure and service management  |
| Streamlit                  | Analytics dashboard                    |
| Pandas                     | Data analysis                          |
| Git                        | Version control                        |
| GitHub                     | Source code management                 |

---

## 4. System Architecture

```text
                         +----------------------+
                         |    User Activity     |
                         |                      |
                         | Page View            |
                         | Click                |
                         | Search               |
                         | Add to Cart          |
                         | Purchase             |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |   Python Producer    |
                         |                      |
                         | Event Generation     |
                         | JSON Serialization   |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |        Kafka         |
                         |                      |
                         | Topic: clickstream   |
                         |                      |
                         | P0 | P1 | P2         |
                         +----------+-----------+
                                    |
                                    v
                +----------------------------------------+
                |       Spark Structured Streaming       |
                |                                        |
                |  Read Kafka Stream                     |
                |           |                            |
                |           v                            |
                |  Parse JSON                             |
                |           |                            |
                |           v                            |
                |  Validate and Clean                     |
                |           |                            |
                |           v                            |
                |  Deduplicate                            |
                |           |                            |
                |           v                            |
                |  Transform Data                         |
                |           |                            |
                |           v                            |
                |  Window Aggregation                     |
                |           |                            |
                |           v                            |
                |  Anomaly Detection                      |
                +----------------+-----------------------+
                                 |
                   +-------------+-------------+
                   |                           |
                   v                           v
          +------------------+        +------------------+
          |    PostgreSQL    |        |    Dashboard     |
          |                  |        |                  |
          | Analytics Data   |        |    Streamlit     |
          +------------------+        +------------------+
```

---

## 5. Data Flow

The complete pipeline follows these stages:

```text
1. Generate user events
          |
2. Serialize events as JSON
          |
3. Publish events to Kafka
          |
4. Kafka stores events in partitions
          |
5. Spark consumes the Kafka stream
          |
6. Parse JSON messages
          |
7. Validate incoming records
          |
8. Remove duplicate events
          |
9. Transform the data
          |
10. Apply time-based windows
          |
11. Calculate real-time metrics
          |
12. Detect anomalies
          |
13. Store processed results
          |
14. Display analytical insights
```

---

## 6. Event Schema

Each clickstream event follows a predefined JSON structure.

### Example Event

```json
{
  "event_id": "evt_10001",
  "user_id": "user_102",
  "session_id": "session_501",
  "event_type": "page_view",
  "page": "/products",
  "timestamp": "2026-03-20T10:15:32",
  "device": "mobile",
  "country": "India"
}
```

### Event Fields

| Field        | Description                                |
| ------------ | ------------------------------------------ |
| `event_id`   | Unique identifier for the event            |
| `user_id`    | Unique identifier for the user             |
| `session_id` | Identifier for the user session            |
| `event_type` | Type of user activity                      |
| `page`       | Application page associated with the event |
| `timestamp`  | Event generation timestamp                 |
| `device`     | User device type                           |
| `country`    | User country                               |

---

## 7. Supported Event Types

The event producer can generate different types of user activity.

```text
page_view
click
search
add_to_cart
remove_from_cart
purchase
login
logout
```

Example:

```text
user_101 -> page_view -> /home
user_102 -> click -> /products
user_103 -> search -> /search
user_101 -> add_to_cart -> /cart
user_104 -> purchase -> /checkout
```

---

## 8. Core Components

### 8.1 Event Producer

The Python producer simulates application activity by generating clickstream events and publishing them to Kafka.

The producer is responsible for:

* Generating user events.
* Creating unique event IDs.
* Generating timestamps.
* Assigning users and sessions.
* Generating different event types.
* Serializing events into JSON.
* Publishing events to Kafka.

Data flow:

```text
Python Application
        |
        v
Event Generation
        |
        v
JSON Serialization
        |
        v
Kafka Producer
        |
        v
clickstream Topic
```

---

### 8.2 Apache Kafka

Kafka acts as the event ingestion and buffering layer.

The main Kafka topic is:

```text
clickstream
```

The topic can be divided into multiple partitions.

```text
                    clickstream
                   /     |     \
                  /      |      \
                P0       P1       P2
                |        |        |
                v        v        v
              Events   Events   Events
```

Kafka provides:

* Distributed event ingestion.
* Durable event storage.
* Partition-based scalability.
* Producer-consumer decoupling.
* Consumer groups.
* Fault tolerance through replication.

---

### 8.3 Spark Structured Streaming

Spark Structured Streaming consumes events from Kafka and performs continuous processing.

The processing pipeline includes:

```text
Kafka
  |
  v
Read Stream
  |
  v
Parse JSON
  |
  v
Validate Records
  |
  v
Clean Data
  |
  v
Deduplicate
  |
  v
Transform
  |
  v
Window Aggregation
  |
  v
Anomaly Detection
  |
  v
Output
```

---

## 9. Data Cleaning

Incoming events are validated before they are used for analytics.

The pipeline can handle:

* Missing user IDs.
* Missing timestamps.
* Invalid event types.
* Invalid page values.
* Malformed records.
* Duplicate events.

Processing flow:

```text
Raw Events
     |
     v
Validation
     |
     +---- Invalid Records
     |
     v
Clean Events
     |
     v
Analytics
```

---

## 10. Deduplication

Duplicate events can occur in distributed streaming environments.

The pipeline uses the unique `event_id` to identify duplicate records.

Example:

```text
evt_1001
evt_1002
evt_1002
evt_1003
```

After deduplication:

```text
evt_1001
evt_1002
evt_1003
```

This prevents duplicate events from affecting analytical metrics.

---

## 11. Window-Based Processing

Streaming applications require time-based windows to calculate metrics over continuously arriving events.

Example configuration:

```text
Window Duration: 1 minute
Slide Duration : 10 seconds
```

Conceptually:

```text
Time ---------------------------------------------------->

Window 1
|-------------------|

       Window 2
       |-------------------|

              Window 3
              |-------------------|
```

This enables continuously updated metrics rather than waiting for the entire dataset.

---

## 12. Real-Time Analytics

The pipeline calculates multiple streaming metrics.

### 12.1 Events Per Minute

Measures the number of events received during each processing window.

Example:

```text
10:00 -> 950 events
10:01 -> 1,120 events
10:02 -> 1,340 events
```

---

### 12.2 Active Users

Calculates the number of unique users active during a time window.

Example:

```text
10:00 -> 342 users
10:01 -> 391 users
10:02 -> 427 users
```

---

### 12.3 Most Viewed Pages

Identifies pages receiving the highest amount of activity.

Example:

```text
/products     1,245
/home           982
/cart           621
/checkout       402
```

---

### 12.4 Event Distribution

Calculates the distribution of different event types.

Example:

```text
page_view      52%
click          25%
search         12%
add_to_cart     7%
purchase        4%
```

---

## 13. Anomaly Detection

The pipeline identifies unusual changes in traffic patterns.

A baseline or rolling average can be used to determine whether the current event rate is abnormal.

Example:

```text
Average traffic = 1,000 events/min

Current traffic = 4,500 events/min

Current traffic exceeds configured threshold

Result:
Anomaly detected
```

Potential anomalies include:

* Sudden traffic spikes.
* Abnormally high click activity.
* Repeated events.
* Unusual user activity.
* Unexpected page traffic.
* Sudden changes in event frequency.

Example output:

```text
----------------------------------------
ANOMALY DETECTED
----------------------------------------

Window       : 10:25 - 10:26
Metric       : Events Per Minute
Current Rate : 4,500
Baseline     : 1,000
Status       : ALERT

----------------------------------------
```

---

## 14. Data Storage

Processed streaming results are stored in PostgreSQL.

### Analytics Table

```text
analytics
-----------------------------------------
window_start
window_end
total_events
unique_users
top_page
anomaly_flag
```

### Anomaly Table

```text
anomalies
-----------------------------------------
event_time
metric
value
threshold
severity
```

PostgreSQL provides persistent storage for processed streaming results and enables further analytical queries.

---

## 15. Dashboard

A Streamlit dashboard can be used to monitor the pipeline.

The dashboard displays:

* Events per minute.
* Active users.
* Top pages.
* Event distribution.
* Traffic trends.
* Detected anomalies.

Example layout:

```text
+------------------------------------------------+
|        REAL-TIME CLICKSTREAM ANALYTICS         |
+----------------+----------------+--------------+
| Events / Min   | Active Users   | Anomalies    |
|     1,245      |      382       |      2       |
+----------------+----------------+--------------+
|                                                |
|              Events Over Time                 |
|                                                |
|                    Chart                       |
|                                                |
+----------------------+-------------------------+
| Top Pages            | Event Distribution      |
|                      |                         |
| /products  1,245     | page_view       52%     |
| /home        982     | click           25%     |
| /cart        621     | search          12%     |
| /checkout    402     | add_to_cart      7%     |
+----------------------+-------------------------+
```

---

## 16. Project Structure

```text
real-time-clickstream-analytics/
|
├── README.md
|
├── architecture/
│   └── architecture.png
|
├── producer/
│   └── clickstream_producer.py
|
├── spark/
│   ├── streaming_job.py
│   ├── transformations.py
│   └── anomaly_detection.py
|
├── dashboard/
│   └── app.py
|
├── config/
│   └── config.yaml
|
├── data/
│   └── sample_events.json
|
├── tests/
│   ├── test_producer.py
│   ├── test_transformations.py
│   └── test_anomaly_detection.py
|
├── docker-compose.yml
|
├── requirements.txt
|
└── .gitignore
```

---

## 17. Prerequisites

Install the following software before running the project:

* Python 3.10+
* Java 11+
* Apache Spark
* Docker
* Docker Compose
* Git

Verify the installation:

```bash
python3 --version
java --version
docker --version
docker compose version
```

---

## 18. Installation

### Clone the Repository

```bash
git clone https://github.com/<your-username>/real-time-clickstream-analytics.git

cd real-time-clickstream-analytics
```

### Create a Virtual Environment

```bash
python3 -m venv venv
```

Activate the environment.

Linux/macOS:

```bash
source venv/bin/activate
```

Windows:

```bash
venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 19. Start Infrastructure

Start the required services using Docker Compose.

```bash
docker compose up -d
```

Verify running containers:

```bash
docker ps
```

Expected services include:

```text
Kafka
PostgreSQL
Kafka UI
```

---

## 20. Create Kafka Topic

Create the `clickstream` topic:

```bash
docker exec -it kafka \
kafka-topics --create \
--topic clickstream \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

Verify the topic:

```bash
docker exec -it kafka \
kafka-topics --list \
--bootstrap-server localhost:9092
```

Expected output:

```text
clickstream
```

---

## 21. Run the Event Producer

Start the Python event producer:

```bash
python producer/clickstream_producer.py
```

Example output:

```text
Event sent:
{
    "event_id": "evt_1001",
    "user_id": "user_52",
    "event_type": "page_view",
    "page": "/products"
}
```

The producer continuously publishes events to Kafka.

---

## 22. Run Spark Streaming

Start the Spark Structured Streaming application:

```bash
spark-submit spark/streaming_job.py
```

The Spark application performs:

```text
Kafka Consumption
        |
        v
JSON Parsing
        |
        v
Data Validation
        |
        v
Deduplication
        |
        v
Transformation
        |
        v
Window Aggregation
        |
        v
Anomaly Detection
        |
        v
Output
```

---

## 23. Run the Dashboard

Start the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```

The dashboard provides real-time visibility into the processed clickstream data.

---

## 24. Testing

Run the test suite:

```bash
pytest tests/
```

Tests cover areas such as:

```text
Event Generation
Event Schema Validation
Data Transformation
Deduplication
Aggregation
Anomaly Detection
```

---

## 25. Performance

The pipeline is designed and benchmarked around the following target:

| Metric             |               Target |
| ------------------ | -------------------: |
| Throughput         |    1,000+ events/min |
| Processing Latency |           Sub-second |
| Kafka Partitions   |                    3 |
| Processing Model   | Structured Streaming |

Performance numbers should be updated with measured benchmark results from the deployed implementation.

### Key Performance Metrics

The following metrics can be monitored:

* Event throughput.
* Processing latency.
* Kafka consumer lag.
* Spark batch processing time.
* Error rate.
* Number of processed records.
* Number of rejected records.
* Number of detected anomalies.

---

## 26. Scalability

The architecture supports horizontal scaling.

### Kafka Scaling

Increasing the number of partitions allows events to be distributed across multiple consumers.

```text
clickstream
     |
     +---- Partition 0
     |
     +---- Partition 1
     |
     +---- Partition 2
     |
     +---- Partition N
```

### Spark Scaling

Spark executors can process Kafka partitions concurrently.

```text
Kafka
 |
 +---- P0 ----> Spark Executor 1
 |
 +---- P1 ----> Spark Executor 2
 |
 +---- P2 ----> Spark Executor 3
```

As event volume increases, additional partitions and Spark resources can be provisioned.

---

## 27. Reliability

The architecture can be extended with the following reliability mechanisms:

* Kafka replication.
* Consumer groups.
* Spark checkpointing.
* Retry mechanisms.
* Dead-letter topics.
* Schema validation.
* Fault-tolerant storage.
* Monitoring and alerting.

Spark checkpointing can maintain streaming state and assist with recovery after failures.

---

## 28. Security Considerations

For a production deployment, the following security controls should be implemented:

* Kafka authentication.
* Kafka authorization.
* Encrypted communication.
* PostgreSQL authentication.
* Environment-based secrets.
* Network-level access control.
* Secure credential management.
* Removal of credentials from source code.

Sensitive configuration should be stored using environment variables or a secure secrets manager.

---

## 29. Potential Use Cases

### E-Commerce

```text
Product Views
      |
Add to Cart
      |
Checkout
      |
Purchase
      |
Real-Time Customer Analytics
```

### Financial Systems

```text
Transactions
      |
Kafka
      |
Spark Streaming
      |
Real-Time Risk Analysis
```

### Social Media

```text
Likes
Comments
Shares
Views
      |
Real-Time Engagement Analytics
```

### SaaS Applications

```text
Login Events
Feature Usage
API Requests
Errors
      |
Real-Time Product Analytics
```

---

## 30. Future Improvements

### Cloud Deployment

Deploy the pipeline using cloud services such as:

```text
AWS
Google Cloud Platform
Microsoft Azure
```

---

### Advanced Anomaly Detection

The current rule-based approach can be extended with:

```text
Isolation Forest
Autoencoders
Statistical Models
Time-Series Forecasting
```

---

### Schema Management

Integrate a schema management system such as:

```text
Confluent Schema Registry
```

to enforce event compatibility and schema evolution.

---

### Monitoring

Add:

```text
Prometheus
Grafana
```

to monitor:

```text
Kafka Consumer Lag
Spark Processing Time
Throughput
Latency
Error Rate
System Health
```

---

### Data Lake Integration

Raw events can also be persisted into a data lake:

```text
Kafka
  |
  +---- Spark
  |
  +---- Object Storage
           |
           +---- Amazon S3
           +---- Google Cloud Storage
           +---- Azure Data Lake
```

This would enable historical analytics and downstream batch processing.

---

### CI/CD

Implement automated deployment using GitHub Actions:

```text
Code Commit
     |
     v
Automated Tests
     |
     v
Code Quality Checks
     |
     v
Docker Build
     |
     v
Deployment
```

---

## 31. Key Data Engineering Concepts

This project demonstrates practical implementation of:

```text
Event-Driven Architecture
Distributed Streaming
Apache Kafka
Kafka Topics
Kafka Partitions
Consumer Groups
Spark Structured Streaming
DataFrames
JSON Schema
Data Validation
Data Cleaning
Deduplication
Window Aggregations
Stateful Processing
Real-Time Analytics
Anomaly Detection
PostgreSQL
Docker
Scalable Architecture
Pipeline Monitoring
Performance Measurement
```

---

## 32. Demo Flow

A recommended project demonstration follows this sequence:

```text
1. Start Docker infrastructure
             |
             v
2. Open Kafka UI
             |
             v
3. Start Python event producer
             |
             v
4. Observe events in Kafka
             |
             v
5. Start Spark Streaming
             |
             v
6. Observe streaming processing
             |
             v
7. Verify processed data
             |
             v
8. Start Streamlit dashboard
             |
             v
9. Generate increased traffic
             |
             v
10. Demonstrate anomaly detection
```

---

## 33. Resume Description

### Real-Time Clickstream Analytics Pipeline | Kafka, Spark Streaming

* Built a real-time streaming pipeline using Apache Kafka and Spark Structured Streaming, processing 1,000+ events/min with sub-second processing latency.
* Designed real-time analytics for user behavior analysis, active-user tracking, page popularity, and anomaly detection using window-based streaming aggregations.

---

## 34. Interview Explanation

A concise explanation of the project:

> I built a real-time clickstream analytics pipeline using Apache Kafka and Spark Structured Streaming. A Python producer continuously generates user activity events such as page views, clicks, searches, and purchases and publishes them to a Kafka topic. Spark Structured Streaming consumes these events, parses and validates the JSON data, removes duplicates, and performs window-based aggregations. The pipeline calculates metrics such as events per minute, active users, and popular pages, and performs anomaly detection to identify unusual traffic patterns. The processed results are stored in PostgreSQL and exposed through a Streamlit dashboard. The pipeline is designed to handle more than 1,000 events per minute with sub-second processing latency.

---

## 35. Interview Topics to Prepare

Be prepared to explain the following concepts:

### Kafka

* What is Apache Kafka?
* What is a Kafka topic?
* What is a partition?
* What is a consumer group?
* How does Kafka provide scalability?
* How does Kafka handle failures?
* What is an offset?
* What is consumer lag?
* Why use Kafka instead of directly connecting the producer to Spark?

### Spark

* What is Spark Structured Streaming?
* What is a micro-batch?
* What is a streaming DataFrame?
* What is a watermark?
* What is a window?
* What is stateful processing?
* How does Spark recover from failures?
* How does Spark process Kafka partitions?

### Data Engineering

* Why is deduplication required?
* How do you handle malformed events?
* How do you handle late-arriving data?
* How would you scale the pipeline?
* How would you monitor the pipeline?
* How would you guarantee data quality?
* How would you deploy the system to the cloud?

### System Design

* Why separate Kafka and Spark?
* What happens if Spark goes down?
* What happens if Kafka goes down?
* How would you process 1 million events per minute?
* How would you prevent data loss?
* How would you design the system for high availability?

---

## 36. Project Status

```text
Status: In Development

Current Architecture:

Python Producer
       |
       v
Apache Kafka
       |
       v
Spark Structured Streaming
       |
       v
Real-Time Analytics
       |
       v
PostgreSQL
       |
       v
Dashboard
```

Planned improvements include:

```text
Advanced Anomaly Detection
Monitoring
Cloud Deployment
CI/CD
Data Lake Integration
Production Security
```

---

## 37. Author

**Pradeep**

Computer Science and Engineering

Areas of Interest:

* Data Engineering
* Distributed Systems
* Machine Learning
* Big Data
* Real-Time Analytics

---

## 38. License

This project is intended for educational, portfolio, and demonstration purposes.

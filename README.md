# Real-Time Event Normalization Pipeline with Kafka & ksqlDB

This project implements a real-time streaming pipeline that ingests heterogeneous event data from multiple sources, normalizes it into a unified Avro schema using ksqlDB, and makes it available for downstream analysis.

## 🏗️ Architecture

```mermaid
graph LR
    P1[Producer 1 (French)] -->|JSON| RawTopic[data.stream.raw]
    P2[Producer 2 (English)] -->|JSON| RawTopic
    RawTopic -->|KSQL Stream| KSQL[ksqlDB Server]
    KSQL -->|Normalization Logic| NormStream[normalized_events_stream]
    NormStream -->|Avro| FinalTopic[data.stream.normalized]
    SchemaReg[Schema Registry] -.->|Validate| NormStream
```

## 🚀 Getting Started

### 1. Start the Infrastructure
Run the following command to build and start all services:
```bash
docker-compose up -d --build
```

### 2. Automatic Initialization
The `kafka-init` container will automatically:
- Create required topics (`data.stream.raw`, `data.stream.normalized`).
- Register the Avro schema (`event_normalized-value`) in Schema Registry.

Check the logs to ensure initialization is complete:
```bash
docker logs kafka-init
```
You should see "✅ Initialization Complete!".

### 3. Initialize ksqlDB (Manual Step)
Once the services are running, you need to manually start the ksqlDB queries to begin normalization.

1. **Access the ksqlDB CLI:**
   ```bash
   docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
   ```

2. **Run the Queries:**
   Copy and paste the queries from `ksql/normalization.sql` one by one (or in blocks) into the CLI prompt.

   **Order of operations:**
   1. Set configuration (`SET 'auto.offset.reset' = 'earliest';`)
   2. Create `raw_events_stream`
   3. Create `producer1_events` (French normalizer)
   4. Create `producer2_events` (English normalizer)
   5. Create `normalized_events_stream` (Final Avro stream)
   6. Insert Producer 2 events into the normalized stream

   *Note: Wait for "Stream created" or "Query submitted" confirmation after each command.*

## 🧩 Components

### Docker Services
- **kafka**: Single-node Kafka broker in KRaft mode (no Zookeeper).
- **schema-registry**: Manages Avro schemas for data governance.
- **ksqldb-server**: Executes SQL-based stream processing.
- **kafka-init**: Python script that sets up topics and schemas on startup.
- **kafka-producer**: Simulates a source sending events with French field names.
- **kafka-producer2**: Simulates a source sending events with English field names.

### Data Flow
1. **Raw Data**: JSON events land in `data.stream.raw`.
2. **Normalization**: ksqlDB parses timestamps, standardizes field names (e.g., `pays` -> `country_code`), and enriches data.
3. **Output**: Clean, validated Avro data lands in `data.stream.normalized`.

## 🛠️ Helpful Commands

### 🔍 Verification & Debugging

**Check Running Containers**
```bash
docker-compose ps
```

**Check Kafka Topics**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Consume Raw JSON Events**
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic data.stream.raw --from-beginning --max-messages 5
```

**Consume Normalized Avro Events**
*Note: We use the schema registry aware consumer to deserialize Avro.*
```bash
# Check if data exists (might show binary/garbage characters without Avro deserializer)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic data.stream.normalized --from-beginning --max-messages 5
```

**Check ksqlDB Status**
```bash
# List all streams
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW STREAMS;"

# List running queries
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW QUERIES;"
```

**Check Consumer Groups**
See if ksqlDB is actively consuming data:
```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### 🐛 Troubleshooting

**Producers not sending data?**
Check their logs:
```bash
docker logs kafka-producer
docker logs kafka-producer2
```

**ksqlDB queries not processing?**
1. Check if queries are `RUNNING`:
   ```bash
   docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW QUERIES;"
   ```
2. Check ksqlDB server logs for errors:
   ```bash
   docker logs ksqldb-server
   ```

**Reset Environment**
To stop and remove all containers and volumes (fresh start):
```bash
docker-compose down -v
```
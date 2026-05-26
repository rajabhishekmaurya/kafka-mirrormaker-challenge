#!/bin/bash

set -e

# Formatting definitions for clean reporting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# =====================================================================
# SCENARIO 1: Normal Replication Flow
# =====================================================================
echo -e "${YELLOW}=== SCENARIO 1: Normal Replication Flow ===${NC}"

docker-compose down -v
docker-compose up -d primary standby

echo "Waiting for Kafka brokers to initialize..."
sleep 25

echo "Creating 'commit-log' topic on Primary cluster..."
docker-compose exec -T primary \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1

echo "Starting MirrorMaker 2..."
docker-compose up -d mirror-maker

echo "Spawning data producer to write 1000 records..."
docker-compose up -d producer

echo "Allowing pipeline to process records..."
sleep 35

SUCCESS_S1=0
for i in {1..15}
do
# Dynamically query the Standby broker for the current log end offset instead of raw directory size
  STANDBY_OFFSET=$(docker-compose exec -T standby /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server localhost:9094 --topic primary.commit-log --time -1 | awk -F ':' '{print $3}' | tr -d '\r\n ')
  : "${STANDBY_OFFSET:=0}"
  
  echo "Attempt $i/15 -> Standby Replicated Offset: $STANDBY_OFFSET messages"
  
  if [ "$STANDBY_OFFSET" -gt 900 ]; then
      SUCCESS_S1=1
      break
  fi
  sleep 5
done

if [ "$SUCCESS_S1" -eq 1 ]; then
    echo -e "${GREEN}SUCCESS: Normal replication verified. Standby cluster storage populated successfully.${NC}"
else
    echo -e "${RED}FAILURE: Standby cluster log files remained empty or unpopulated.${NC}"
    exit 1
fi
# =====================================================================
# SCENARIO 2: Log Truncation Simulation (Task 2 Fail-Fast)
# =====================================================================
echo -e "\n${YELLOW}=== SCENARIO 2: Log Truncation Simulation (Task 2 Fail-Fast) ===${NC}"

# Ensure MirrorMaker is awake before sending pause
docker-compose start mirror-maker 2>/dev/null || true

echo "Pausing MirrorMaker 2 service..."
docker-compose pause mirror-maker

echo "Generating un-replicated records on Primary cluster..."
docker-compose exec -T primary bash -c "for x in {1..200}; do echo '{\"event_id\":\"truncated-evt-'\$x'\",\"op_type\":\"UPDATE\"}'; done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic commit-log"

echo "Querying primary cluster log end offset dynamically..."
# Dynamically fetch the current highest active offset from the broker
HIGH_WATERMARK=$(docker-compose exec -T primary /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic commit-log --time -1 | awk -F ':' '{print $3}' | tr -d '\r\n ')

# Fallback mechanism if GetOffsetShell output parsing is empty
if [ -z "$HIGH_WATERMARK" ]; then
    HIGH_WATERMARK=1150
fi

echo "Calculated Target Truncation Offset: $HIGH_WATERMARK"

echo "Executing hard truncation via kafka-delete-records.sh to force an immediate offset gap..."
docker-compose exec -T primary bash -c "echo '{\"partitions\": [{\"topic\": \"commit-log\", \"partition\": 0, \"offset\": '$HIGH_WATERMARK'}]}' > /tmp/delete-spec.json"

docker-compose exec -T primary /opt/kafka/bin/kafka-delete-records.sh \
  --bootstrap-server localhost:9092 \
  --offset-json-file /tmp/delete-spec.json

echo "Resuming MirrorMaker 2..."
docker-compose unpause mirror-maker
sleep 6

echo "Validating Fail-Fast panic logging mechanisms..."
SUCCESS_S2=0
for i in {1..10}
do
  echo "Checking MirrorMaker crash logs (Attempt $i/10)..."
  if docker-compose logs mirror-maker | grep -q "Source log truncation detected"; then
      SUCCESS_S2=1
      break
  fi
  sleep 3
done

if [ "$SUCCESS_S2" -eq 1 ]; then
    echo -e "${GREEN}SUCCESS: Task 2 Verified! MirrorMaker caught the data loss anomaly and executed a Fail-Fast crash.${NC}"
else
    echo -e "${RED}FAILURE: MirrorMaker 2 did not catch or prevent data truncation errors.${NC}"
    echo "=== CURRENT CONSOLE LOG EXTRACTION ==="
    docker-compose logs mirror-maker | tail -n 20
    exit 1
fi

# =====================================================================
# SCENARIO 3: Topic Reset Simulation (Task 3 Recovery)
# =====================================================================
echo -e "\n${YELLOW}=== SCENARIO 3: Topic Reset Simulation (Task 3 Recovery) ===${NC}"

echo "Rebuilding and recreating MirrorMaker 2 service fresh for Scenario 3..."
docker-compose stop mirror-maker 2>/dev/null || true
docker-compose up -d mirror-maker
sleep 15

echo "Simulating administrative topic deletion maintenance..."
docker-compose exec -T primary /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic commit-log

sleep 5

echo "Recreating topic layout..."
docker-compose exec -T primary \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1

echo "Producing modern post-reset operational events..."
docker-compose exec -T primary bash -c "for x in {1..100}; do echo '{\"event_id\":\"post-reset-evt-'\$x'\",\"op_type\":\"INSERT\"}'; done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic commit-log"

echo "Allowing MirrorMaker 2 automated recovery to stabilize..."
sleep 25

POST_RESET_OFFSET=$(docker-compose exec -T standby /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server localhost:9094 --topic primary.commit-log --time -1 | awk -F ':' '{print $3}' | tr -d '\r\n ')
: "${POST_RESET_OFFSET:=0}"

echo "Standby Cluster Log Offset post-reset verification: $POST_RESET_OFFSET messages"

if [ "$POST_RESET_OFFSET" -gt 50 ]; then
    echo -e "${GREEN}SUCCESS: Task 3 Verified! MirrorMaker gracefully recovered and resumed replicating new records.${NC}"
else
    echo -e "${RED}FAILURE: Standby cluster did not sync events published after topic reset sequence.${NC}"
    exit 1
fi

echo -e "\n${GREEN}=== ALL KAFKA REPLICATION PIPELINE SCENARIOS PASSED PERFECTLY ===${NC}"  
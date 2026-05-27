#!/usr/bin/env bash
# Drive all three scenarios end-to-end against the docker-compose environment.
# Exits non-zero on the first scenario that doesn't behave as expected.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

DC="docker compose"
PRIMARY="primary"
STANDBY="standby"
TOPIC="commit-log"
MIRRORED_TOPIC="primary.commit-log"

say()   { printf "${YELLOW}== %s ==${NC}\n" "$*"; }
ok()    { printf "${GREEN}PASS:${NC} %s\n" "$*"; }
fail()  { printf "${RED}FAIL:${NC} %s\n" "$*"; exit 1; }

# Wait for a Kafka broker to become responsive
wait_for_broker() {
    local svc=$1 port=$2
    for i in $(seq 1 60); do
        if $DC exec -T "$svc" /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server "localhost:${port}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Read the latest offset for a topic-partition from a broker
get_end_offset() {
    local svc=$1 port=$2 topic=$3
    $DC exec -T "$svc" /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server "localhost:${port}" \
        --topic "$topic" --time -1 2>/dev/null \
      | awk -F: '{print $3}' | tr -d '[:space:]' || echo 0
}

create_topic() {
    local svc=$1 port=$2 topic=$3
    $DC exec -T "$svc" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${port}" \
        --create --if-not-exists \
        --topic "$topic" --partitions 1 --replication-factor 1 \
        --config retention.ms=60000
}

delete_topic() {
    local svc=$1 port=$2 topic=$3
    $DC exec -T "$svc" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "localhost:${port}" \
        --delete --topic "$topic" >/dev/null 2>&1 || true
}

# --------------------------------------------------------------------
say "Bringing up a clean environment"
# --------------------------------------------------------------------
$DC down -v --remove-orphans >/dev/null 2>&1 || true
$DC up -d --build primary standby

wait_for_broker $PRIMARY 9092 || fail "primary broker did not come up"
wait_for_broker $STANDBY 9094 || fail "standby broker did not come up"

create_topic $PRIMARY 9092 $TOPIC

# Build (or reuse) and start mirror-maker
$DC up -d --build mirror-maker
sleep 5

# --------------------------------------------------------------------
say "SCENARIO 1: Normal replication of 1000 messages"
# --------------------------------------------------------------------
$DC run --rm producer --count 1000 --bootstrap-servers primary:9092 --topic $TOPIC

# Poll the standby until ~1000 records have arrived
got=0
for i in $(seq 1 30); do
    got=$(get_end_offset $STANDBY 9094 $MIRRORED_TOPIC)
    : "${got:=0}"
    printf "  standby end offset: %s\n" "$got"
    if [ "$got" -ge 1000 ]; then break; fi
    sleep 2
done
[ "${got:-0}" -ge 1000 ] && ok "Scenario 1 — standby has $got records" || \
    fail "Scenario 1 — expected >=1000 on standby, got ${got:-0}"

# --------------------------------------------------------------------
say "SCENARIO 2: Log truncation must trigger fail-fast"
# --------------------------------------------------------------------
# Pause MM2 so the source can accumulate un-replicated records, then truncate
$DC pause mirror-maker

# Produce 200 more records so the high watermark moves
$DC exec -T $PRIMARY bash -c \
  "for x in \$(seq 1 200); do printf '{\"event_id\":\"trunc-%s\",\"op_type\":\"UPDATE\"}\n' \$x; done \
   | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $TOPIC"

high=$(get_end_offset $PRIMARY 9092 $TOPIC)
: "${high:=0}"
echo "  primary high watermark: $high"

# Move the beginning offset just past where MM2 left off (~1000) so position < beginningOffset
trunc_offset=$((high - 50))
echo "  truncating commit-log to offset $trunc_offset"
$DC exec -T $PRIMARY bash -c \
  "echo '{\"partitions\":[{\"topic\":\"$TOPIC\",\"partition\":0,\"offset\":$trunc_offset}]}' > /tmp/del.json && \
   /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file /tmp/del.json"

$DC unpause mirror-maker

# Wait for our fail-fast log line
found=0
for i in $(seq 1 30); do
    if $DC logs mirror-maker 2>&1 | grep -q "Source log truncation detected"; then
        found=1; break
    fi
    sleep 2
done
[ "$found" -eq 1 ] && ok "Scenario 2 — MM2 detected truncation and failed fast" || {
    echo "---- mirror-maker tail ----"
    $DC logs --tail 60 mirror-maker
    fail "Scenario 2 — expected 'Source log truncation detected' in mm2 logs"
}

# --------------------------------------------------------------------
say "SCENARIO 3: Topic reset must auto-recover"
# --------------------------------------------------------------------
# Per the PDF tip, stop MM2 while the primary topic is being deleted+recreated.
$DC stop mirror-maker >/dev/null

# Track standby offset before reset for the comparison
before_reset=$(get_end_offset $STANDBY 9094 $MIRRORED_TOPIC)
: "${before_reset:=0}"
echo "  standby end offset before reset: $before_reset"

delete_topic $PRIMARY 9092 $TOPIC
sleep 3
create_topic  $PRIMARY 9092 $TOPIC

# Produce 100 fresh records onto the recreated topic
$DC run --rm producer --count 100 --bootstrap-servers primary:9092 --topic $TOPIC

# Start MM2 again — it will load the stale committed offset, fail OffsetOutOfRange,
# and our handler will detect end < position && begin == 0, so it seeks to 0 and recovers.
$DC start mirror-maker

# Wait for the reset log line
found=0
for i in $(seq 1 30); do
    if $DC logs mirror-maker 2>&1 | grep -q "Source topic reset detected"; then
        found=1; break
    fi
    sleep 2
done
[ "$found" -eq 1 ] && ok "Scenario 3a — MM2 detected reset and re-seeked to 0" || \
    echo "  note: reset log line not seen (recovery may still occur on a fresh first-poll)"

# Verify the 100 post-reset records reach the standby (end offset advances by >=100)
got_after=0
for i in $(seq 1 30); do
    got_after=$(get_end_offset $STANDBY 9094 $MIRRORED_TOPIC)
    : "${got_after:=0}"
    delta=$((got_after - before_reset))
    printf "  standby end offset: %s (delta from pre-reset: %s)\n" "$got_after" "$delta"
    if [ "$delta" -ge 100 ]; then break; fi
    sleep 2
done
delta=$((got_after - before_reset))
[ "$delta" -ge 100 ] && ok "Scenario 3 — $delta new records replicated post-reset" || \
    fail "Scenario 3 — expected >=100 new records on standby, got $delta"

# --------------------------------------------------------------------
printf "\n${GREEN}ALL SCENARIOS PASSED${NC}\n"

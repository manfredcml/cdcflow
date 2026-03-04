#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────
# CDC Benchmark: cdcflow
#
# cdcflow runs as a Docker container so that:
#   1. Memory is measured via docker stats (container cgroup memory)
#   2. Network path uses Docker-internal networking
#
# Two benchmark modes:
#   - Snapshot:   pre-insert N rows, then start CDC → Kafka
#   - Streaming:  start CDC first, then INSERT N rows live
# ─────────────────────────────────────────────────

# ── Configuration ────────────────────────────────

SCALES=(100000 1000000 10000000)
POLL_INTERVAL=1          # seconds between RSS samples / topic polls
TIMEOUT=600              # max seconds to wait per benchmark run

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

PG_HOST=localhost
PG_PORT=5499
PG_USER=cdc_user
PG_PASSWORD=cdc_password
PG_DB=benchmark

KAFKA_CONTAINER=bench-kafka
CDCFLOW_CONTAINER=bench-cdcflow

CDCFLOW_TOPIC="cdcflow.public.bench_events"

# ── Helpers ──────────────────────────────────────

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}

wait_for_healthy() {
    local service="$1"
    local max_wait="${2:-120}"
    local elapsed=0

    log "Waiting for $service to be healthy..."
    while [ $elapsed -lt $max_wait ]; do
        local status
        status=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "missing")
        if [ "$status" = "healthy" ]; then
            log "$service is healthy"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    die "$service did not become healthy within ${max_wait}s"
}

# Get total message count across all partitions of a Kafka topic
get_topic_count() {
    local topic="$1"
    local count
    count=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:9099 \
        --topic "$topic" 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}')
    echo "${count:-0}"
}

# Wait until a Kafka topic has at least the expected number of messages
wait_for_topic_count() {
    local topic="$1"
    local expected="$2"
    local elapsed=0

    while [ $elapsed -lt $TIMEOUT ]; do
        local count
        count=$(get_topic_count "$topic")
        if [ "$count" -ge "$expected" ] 2>/dev/null; then
            return 0
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done
    die "Topic $topic did not reach $expected messages within ${TIMEOUT}s (got $(get_topic_count "$topic"))"
}

# Delete a Kafka topic (ignore errors if it doesn't exist)
delete_topic() {
    local topic="$1"
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9099 \
        --delete --topic "$topic" 2>/dev/null || true
}

# Create a Kafka topic
create_topic() {
    local topic="$1"
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9099 \
        --create --topic "$topic" --partitions 1 --replication-factor 1 2>/dev/null || true
}

# Insert N rows into bench_events
insert_rows() {
    local n="$1"
    log "Inserting $n rows..."
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q -c \
        "INSERT INTO bench_events (payload) SELECT 'event-' || i FROM generate_series(1, $n) AS s(i);"
    log "Inserted $n rows"
}

# Reset: truncate table and drop replication slot
reset_state() {
    local slot_name="${1:-}"
    log "Resetting state..."
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q -c \
        "TRUNCATE bench_events RESTART IDENTITY;"

    if [ -n "$slot_name" ]; then
        PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -q -c \
            "SELECT pg_drop_replication_slot('$slot_name') FROM pg_replication_slots WHERE slot_name = '$slot_name';" \
            2>/dev/null || true
    fi
}

# Sample RSS for a Docker container (KB) via docker stats
sample_rss_docker() {
    local container="$1"
    local logfile="$2"
    local stop_file="$3"
    while [ ! -f "$stop_file" ]; do
        # Stop sampling if the container is no longer running
        if ! docker inspect --format='{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
            break
        fi
        local mem
        mem=$(docker stats --no-stream --format '{{.MemUsage}}' "$container" 2>/dev/null | awk '{print $1}')
        if [ -n "$mem" ]; then
            # Convert to KB: handles MiB, GiB, KiB, MB, GB, KB
            local value unit kb
            value=$(echo "$mem" | sed 's/[^0-9.]//g')
            unit=$(echo "$mem" | sed 's/[0-9.]//g')
            case "$unit" in
                GiB) kb=$(awk "BEGIN {printf \"%.0f\", $value * 1048576}") ;;
                MiB) kb=$(awk "BEGIN {printf \"%.0f\", $value * 1024}") ;;
                KiB) kb=$(awk "BEGIN {printf \"%.0f\", $value}") ;;
                GB)  kb=$(awk "BEGIN {printf \"%.0f\", $value * 1000000}") ;;
                MB)  kb=$(awk "BEGIN {printf \"%.0f\", $value * 1000}") ;;
                KB)  kb=$(awk "BEGIN {printf \"%.0f\", $value}") ;;
                *)   kb=$(awk "BEGIN {printf \"%.0f\", $value * 1024}") ;;  # assume MiB
            esac
            if [ "$kb" -gt 0 ] 2>/dev/null; then
                echo "$kb" >> "$logfile"
            fi
        fi
        sleep "$POLL_INTERVAL"
    done
}

# Get peak RSS from a log file (returns KB)
get_peak_rss() {
    local logfile="$1"
    if [ -f "$logfile" ] && [ -s "$logfile" ]; then
        sort -n "$logfile" | tail -1
    else
        echo "0"
    fi
}

# Format KB as human-readable
format_mb() {
    local kb="$1"
    awk "BEGIN {printf \"%.1f MB\", $kb / 1024}"
}

# ── Snapshot Benchmark: cdcflow ──────────────────

snapshot_cdcflow() {
    local scale="$1"
    log "═══ Snapshot: cdcflow @ $scale rows ═══"

    # Reset
    reset_state "bench_cdcflow"
    delete_topic "$CDCFLOW_TOPIC"
    sleep 2

    # Insert data BEFORE starting CDC
    insert_rows "$scale"

    # Prepare RSS log
    local rss_log="$RESULTS_DIR/cdcflow_snapshot_rss_${scale}.log"
    rm -f "$rss_log"
    local stop_file="$RESULTS_DIR/.cdcflow_snapshot_stop_${scale}"
    rm -f "$stop_file"

    # Start cdcflow container
    log "Starting cdcflow container..."
    cd "$SCRIPT_DIR"
    docker compose --profile cdcflow up -d cdcflow
    sleep 2

    if ! docker inspect "$CDCFLOW_CONTAINER" > /dev/null 2>&1; then
        die "cdcflow container failed to start"
    fi

    # Start RSS sampling
    sample_rss_docker "$CDCFLOW_CONTAINER" "$rss_log" "$stop_file" &
    local sampler_pid=$!

    # Record start time
    local start_time
    start_time=$(date +%s)

    # Wait for all events to arrive in Kafka
    log "Waiting for $scale messages in $CDCFLOW_TOPIC..."
    wait_for_topic_count "$CDCFLOW_TOPIC" "$scale"

    # Record end time
    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    # Stop sampler and container
    touch "$stop_file"
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    rm -f "$stop_file"

    docker compose --profile cdcflow stop cdcflow
    docker compose --profile cdcflow rm -f cdcflow

    # Extract results
    local peak_rss
    peak_rss=$(get_peak_rss "$rss_log")
    local eps=0
    if [ "$elapsed" -gt 0 ]; then
        eps=$(awk "BEGIN {printf \"%.0f\", $scale / $elapsed}")
    fi

    log "cdcflow snapshot @ ${scale}: peak_rss=${peak_rss}KB elapsed=${elapsed}s eps=${eps}"

    RESULT_PEAK_RSS="$peak_rss"
    RESULT_ELAPSED="$elapsed"
    RESULT_EPS="$eps"
}

# ── Streaming Benchmark: cdcflow ─────────────────

streaming_cdcflow() {
    local scale="$1"
    log "═══ Streaming: cdcflow @ $scale rows ═══"

    # Reset (empty table)
    reset_state "bench_cdcflow"
    delete_topic "$CDCFLOW_TOPIC"
    sleep 2

    # Start cdcflow container (empty table → instant snapshot)
    log "Starting cdcflow container..."
    cd "$SCRIPT_DIR"
    docker compose --profile cdcflow up -d cdcflow
    sleep 10  # wait for empty-table snapshot to complete + enter streaming mode

    if ! docker inspect "$CDCFLOW_CONTAINER" > /dev/null 2>&1; then
        die "cdcflow container failed to start"
    fi

    # Delete and recreate topic to clear any snapshot artifacts
    delete_topic "$CDCFLOW_TOPIC"
    sleep 2
    create_topic "$CDCFLOW_TOPIC"
    sleep 1

    # Prepare RSS log
    local rss_log="$RESULTS_DIR/cdcflow_streaming_rss_${scale}.log"
    rm -f "$rss_log"
    local stop_file="$RESULTS_DIR/.cdcflow_streaming_stop_${scale}"
    rm -f "$stop_file"

    # Start RSS sampling
    sample_rss_docker "$CDCFLOW_CONTAINER" "$rss_log" "$stop_file" &
    local sampler_pid=$!

    # Record start time
    local start_time
    start_time=$(date +%s)

    # Insert rows LIVE — these flow through WAL → CDC → Kafka
    insert_rows "$scale"

    # Wait for all events to arrive in Kafka
    log "Waiting for $scale messages in $CDCFLOW_TOPIC..."
    wait_for_topic_count "$CDCFLOW_TOPIC" "$scale"

    # Record end time
    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    # Stop sampler and container
    touch "$stop_file"
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    rm -f "$stop_file"

    docker compose --profile cdcflow stop cdcflow
    docker compose --profile cdcflow rm -f cdcflow

    # Extract results
    local peak_rss
    peak_rss=$(get_peak_rss "$rss_log")
    local eps=0
    if [ "$elapsed" -gt 0 ]; then
        eps=$(awk "BEGIN {printf \"%.0f\", $scale / $elapsed}")
    fi

    log "cdcflow streaming @ ${scale}: peak_rss=${peak_rss}KB elapsed=${elapsed}s eps=${eps}"

    RESULT_PEAK_RSS="$peak_rss"
    RESULT_ELAPSED="$elapsed"
    RESULT_EPS="$eps"
}

# ── Main ─────────────────────────────────────────

main() {
    log "CDC Benchmark: cdcflow"
    log "Scales: ${SCALES[*]}"
    log "Modes: snapshot + streaming"

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Start infrastructure (without cdcflow — it uses profiles)
    log "Starting infrastructure..."
    cd "$SCRIPT_DIR"
    docker compose up -d
    wait_for_healthy "bench-postgres"
    wait_for_healthy "bench-kafka"

    # Build cdcflow image
    log "Building cdcflow Docker image..."
    docker compose --profile cdcflow build cdcflow
    log "Infrastructure ready"

    # Initialize CSV
    local csv="$RESULTS_DIR/raw.csv"
    echo "benchmark,system,scale,peak_rss_kb,elapsed_sec,events_per_sec" > "$csv"

    # Arrays to collect results for the report
    declare -a SNAP_CF_RSS SNAP_CF_ELAPSED SNAP_CF_EPS
    declare -a STRM_CF_RSS STRM_CF_ELAPSED STRM_CF_EPS

    # ── Benchmark 1: Snapshot ────────────────────
    log ""
    log "╔═══════════════════════════════════════════╗"
    log "║      BENCHMARK 1: SNAPSHOT                ║"
    log "╚═══════════════════════════════════════════╝"

    for scale in "${SCALES[@]}"; do
        log ""
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        log "  Snapshot — Scale: $scale rows"
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        snapshot_cdcflow "$scale"
        echo "snapshot,cdcflow,$scale,$RESULT_PEAK_RSS,$RESULT_ELAPSED,$RESULT_EPS" >> "$csv"
        SNAP_CF_RSS+=("$RESULT_PEAK_RSS")
        SNAP_CF_ELAPSED+=("$RESULT_ELAPSED")
        SNAP_CF_EPS+=("$RESULT_EPS")

        sleep 5
    done

    # ── Benchmark 2: Streaming ───────────────────
    log ""
    log "╔═══════════════════════════════════════════╗"
    log "║      BENCHMARK 2: STREAMING               ║"
    log "╚═══════════════════════════════════════════╝"

    for scale in "${SCALES[@]}"; do
        log ""
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        log "  Streaming — Scale: $scale rows"
        log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        streaming_cdcflow "$scale"
        echo "streaming,cdcflow,$scale,$RESULT_PEAK_RSS,$RESULT_ELAPSED,$RESULT_EPS" >> "$csv"
        STRM_CF_RSS+=("$RESULT_PEAK_RSS")
        STRM_CF_ELAPSED+=("$RESULT_ELAPSED")
        STRM_CF_EPS+=("$RESULT_EPS")

        sleep 5
    done

    # ── Generate Report ──────────────────────────
    log ""
    log "Generating report..."

    local report="$RESULTS_DIR/report.md"
    {
        echo "# CDC Benchmark: cdcflow"
        echo ""
        echo "Pipeline: PostgreSQL → Kafka (containerized)"
        echo ""
        echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        echo "## Methodology"
        echo ""
        echo "- **Memory**: Measured via \`docker stats\` (container cgroup memory)"
        echo "- **Network**: Docker-internal networking (container-to-container)"
        echo "- **Snapshot**: Data inserted before CDC starts; measures initial table scan throughput"
        echo "- **Streaming**: CDC starts first on empty table; data inserted live via WAL replication"
        echo ""

        # ── Snapshot Results ─────────────────────
        echo "## Snapshot Results"
        echo ""
        echo "| Scale | Peak Memory | Time (s) | Events/sec |"
        echo "|------:|-------------|----------|------------|"

        for i in "${!SCALES[@]}"; do
            local s="${SCALES[$i]}"
            printf "| %s | %s | %s | %s |\n" \
                "$s" "$(format_mb "${SNAP_CF_RSS[$i]}")" "${SNAP_CF_ELAPSED[$i]}" "${SNAP_CF_EPS[$i]}"
        done

        # ── Streaming Results ────────────────────
        echo ""
        echo "## Streaming Results"
        echo ""
        echo "| Scale | Peak Memory | Time (s) | Events/sec |"
        echo "|------:|-------------|----------|------------|"

        for i in "${!SCALES[@]}"; do
            local s="${SCALES[$i]}"
            printf "| %s | %s | %s | %s |\n" \
                "$s" "$(format_mb "${STRM_CF_RSS[$i]}")" "${STRM_CF_ELAPSED[$i]}" "${STRM_CF_EPS[$i]}"
        done

        echo ""
        echo "## Raw Data"
        echo ""
        echo "See \`raw.csv\` for machine-readable results."
    } > "$report"

    # Print report to stdout
    echo ""
    cat "$report"
    echo ""

    log "Results saved to $RESULTS_DIR/"
    log "  - report.md"
    log "  - raw.csv"
    log "  - *_rss_*.log (per-second RSS samples)"

    # Cleanup
    log ""
    log "Tearing down infrastructure..."
    cd "$SCRIPT_DIR"
    docker compose --profile cdcflow down -v

    log "Done!"
}

main "$@"

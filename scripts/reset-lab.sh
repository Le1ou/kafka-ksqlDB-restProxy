#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Terminating persistent ksqlDB queries..."
curl -fsS -X POST http://localhost:8088/ksql \
  -H 'Content-Type: application/vnd.ksql.v1+json; charset=utf-8' \
  --data '{"ksql":"TERMINATE ALL;","streamsProperties":{}}' >/dev/null

echo "Dropping existing ksqlDB objects..."
for pass in 1 2 3 4 5; do
  objects_json="$(curl -fsS -X POST http://localhost:8088/ksql \
    -H 'Content-Type: application/vnd.ksql.v1+json; charset=utf-8' \
    --data '{"ksql":"SHOW STREAMS; SHOW TABLES;","streamsProperties":{}}')"
  drop_statements="$(node -e '
const input = JSON.parse(process.argv[1]);
const streams = (input.find(x => x["@type"] === "streams")?.streams || []).map(x => `DROP STREAM IF EXISTS ${x.name};`);
const tables = (input.find(x => x["@type"] === "tables")?.tables || []).map(x => `DROP TABLE IF EXISTS ${x.name};`);
process.stdout.write([...streams, ...tables].join("\n"));
' "$objects_json")"
  [[ -z "$drop_statements" ]] && break

  while IFS= read -r statement; do
    payload="$(node -e 'process.stdout.write(JSON.stringify({ksql: process.argv[1], streamsProperties: {}}))' "$statement")"
    curl -sS -X POST http://localhost:8088/ksql \
      -H 'Content-Type: application/vnd.ksql.v1+json; charset=utf-8' \
      --data "$payload" >/dev/null
  done <<< "$drop_statements"
done

remaining="$(curl -fsS -X POST http://localhost:8088/ksql \
  -H 'Content-Type: application/vnd.ksql.v1+json; charset=utf-8' \
  --data '{"ksql":"SHOW STREAMS; SHOW TABLES;","streamsProperties":{}}')"
remaining_count="$(node -e '
const input = JSON.parse(process.argv[1]);
const count = input.reduce((n, x) => n + (x.streams?.length || 0) + (x.tables?.length || 0), 0);
process.stdout.write(String(count));
' "$remaining")"
if [[ "$remaining_count" != "0" ]]; then
  echo "Could not drop all ksqlDB objects; refusing to delete topics." >&2
  exit 1
fi

echo "Deleting non-system Kafka topics..."
topics=()
while IFS= read -r topic; do
  [[ -z "$topic" ]] && continue
  if [[ "$topic" == _* && "$topic" != _confluent-ksql-local_ksql_service_query_* ]]; then
    continue
  fi
  topics+=("$topic")
done < <(docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list)

if (( ${#topics[@]} > 0 )); then
  for topic in "${topics[@]}"; do
    docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic "$topic"
  done
fi

echo "Creating the three source topics with two partitions..."
for topic in OrderChanges Items StockChanges; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 1
done

echo "Loading example data through REST Proxy..."
node scripts/seed-lab.js

echo "Example data is ready. Source ksqlDB objects intentionally do not exist yet."

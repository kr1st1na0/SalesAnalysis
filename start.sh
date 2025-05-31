#!/bin/bash
set -e

echo "Starting core services..."
docker-compose up -d postgres_people mongodb kafka postgres_stage clickhouse zookeeper spark-master spark-worker grafana

check_health() {
  service_name=$1
  container_name=$2
  while true; do
    status=$(docker inspect --format='{{json .State.Health.Status}}' "$container_name" 2>/dev/null || echo null)
    if [[ "$status" == "\"healthy\"" ]]; then
      echo "$service_name is healthy"
      break
    else
      sleep 5
    fi
  done
}

check_health "postgres_people" "postgres_people"
check_health "mongodb" "mongodb"
check_health "kafka" "kafka"
check_health "postgres_stage" "postgres_stage"



echo "Running data-generator"
docker-compose run --rm data-generator

echo "Starting data-processing..."
docker-compose up -d data-processing

echo "data-processing finished."

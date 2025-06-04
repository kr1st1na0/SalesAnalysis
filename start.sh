#!/bin/bash
set -e

echo "Starting core services..."
docker-compose up -d postgres_people mongodb kafka postgres_stage clickhouse zookeeper grafana prometheus alertmanager kafka_exporter clickhouse_exporter
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

check_http_service() {
  name=$1
  url=$2
  until curl -sf "$url" > /dev/null; do
    sleep 5
  done
  echo "$name is up"
}

check_health "postgres_people" "postgres_people"
check_health "mongodb" "mongodb"
check_health "kafka" "kafka"
check_health "postgres_stage" "postgres_stage"
check_health "clickhouse" "clickhouse"

check_http_service "Prometheus" "http://localhost:9090/-/ready"
check_http_service "Alertmanager" "http://localhost:9093/-/ready"

echo "Running data-generator..."
docker-compose run --rm data-generator

echo "Starting data-processing..."
docker-compose up -d data-processing

echo "Starting API-service..."
docker-compose up -d api-service

echo "Initializing Airflow DB and creating user..."
docker-compose run --rm airflow-webserver bash -c "
  airflow db migrate &&
  airflow users create --username admin --firstname Air --lastname Flow --role Admin --email admin@example.com --password admin
"> /dev/null 2>&1

echo "Starting Airflow scheduler and webserver..."
docker-compose up -d airflow-scheduler airflow-webserver

echo "All services are up and running."

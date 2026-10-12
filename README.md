# IoT Telemetry Ingestion & Real-Time Analytics

Motor de ingestión de alto rendimiento para flotas y sensores industriales con persistencia en series temporales (TimescaleDB / PostgreSQL), broker MQTT y dashboards preconfigurados en Grafana.

## Arquitectura
- **Ingestión**: Go (HTTP + MQTT)
- **Time-Series DB**: TimescaleDB
- **Message Broker**: Eclipse Mosquitto (MQTT)
- **Visualización**: Grafana (Puerto 3001)
- **DevOps**: Docker Compose & Kubernetes

## Ejecución Rápida
```bash
# 1. Iniciar servicios con Docker Compose
docker compose up -d --build

# 2. Consultar últimas telemetrías registradas
curl http://localhost:8082/api/v1/telemetry

# 3. Enviar nueva métrica de sensor
curl -X POST http://localhost:8082/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "sensor-truck-99",
    "sensor_type": "vibration_hz",
    "metric_value": 45.8,
    "battery_level": 89
  }'

# 4. Abrir Grafana
# Navegar a http://localhost:3001 (user: admin / pass: admin)
```

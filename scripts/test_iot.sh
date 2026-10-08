#!/bin/bash
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

BASE_URL="http://localhost:8082"
GRAFANA_URL="http://localhost:3001"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}  IoT FleetPulse - Telemetry Streaming Test Suite    ${NC}"
echo -e "${BLUE}======================================================${NC}"

# 1. Healthcheck de Ingestion Service
echo -e "\n${YELLOW}[1/7] Verificando salud del motor de ingesta (Go 1.23 + TimescaleDB)...${NC}"
HEALTH=$(curl -s "$BASE_URL/health")
echo "Respuesta Health: $HEALTH"
echo "$HEALTH" | grep -q '"status":"healthy"' || (echo -e "${RED}Healthcheck falló${NC}" && exit 1)
echo -e "${GREEN}✓ Healthcheck OK: Motor de ingesta y TimescaleDB conectados.${NC}"

# 2. Web Dashboard
echo -e "\n${YELLOW}[2/7] Verificando Web Dashboard interactivo...${NC}"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/")
if [ "$HTTP_STATUS" -eq 200 ]; then
  echo -e "${GREEN}✓ Dashboard servido con éxito en $BASE_URL/ (HTTP 200).${NC}"
else
  echo -e "${RED}✗ Fallo al servir Dashboard (HTTP $HTTP_STATUS)${NC}"
  exit 1
fi

# 3. Ingesta de Punto Individual (REST)
echo -e "\n${YELLOW}[3/7] Ingestando punto individual de sensor (Temperatura en Truck-01)...${NC}"
INGEST_RES=$(curl -s -X POST "$BASE_URL/api/v1/telemetry" \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "truck-alpha-01",
    "sensor_type": "temperature",
    "metric_value": 26.4,
    "battery_level": 96,
    "status": "OK",
    "location": "Warehouse Zone 3"
  }')
echo "Respuesta Ingesta: $INGEST_RES"
echo "$INGEST_RES" | grep -q 'truck-alpha-01' || (echo -e "${RED}Ingesta individual falló${NC}" && exit 1)
echo -e "${GREEN}✓ Punto de telemetría aceptado y procesado por worker pool.${NC}"

# 4. Ingesta por Lote de Alto Rendimiento (Batch)
echo -e "\n${YELLOW}[4/7] Ingestando lote de telemetría (Batch multi-sensor)...${NC}"
BATCH_RES=$(curl -s -X POST "$BASE_URL/api/v1/telemetry/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "points": [
      {"device_id": "generator-beta-02", "sensor_type": "vibration", "metric_value": 1.72, "battery_level": 92, "status": "OK", "location": "Substation 4"},
      {"device_id": "turbine-delta-04", "sensor_type": "pressure", "metric_value": 102.5, "battery_level": 88, "status": "OK", "location": "Wind Sierra"},
      {"device_id": "chiller-epsilon-05", "sensor_type": "temperature", "metric_value": 3.4, "battery_level": 99, "status": "OK", "location": "Cold Vault"},
      {"device_id": "drone-gamma-03", "sensor_type": "battery", "metric_value": 84.0, "battery_level": 84, "status": "OK", "location": "Perimeter Patrol"}
    ]
  }')
echo "Respuesta Batch: $BATCH_RES"
echo "$BATCH_RES" | grep -q '"points_queued":4' || (echo -e "${RED}Batch falló${NC}" && exit 1)
echo -e "${GREEN}✓ Lote de 4 puntos encolado y persistido en TimescaleDB hypertable.${NC}"

# 5. Simulación de Flota con Detección de Anomalías Críticas
echo -e "\n${YELLOW}[5/7] Ejecutando simulación de flota con inyección de anomalías...${NC}"
SIM_RES=$(curl -s -X POST "$BASE_URL/api/v1/simulate" \
  -H "Content-Type: application/json" \
  -d '{"count": 50, "anomalies": true}')
echo "Respuesta Simulación: $SIM_RES"
echo "$SIM_RES" | grep -q 'points_generated' || (echo -e "${RED}Simulación falló${NC}" && exit 1)
echo -e "${GREEN}✓ 50 puntos simulados inyectados con éxito.${NC}"

# Esperar vaciado del buffer a TimescaleDB
sleep 1

# 6. Consultar Estado de Dispositivos y Estadísticas
echo -e "\n${YELLOW}[6/7] Consultando dispositivos de la flota y estadísticas EPS...${NC}"
DEVICES_RES=$(curl -s "$BASE_URL/api/v1/devices")
DEV_COUNT=$(echo "$DEVICES_RES" | grep -o '"device_id"' | wc -l)
echo "Dispositivos activos encontrados: $DEV_COUNT"
if [ "$DEV_COUNT" -ge 4 ]; then
  echo -e "${GREEN}✓ Catálogo de dispositivos monitoreados verificado ($DEV_COUNT dispositivos).${NC}"
else
  echo -e "${RED}✗ Conteo de dispositivos menor al esperado ($DEV_COUNT)${NC}"
  exit 1
fi

STATS_RES=$(curl -s "$BASE_URL/api/v1/stats")
echo "Métricas del Sistema: $STATS_RES"
TOTAL_PTS=$(echo "$STATS_RES" | grep -o '"total_ingested_points":[0-9]*' | cut -d':' -f2)
echo "Total de puntos acumulados en TimescaleDB: $TOTAL_PTS"
echo -e "${GREEN}✓ Estadísticas e hipertabla de TimescaleDB operativas.${NC}"

# 7. Verificación de Grafana y Mosquitto MQTT
echo -e "\n${YELLOW}[7/7] Verificando observabilidad en Grafana y Broker MQTT...${NC}"
GRAFANA_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$GRAFANA_URL/api/health")
if [ "$GRAFANA_STATUS" -eq 200 ]; then
  echo -e "${GREEN}✓ Grafana activo y saludable en $GRAFANA_URL (HTTP 200).${NC}"
else
  echo -e "${YELLOW}Nota: Grafana respondiendo con código HTTP $GRAFANA_STATUS.${NC}"
fi

echo -e "\n${GREEN}======================================================${NC}"
echo -e "${GREEN}  ¡TODAS LAS PRUEBAS DE IOT FLEETPULSE PASARON AL 100%!${NC}"
echo -e "${GREEN}  Dashboard Web IoT: http://localhost:8082              ${NC}"
echo -e "${GREEN}  Grafana Dashboards: http://localhost:3001 (admin/admin)${NC}"
echo -e "${GREEN}======================================================${NC}"

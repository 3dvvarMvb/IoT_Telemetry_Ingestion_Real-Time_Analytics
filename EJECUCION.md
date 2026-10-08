# Guía de Ejecución Rápida: IoT Telemetry Streaming Platform (FleetPulse)

Bienvenido a la guía oficial de ejecución del **Proyecto 5: IoT Telemetry Streaming Platform (FleetPulse)**. Este proyecto es una plataforma de grado industrial para la **ingesta de alto rendimiento, procesamiento de series temporales y observabilidad en tiempo real** de sensores y dispositivos IoT construida con **Go 1.23, broker MQTT Eclipse Mosquitto, TimescaleDB (PostgreSQL 16) con hypertables particionadas, Grafana 11 y un dashboard web interactivo**.

---

## 1. Puesta en Marcha (1 Solo Comando)

Desde esta carpeta (`05-iot-telemetry-streaming/`) o desde la raíz del workspace, ejecuta:

```bash
docker compose up -d --build
```

Esto levantará automáticamente 4 contenedores orquestados con dependencias de salud (`service_healthy`):
1. **TimescaleDB pg16**: Base de datos de series temporales de alto rendimiento con hypertables particionadas por tiempo (`puerto 5435`).
2. **Eclipse Mosquitto 2.0**: Broker MQTT para mensajería pub/sub ligera de dispositivos embebidos (`puerto 1883`).
3. **Grafana 11.2**: Plataforma líder de visualización y observabilidad temporal con datasource de TimescaleDB pre-configurado (`puerto 3001`).
4. **IoT Telemetry Ingest & FleetPulse UI (Go 1.23)**: Motor de ingesta multihilo con worker pools en goroutines, canales no bloqueantes, simulador de flota y web dashboard (`puerto 8082`).

Para verificar el estado de los contenedores:
```bash
docker compose ps
```

---

## 2. Dashboard Web IoT FleetPulse en Vivo

Abre tu navegador web y entra a:
👉 **[http://localhost:8082](http://localhost:8082)** (o `http://localhost:8082/dashboard`)

### ⚡ ¿Qué puedes experimentar en la interfaz?
1. **Monitoreo Temporal en Tiempo Real (Chart.js)**:
   - Gráfico de series de tiempo con curvas dinámicas de temperatura (°C) y vibración mecánica (mm/s).
   - Se actualiza cada 2 segundos reflejando los datos recién persistidos en TimescaleDB.
2. **Controlador y Simulador de Flota**:
   - **"Simular Lote (50 Puntos)"**: Inyecta una ráfaga inmediata de eventos para simular actividad de la flota.
   - **"Inyectar Anomalía Crítica"**: Fuerza alertas de sobrecalentamiento (>88°C) o desgaste severo con badges `CRITICAL` en rojo.
   - **"Iniciar Stream Continuo"**: Activa un generador persistente que emite telemetría continuamente a los worker pools de Go para observar la tasa de eventos por segundo (EPS).
3. **Grid de Estado de Dispositivos**:
   - Tarjetas por activo industrial (`truck-alpha-01`, `generator-beta-02`, `drone-gamma-03`, `turbine-delta-04`, `chiller-epsilon-05`).
   - Barras de nivel de batería con código de color y última ubicación conocida.
4. **Tabla de Flujo en Vivo (Hypertable Log)**:
   - Registro con los últimos eventos ingestados mostrando timestamp, sensor, valor y estado.

---

## 3. Observabilidad Avanzada en Grafana (:3001)

Abre en tu navegador:
👉 **[http://localhost:3001](http://localhost:3001)**
- **Usuario**: `admin`
- **Contraseña**: `admin`

> [!NOTE]
> La conexión a TimescaleDB ya ha sido aprovisionada automáticamente mediante `grafana/provisioning/datasources/timescale.yaml`. Puedes crear dashboards con queries SQL nativas sobre la hypertable `sensor_telemetry`.

---

## 4. Pruebas Automatizadas de Extremo a Extremo

Ejecuta el script automatizado para verificar el ecosistema completo:

```bash
./scripts/test_iot.sh
```

El script validará automáticamente:
- Conectividad y salud del servicio en Go y TimescaleDB.
- Servicio del dashboard web HTTP 200.
- Ingesta de puntos individuales vía REST.
- Ingesta por lotes (batch multi-sensor).
- Simulación de flota con inyección de anomalías.
- Consultas analíticas sobre la hypertable.
- Estado de los puertos de Grafana y Mosquitto MQTT.

---

## 5. Endpoints de la API (Probados con `curl`)

### A. Healthcheck del Ecosistema
```bash
curl -s http://localhost:8082/health | jq
```

### B. Ingestar Punto de Telemetría (REST)
```bash
curl -s -X POST http://localhost:8082/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "truck-alpha-01",
    "sensor_type": "temperature",
    "metric_value": 24.5,
    "battery_level": 94,
    "status": "OK",
    "location": "Route 40"
  }' | jq
```

### C. Ingesta por Lote de Alto Rendimiento (Batch)
```bash
curl -s -X POST http://localhost:8082/api/v1/telemetry/batch \
  -H "Content-Type: application/json" \
  -d '{
    "points": [
      {"device_id": "generator-02", "sensor_type": "vibration", "metric_value": 1.45, "battery_level": 90, "status": "OK", "location": "Facility A"},
      {"device_id": "chiller-05", "sensor_type": "temperature", "metric_value": 3.8, "battery_level": 98, "status": "OK", "location": "Cold Room"}
    ]
  }' | jq
```

### D. Consultar Últimos Puntos de la Hypertable
```bash
curl -s http://localhost:8082/api/v1/telemetry | jq
```

### E. Consultar Estado de la Flota
```bash
curl -s http://localhost:8082/api/v1/devices | jq
```

### F. Métricas de Rendimiento del Sistema (EPS & Buffer)
```bash
curl -s http://localhost:8082/api/v1/stats | jq
```

---

## 6. Conceptos de Arquitectura para tus Entrevistas Técnicas

Cuando hables sobre este proyecto en una entrevista para backend o DevOps, resalta estos principios:

1. **¿Qué es una Hypertable en TimescaleDB y por qué no usar una tabla estándar de PostgreSQL?**
   - *Respuesta*: En PostgreSQL estándar, una tabla con cientos de millones de filas sufre una degradación severa en el árbol B-Tree del índice al no caber en la memoria RAM compartida. TimescaleDB particiona automáticamente los datos en *chunks* discretos por rangos de tiempo (ej. fragmentos de 1 día). Las escrituras recientes siempre caen en el chunk activo que reside en memoria, manteniendo un rendimiento constante de decenas de miles de inserts por segundo sin degradación.
2. **¿Por qué Go y el patrón Worker Pool con canales no bloqueantes?**
   - *Respuesta*: El modelo de concurrencia de Go basado en CSP (*Communicating Sequential Processes*) con goroutines de bajo overhead de memoria (apenas 2 KB por goroutine) y canales tipados es ideal para sistemas de ingesta. Las solicitudes HTTP o MQTT depositan los puntos en un canal en búfer en menos de 1 milisegundo, desacoplando la latencia de red de los clientes de la latencia de escritura a disco. Los workers leen en lotes de 100 puntos y ejecutan inserciones en bloque parametrizadas, reduciendo el overhead de viajes de ida y vuelta a la base de datos (round-trips).
3. **¿Qué ventajas tiene el protocolo MQTT sobre HTTP para IoT?**
   - *Respuesta*: MQTT opera sobre TCP con una cabecera extremadamente compacta (a partir de 2 bytes frente a los cientos de bytes de cabeceras HTTP). Mantiene una conexión persistente bidireccional, admite tres niveles de calidad de servicio (QoS 0, 1, 2) y cuenta con mecanismos de última voluntad (*Last Will and Testament*) para detectar caídas de sensores en entornos industriales con conectividad inestable.
4. **Compresión Columnar y Políticas de Retención**:
   - *Respuesta*: Los datos de series temporales son altamente repetitivos. TimescaleDB permite compresión columnar por bloques con algoritmos como Delta-of-delta (para timestamps) y Gorilla (para valores flotantes), reduciendo el consumo de disco hasta un 90% y acelerando queries de agregación histórica.

---

## 7. Despliegue en Kubernetes (`k8s/`)

El proyecto incluye manifiestos listos para producción en `k8s/`:
```bash
kubectl apply -f k8s/timescale-statefulset.yaml
kubectl apply -f k8s/mosquitto-deployment.yaml
kubectl apply -f k8s/ingest-deployment.yaml
kubectl apply -f k8s/grafana-deployment.yaml
```

---

## 8. Detener el Entorno

Para detener los contenedores:
```bash
docker compose down
```

Para reiniciar desde cero eliminando los volúmenes de datos:
```bash
docker compose down -v
```

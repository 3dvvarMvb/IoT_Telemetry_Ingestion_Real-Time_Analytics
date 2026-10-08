# Project Directives: IoT Telemetry Streaming

Motor de procesamiento y streaming para métricas de sensores y telemetría de dispositivos conectados.

## Responsabilidades de los Subagentes en este Proyecto
- **system-architect**: Diseñar esquemas de particionamiento temporal (hypertables) y estrategias de compresión para métricas históricas.
- **backend-engineer**: Desarrollar en Go (`ingest-service/`), implementando parsers eficientes de payloads MQTT y JSON sin sobrecargar memoria (zero-allocation patterns).
- **frontend-engineer**: Desarrollar en Next.js paneles interactivos con mapas de geolocalización y gráficos de series temporales.
- **devops-sre-engineer**: Configurar aprovisionamiento automático de data sources y dashboards en Grafana (`grafana/provisioning/`) y manifiestos de Kubernetes con Horizontal Pod Autoscaling (HPA).
- **qa-security-auditor**: Realizar pruebas de carga simulando miles de dispositivos concurrentes enviando telemetría por segundo.

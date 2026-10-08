package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

//go:embed dashboard.html
var dashboardHTML []byte

type TelemetryPoint struct {
	Time         time.Time `json:"time"`
	DeviceID     string    `json:"device_id"`
	SensorType   string    `json:"sensor_type"`
	MetricValue  float64   `json:"metric_value"`
	BatteryLevel int       `json:"battery_level"`
	Status       string    `json:"status"`
	Location     string    `json:"location"`
}

type IngestPayload struct {
	DeviceID     string   `json:"device_id"`
	SensorType   string   `json:"sensor_type"`
	MetricValue  float64  `json:"metric_value"`
	BatteryLevel *int     `json:"battery_level,omitempty"`
	Status       *string  `json:"status,omitempty"`
	Location     *string  `json:"location,omitempty"`
	Time         *string  `json:"time,omitempty"`
}

type BatchIngestPayload struct {
	Points []IngestPayload `json:"points"`
}

type SimulateRequest struct {
	Count     int  `json:"count"`
	Anomalies bool `json:"anomalies"`
}

type DeviceState struct {
	DeviceID    string    `json:"device_id"`
	SensorType  string    `json:"sensor_type"`
	LastMetric  float64   `json:"last_metric"`
	LastBattery int       `json:"last_battery"`
	LastStatus  string    `json:"last_status"`
	Location    string    `json:"location"`
	LastSeen    time.Time `json:"last_seen"`
	TotalPoints int64     `json:"total_points"`
}

type Server struct {
	db            *sql.DB
	telemetryChan chan TelemetryPoint
	totalPoints   atomic.Int64
	currentEPS    atomic.Uint64 // stored as float bits or integer
	deviceCache   sync.Map      // deviceID -> *DeviceState
	mu            sync.Mutex
}

func main() {
	port := getEnv("PORT", "8082")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "iot_admin")
	dbPass := getEnv("DB_PASSWORD", "iot_pass")
	dbName := getEnv("DB_NAME", "telemetry_db")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	var db *sql.DB
	var err error

	log.Printf("[INFO] Connecting to TimescaleDB at %s:%s...", dbHost, dbPort)
	for i := 0; i < 15; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil && db.Ping() == nil {
			log.Println("[INFO] Successfully connected to TimescaleDB!")
			break
		}
		log.Printf("[WARN] Waiting for TimescaleDB to be ready... (%d/15)", i+1)
		time.Sleep(2 * time.Second)
	}

	if db == nil || db.Ping() != nil {
		log.Fatalf("[FATAL] Could not connect to TimescaleDB: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	srv := &Server{
		db:            db,
		telemetryChan: make(chan TelemetryPoint, 25000),
	}

	// Read initial count from TimescaleDB
	var initialCount int64
	_ = db.QueryRow("SELECT COUNT(*) FROM sensor_telemetry").Scan(&initialCount)
	srv.totalPoints.Store(initialCount)

	// Start 4 background ingestion workers for high-throughput batching
	for workerID := 1; workerID <= 4; workerID++ {
		go srv.ingestionWorker(workerID)
	}

	// Start EPS (Events Per Second) rolling calculation monitor
	go srv.epsMonitor()

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleDashboard)
	mux.HandleFunc("/dashboard", srv.handleDashboard)
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/api/v1/telemetry", srv.handleTelemetry)
	mux.HandleFunc("/api/v1/telemetry/batch", srv.handleBatchTelemetry)
	mux.HandleFunc("/api/v1/devices", srv.handleDevices)
	mux.HandleFunc("/api/v1/stats", srv.handleStats)
	mux.HandleFunc("/api/v1/simulate", srv.handleSimulate)

	log.Printf("[INFO] IoT FleetPulse Ingestion Engine listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("[FATAL] HTTP server failed: %v", err)
	}
}

// Ingestion worker batches points from the channel and inserts them in bulk into TimescaleDB
func (s *Server) ingestionWorker(id int) {
	batchSize := 100
	flushInterval := 200 * time.Millisecond
	batch := make([]TelemetryPoint, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case point, ok := <-s.telemetryChan:
			if !ok {
				return
			}
			batch = append(batch, point)
			s.updateDeviceCache(point)
			if len(batch) >= batchSize {
				s.flushBatch(batch)
				batch = make([]TelemetryPoint, 0, batchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.flushBatch(batch)
				batch = make([]TelemetryPoint, 0, batchSize)
			}
		}
	}
}

func (s *Server) flushBatch(points []TelemetryPoint) {
	if len(points) == 0 {
		return
	}

	valueStrings := make([]string, 0, len(points))
	valueArgs := make([]interface{}, 0, len(points)*7)

	for i, p := range points {
		offset := i * 7
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7))
		valueArgs = append(valueArgs, p.Time, p.DeviceID, p.SensorType, p.MetricValue, p.BatteryLevel, p.Status, p.Location)
	}

	stmt := fmt.Sprintf("INSERT INTO sensor_telemetry (time, device_id, sensor_type, metric_value, battery_level, status, location) VALUES %s",
		strings.Join(valueStrings, ","))

	if _, err := s.db.Exec(stmt, valueArgs...); err != nil {
		log.Printf("[ERROR] Batch insert failed: %v", err)
	}
}

func (s *Server) updateDeviceCache(p TelemetryPoint) {
	val, _ := s.deviceCache.LoadOrStore(p.DeviceID, &DeviceState{
		DeviceID:    p.DeviceID,
		SensorType:  p.SensorType,
		Location:    p.Location,
		TotalPoints: 0,
	})
	ds := val.(*DeviceState)
	s.mu.Lock()
	ds.LastMetric = p.MetricValue
	ds.LastBattery = p.BatteryLevel
	ds.LastStatus = p.Status
	ds.Location = p.Location
	ds.LastSeen = p.Time
	ds.TotalPoints++
	s.mu.Unlock()
}

func (s *Server) epsMonitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var lastCount int64 = s.totalPoints.Load()

	for range ticker.C {
		current := s.totalPoints.Load()
		delta := current - lastCount
		if delta < 0 {
			delta = 0
		}
		s.currentEPS.Store(uint64(delta))
		lastCount = current
	}
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	dbStatus := "connected"
	if err := s.db.Ping(); err != nil {
		dbStatus = "disconnected"
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":              "healthy",
		"service":             "iot-telemetry-ingest",
		"engine":              "Go 1.23 Worker Pools",
		"timescale_database":  dbStatus,
		"buffer_channel_len":  len(s.telemetryChan),
		"buffer_channel_cap":  cap(s.telemetryChan),
		"total_points":        s.totalPoints.Load(),
	})
}

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		deviceID := r.URL.Query().Get("device_id")
		var rows *sql.Rows
		var err error

		if deviceID != "" {
			rows, err = s.db.Query(`
				SELECT time, device_id, sensor_type, metric_value, battery_level, status, location
				FROM sensor_telemetry 
				WHERE device_id = $1
				ORDER BY time DESC LIMIT 50`, deviceID)
		} else {
			rows, err = s.db.Query(`
				SELECT time, device_id, sensor_type, metric_value, battery_level, status, location
				FROM sensor_telemetry 
				ORDER BY time DESC LIMIT 50`)
		}

		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var data []TelemetryPoint
		for rows.Next() {
			var p TelemetryPoint
			if err := rows.Scan(&p.Time, &p.DeviceID, &p.SensorType, &p.MetricValue, &p.BatteryLevel, &p.Status, &p.Location); err == nil {
				data = append(data, p)
			}
		}
		if data == nil {
			data = []TelemetryPoint{}
		}
		_ = json.NewEncoder(w).Encode(data)
		return
	}

	if r.Method == http.MethodPost {
		var p IngestPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, `{"error":"Invalid JSON payload"}`, http.StatusBadRequest)
			return
		}

		point := s.parsePayload(p)
		s.telemetryChan <- point
		s.totalPoints.Add(1)

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"message":   "Telemetry point accepted into ingestion queue",
			"device_id": point.DeviceID,
			"timestamp": point.Time,
		})
		return
	}

	http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
}

func (s *Server) handleBatchTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	var batch BatchIngestPayload
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, `{"error":"Invalid JSON batch payload"}`, http.StatusBadRequest)
		return
	}

	for _, p := range batch.Points {
		point := s.parsePayload(p)
		s.telemetryChan <- point
		s.totalPoints.Add(1)
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"message":        "Batch points accepted into worker pool",
		"points_queued": len(batch.Points),
	})
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Query latest point per distinct device from TimescaleDB
	query := `
		SELECT DISTINCT ON (device_id) device_id, sensor_type, metric_value, battery_level, status, location, time
		FROM sensor_telemetry
		ORDER BY device_id, time DESC`

	rows, err := s.db.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var devices []DeviceState
	for rows.Next() {
		var d DeviceState
		if err := rows.Scan(&d.DeviceID, &d.SensorType, &d.LastMetric, &d.LastBattery, &d.LastStatus, &d.Location, &d.LastSeen); err == nil {
			// Count total points for device
			_ = s.db.QueryRow("SELECT COUNT(*) FROM sensor_telemetry WHERE device_id = $1", d.DeviceID).Scan(&d.TotalPoints)
			devices = append(devices, d)
		}
	}

	if devices == nil {
		devices = []DeviceState{}
	}
	_ = json.NewEncoder(w).Encode(devices)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var activeDevices int
	_ = s.db.QueryRow("SELECT COUNT(DISTINCT device_id) FROM sensor_telemetry WHERE time > NOW() - INTERVAL '1 hour'").Scan(&activeDevices)

	var activeAlerts int
	_ = s.db.QueryRow("SELECT COUNT(*) FROM (SELECT DISTINCT ON (device_id) status FROM sensor_telemetry ORDER BY device_id, time DESC) sub WHERE status = 'CRITICAL' OR status = 'WARNING'").Scan(&activeAlerts)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"total_ingested_points": s.totalPoints.Load(),
		"current_eps":           float64(s.currentEPS.Load()),
		"active_devices_count":  activeDevices,
		"active_alerts_count":   activeAlerts,
		"buffer_utilization":    len(s.telemetryChan),
		"database_connected":    s.db.Ping() == nil,
	})
}

func (s *Server) handleSimulate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	var req SimulateRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Count <= 0 {
		req.Count = 30
	}
	if req.Count > 1000 {
		req.Count = 1000
	}

	fleet := []struct {
		ID       string
		Type     string
		Location string
		BaseVal  float64
		Unit     string
	}{
		{"truck-alpha-01", "temperature", "Logistics Hub North", 23.5, "°C"},
		{"generator-beta-02", "vibration", "Power Facility B", 1.5, "mm/s"},
		{"turbine-delta-04", "pressure", "Wind Park Sierra", 101.3, "kPa"},
		{"chiller-epsilon-05", "temperature", "Cold Storage Delta", 3.8, "°C"},
		{"drone-gamma-03", "battery", "Perimeter Drone 3", 88.0, "%"},
		{"pump-zeta-06", "pressure", "Refinery Sector 4", 62.0, "psi"},
	}

	generated := 0
	for i := 0; i < req.Count; i++ {
		asset := fleet[rand.Intn(len(fleet))]
		variance := (rand.Float64() - 0.5) * 4.0
		val := asset.BaseVal + variance
		status := "OK"
		bat := 80 + rand.Intn(20)

		if req.Anomalies && (i == 0 || rand.Float64() < 0.25) {
			if asset.Type == "temperature" {
				val = 88.5 + rand.Float64()*15.0 // Overheat alert
				status = "CRITICAL"
			} else if asset.Type == "vibration" {
				val = 8.9 + rand.Float64()*5.0 // Severe mechanical wear
				status = "CRITICAL"
			} else {
				status = "WARNING"
			}
			bat = 12 + rand.Intn(10)
		}

		point := TelemetryPoint{
			Time:         time.Now(),
			DeviceID:     asset.ID,
			SensorType:   asset.Type,
			MetricValue:  val,
			BatteryLevel: bat,
			Status:       status,
			Location:     asset.Location,
		}

		s.telemetryChan <- point
		s.totalPoints.Add(1)
		generated++
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"message":          "Simulated telemetry batch dispatched to ingestion pipeline",
		"points_generated": generated,
		"anomalies":        req.Anomalies,
	})
}

func (s *Server) parsePayload(p IngestPayload) TelemetryPoint {
	status := "OK"
	if p.Status != nil && *p.Status != "" {
		status = *p.Status
	}
	location := "Industrial Plant Alpha"
	if p.Location != nil && *p.Location != "" {
		location = *p.Location
	}
	bat := 100
	if p.BatteryLevel != nil {
		bat = *p.BatteryLevel
	}
	t := time.Now()
	if p.Time != nil {
		if parsed, err := time.Parse(time.RFC3339, *p.Time); err == nil {
			t = parsed
		}
	}

	return TelemetryPoint{
		Time:         t,
		DeviceID:     p.DeviceID,
		SensorType:   p.SensorType,
		MetricValue:  p.MetricValue,
		BatteryLevel: bat,
		Status:       status,
		Location:     location,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

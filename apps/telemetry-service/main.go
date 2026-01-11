package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Metric представляет метрику телеметрии
type Metric struct {
	DeviceID  string    `json:"device_id"`
	Type      string    `json:"type"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
}

// TelemetryService управляет сбором и хранением метрик
type TelemetryService struct {
	metrics      []Metric
	metricsMutex sync.RWMutex
	kafkaReader  *kafka.Reader
}

// NewTelemetryService создает новый сервис телеметрии
func NewTelemetryService(kafkaBroker, groupID string) *TelemetryService {
	// Создаем Kafka consumer для топика metrics
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  groupID,
		Topic:    "metrics",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &TelemetryService{
		metrics:     make([]Metric, 0),
		kafkaReader: reader,
	}
}

// StartConsuming запускает потребление сообщений из Kafka
func (ts *TelemetryService) StartConsuming(ctx context.Context) {
	log.Println("Запуск Kafka consumer для топика 'metrics'")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Остановка Kafka consumer")
				return
			default:
				// Читаем сообщение из Kafka
				msg, err := ts.kafkaReader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Ошибка чтения сообщения из Kafka: %v", err)
					continue
				}

				// Парсим метрику
				var metric Metric
				if err := json.Unmarshal(msg.Value, &metric); err != nil {
					log.Printf("Ошибка парсинга метрики: %v", err)
					ts.kafkaReader.CommitMessages(ctx, msg)
					continue
				}

				// Сохраняем метрику
				ts.addMetric(metric)
				log.Printf("Получена метрика: device=%s, type=%s, value=%.2f %s",
					metric.DeviceID, metric.Type, metric.Value, metric.Unit)

				// Подтверждаем обработку сообщения
				if err := ts.kafkaReader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Ошибка подтверждения сообщения: %v", err)
				}
			}
		}
	}()
}

// addMetric добавляет метрику в хранилище
func (ts *TelemetryService) addMetric(metric Metric) {
	ts.metricsMutex.Lock()
	defer ts.metricsMutex.Unlock()
	ts.metrics = append(ts.metrics, metric)
}

// GetAllMetrics возвращает все метрики
func (ts *TelemetryService) GetAllMetrics(w http.ResponseWriter, r *http.Request) {
	ts.metricsMutex.RLock()
	defer ts.metricsMutex.RUnlock()

	response := map[string]interface{}{
		"count":   len(ts.metrics),
		"metrics": ts.metrics,
	}

	respondJSON(w, http.StatusOK, response)
}

// GetMetricsByDeviceID возвращает метрики для конкретного устройства
func (ts *TelemetryService) GetMetricsByDeviceID(w http.ResponseWriter, r *http.Request) {
	// Извлекаем device_id из URL
	deviceID := strings.TrimPrefix(r.URL.Path, "/api/v1/metrics/")
	if deviceID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "отсутствует device_id"})
		return
	}

	ts.metricsMutex.RLock()
	defer ts.metricsMutex.RUnlock()

	// Фильтруем метрики по device_id
	filteredMetrics := make([]Metric, 0)
	for _, metric := range ts.metrics {
		if metric.DeviceID == deviceID {
			filteredMetrics = append(filteredMetrics, metric)
		}
	}

	response := map[string]interface{}{
		"device_id": deviceID,
		"count":     len(filteredMetrics),
		"metrics":   filteredMetrics,
	}

	respondJSON(w, http.StatusOK, response)
}

// Close закрывает подключения
func (ts *TelemetryService) Close() {
	if ts.kafkaReader != nil {
		ts.kafkaReader.Close()
	}
}

// respondJSON отправляет JSON ответ
func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Ошибка кодирования JSON: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
	// Получаем конфигурацию из переменных окружения
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:29092")
	groupID := getEnv("KAFKA_GROUP_ID", "telemetry-service")
	port := getEnv("PORT", ":8083")

	// Инициализируем сервис
	telemetryService := NewTelemetryService(kafkaBroker, groupID)
	defer telemetryService.Close()

	log.Printf("Telemetry Service инициализирован")
	log.Printf("Kafka брокер: %s", kafkaBroker)
	log.Printf("Kafka группа: %s", groupID)

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запускаем consumer
	telemetryService.StartConsuming(ctx)

	// Настраиваем роутер с использованием стандартной библиотеки
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "telemetry-service"})
	})

	// API routes
	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			telemetryService.GetAllMetrics(w, r)
		} else {
			respondJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "метод не поддерживается"})
		}
	})

	mux.HandleFunc("/api/v1/metrics/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			telemetryService.GetMetricsByDeviceID(w, r)
		} else {
			respondJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "метод не поддерживается"})
		}
	})

	// Запуск сервера
	srv := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	// Запускаем сервер в горутине
	go func() {
		log.Printf("Telemetry Service запущен на порту %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска сервера: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Остановка сервера...")

	cancel() // Останавливаем consumer

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Принудительная остановка сервера: %v", err)
	}

	log.Println("Сервер остановлен")
}

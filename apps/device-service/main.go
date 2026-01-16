package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// DeviceEvent представляет событие устройства для Kafka
type DeviceEvent struct {
	EventType string    `json:"event_type"` // "device.created" или "device.deleted"
	DeviceID  string    `json:"device_id"`
	Name      string    `json:"name,omitempty"`
	Type      string    `json:"type,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// DeviceService обрабатывает запросы устройств
type DeviceService struct {
	MonolithURL string
	KafkaWriter *kafka.Writer
}

// NewDeviceService создает новый сервис устройств
func NewDeviceService(monolithURL string, kafkaBroker string) *DeviceService {
	// Создаем Kafka producer
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}

	return &DeviceService{
		MonolithURL: monolithURL,
		KafkaWriter: writer,
	}
}

// PublishEvent публикует событие в Kafka
func (ds *DeviceService) PublishEvent(ctx context.Context, topic string, event DeviceEvent) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("ошибка сериализации события: %w", err)
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.DeviceID),
		Value: eventBytes,
	}

	err = ds.KafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		return fmt.Errorf("ошибка отправки события в Kafka: %w", err)
	}

	log.Printf("Событие опубликовано в топик %s: %s", topic, event.EventType)
	return nil
}

// ProxyToMonolith проксирует запрос в монолит
func (ds *DeviceService) ProxyToMonolith(method, path string, body []byte) ([]byte, int, error) {
	url := fmt.Sprintf("%s%s", ds.MonolithURL, path)

	var req *http.Request
	var err error

	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("ошибка создания запроса: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("ошибка запроса к монолиту: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("ошибка чтения ответа: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// CreateDevice обрабатывает POST /api/v1/devices
func (ds *DeviceService) CreateDevice(w http.ResponseWriter, r *http.Request) {
	// Читаем тело запроса
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "ошибка чтения запроса"})
		return
	}

	// Проксируем запрос в монолит
	respBody, statusCode, err := ds.ProxyToMonolith("POST", "/api/v1/sensors", body)
	if err != nil {
		log.Printf("Ошибка проксирования запроса: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Если устройство успешно создано, публикуем событие
	if statusCode == http.StatusCreated {
		var deviceResponse map[string]interface{}
		if err := json.Unmarshal(respBody, &deviceResponse); err == nil {
			event := DeviceEvent{
				EventType: "device.created",
				DeviceID:  fmt.Sprintf("%v", deviceResponse["id"]),
				Name:      fmt.Sprintf("%v", deviceResponse["name"]),
				Type:      fmt.Sprintf("%v", deviceResponse["type"]),
				Timestamp: time.Now(),
			}

			// Публикуем в топик device.created
			if err := ds.PublishEvent(r.Context(), "device.created", event); err != nil {
				log.Printf("Ошибка публикации события: %v", err)
			}
		}
	}

	// Возвращаем ответ от монолита
	var jsonResponse interface{}
	json.Unmarshal(respBody, &jsonResponse)
	respondJSON(w, statusCode, jsonResponse)
}

// DeleteDevice обрабатывает DELETE /api/v1/devices/:id
func (ds *DeviceService) DeleteDevice(w http.ResponseWriter, r *http.Request) {
	// Извлекаем ID из URL
	deviceID := strings.TrimPrefix(r.URL.Path, "/api/v1/devices/")
	if deviceID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "отсутствует ID устройства"})
		return
	}

	path := fmt.Sprintf("/api/v1/sensors/%s", deviceID)

	// Проксируем запрос в монолит
	respBody, statusCode, err := ds.ProxyToMonolith("DELETE", path, nil)
	if err != nil {
		log.Printf("Ошибка проксирования запроса: %v", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Если устройство успешно удалено, публикуем событие
	if statusCode == http.StatusOK {
		event := DeviceEvent{
			EventType: "device.deleted",
			DeviceID:  deviceID,
			Timestamp: time.Now(),
		}

		// Публикуем в топик device.deleted
		if err := ds.PublishEvent(r.Context(), "device.deleted", event); err != nil {
			log.Printf("Ошибка публикации события: %v", err)
		}
	}

	// Возвращаем ответ от монолита
	var jsonResponse interface{}
	json.Unmarshal(respBody, &jsonResponse)
	respondJSON(w, statusCode, jsonResponse)
}

// Close закрывает подключения
func (ds *DeviceService) Close() {
	if ds.KafkaWriter != nil {
		ds.KafkaWriter.Close()
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
	monolithURL := getEnv("MONOLITH_URL", "http://app:8080")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:29092")
	port := getEnv("PORT", ":8082")

	// Инициализируем сервис
	deviceService := NewDeviceService(monolithURL, kafkaBroker)
	defer deviceService.Close()

	log.Printf("Device Service инициализирован")
	log.Printf("Монолит: %s", monolithURL)
	log.Printf("Kafka брокер: %s", kafkaBroker)

	// Настраиваем роутер с использованием стандартной библиотеки
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "device-service"})
	})

	// API routes
	mux.HandleFunc("/api/v1/devices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			deviceService.CreateDevice(w, r)
		default:
			respondJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "метод не поддерживается"})
		}
	})

	mux.HandleFunc("/api/v1/devices/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deviceService.DeleteDevice(w, r)
		default:
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
		log.Printf("Device Service запущен на порту %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска сервера: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Остановка сервера...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Принудительная остановка сервера: %v", err)
	}

	log.Println("Сервер остановлен")
}

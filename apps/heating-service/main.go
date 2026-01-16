package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// DeviceEvent представляет событие устройства из Kafka
type DeviceEvent struct {
	EventType string    `json:"event_type"`
	DeviceID  string    `json:"device_id"`
	Name      string    `json:"name,omitempty"`
	Type      string    `json:"type,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Metric представляет метрику телеметрии
type Metric struct {
	DeviceID  string    `json:"device_id"`
	Type      string    `json:"type"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
}

// TemperatureResponse представляет ответ от temperature-api
type TemperatureResponse struct {
	Value      float64 `json:"value"`
	Location   string  `json:"location"`
	SensorID   string  `json:"sensor_id"`
	Unit       string  `json:"unit"`
	Timestamp  string  `json:"timestamp"`
	Status     string  `json:"status"`
	SensorType string  `json:"sensor_type"`
}

// HeatingService управляет генерацией метрик отопления
type HeatingService struct {
	kafkaReader      *kafka.Reader
	kafkaWriter      *kafka.Writer
	activeDevices    map[string]context.CancelFunc
	devicesMutex     sync.RWMutex
	temperatureAPIURL string
	httpClient       *http.Client
}

// NewHeatingService создает новый сервис отопления
func NewHeatingService(kafkaBroker, groupID, temperatureAPIURL string) *HeatingService {
	// Создаем Kafka consumer для событий устройств
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  groupID,
		Topic:    "device.created",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// Создаем Kafka producer для метрик
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}

	return &HeatingService{
		kafkaReader:       reader,
		kafkaWriter:       writer,
		activeDevices:     make(map[string]context.CancelFunc),
		temperatureAPIURL: temperatureAPIURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// StartConsuming запускает потребление событий устройств
func (hs *HeatingService) StartConsuming(ctx context.Context) {
	log.Println("Запуск Kafka consumer для топика 'device.created'")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Остановка Kafka consumer")
				return
			default:
				// Читаем сообщение из Kafka
				msg, err := hs.kafkaReader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Ошибка чтения сообщения из Kafka: %v", err)
					continue
				}

				// Парсим событие устройства
				var event DeviceEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf("Ошибка парсинга события: %v", err)
					hs.kafkaReader.CommitMessages(ctx, msg)
					continue
				}

				// Обрабатываем событие
				hs.handleDeviceEvent(ctx, event)

				// Подтверждаем обработку сообщения
				if err := hs.kafkaReader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Ошибка подтверждения сообщения: %v", err)
				}
			}
		}
	}()

	// Дополнительно слушаем события удаления устройств
	hs.StartConsumingDeleted(ctx)
}

// StartConsumingDeleted слушает события удаления устройств
func (hs *HeatingService) StartConsumingDeleted(ctx context.Context) {
	deletedReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  hs.kafkaReader.Config().Brokers,
		GroupID:  hs.kafkaReader.Config().GroupID,
		Topic:    "device.deleted",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	go func() {
		defer deletedReader.Close()

		for {
			select {
			case <-ctx.Done():
				log.Println("Остановка consumer для device.deleted")
				return
			default:
				msg, err := deletedReader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Ошибка чтения device.deleted: %v", err)
					continue
				}

				var event DeviceEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					log.Printf("Ошибка парсинга device.deleted: %v", err)
					deletedReader.CommitMessages(ctx, msg)
					continue
				}

				// Останавливаем генерацию метрик для этого устройства
				hs.stopMetricsGeneration(event.DeviceID)

				deletedReader.CommitMessages(ctx, msg)
			}
		}
	}()
}

// handleDeviceEvent обрабатывает событие создания устройства
func (hs *HeatingService) handleDeviceEvent(ctx context.Context, event DeviceEvent) {
	log.Printf("Получено событие: %s для устройства %s (%s)",
		event.EventType, event.DeviceID, event.Name)

	// Запускаем генерацию метрик для нового устройства
	hs.startMetricsGeneration(ctx, event.DeviceID, event.Name)
}

// startMetricsGeneration запускает генерацию метрик для устройства
func (hs *HeatingService) startMetricsGeneration(parentCtx context.Context, deviceID, deviceName string) {
	hs.devicesMutex.Lock()
	defer hs.devicesMutex.Unlock()

	// Проверяем, не генерируем ли мы уже метрики для этого устройства
	if _, exists := hs.activeDevices[deviceID]; exists {
		log.Printf("Генерация метрик для устройства %s уже запущена", deviceID)
		return
	}

	// Создаем контекст для управления этой горутиной
	ctx, cancel := context.WithCancel(parentCtx)
	hs.activeDevices[deviceID] = cancel

	log.Printf("Запуск генерации метрик для устройства %s (%s)", deviceID, deviceName)

	// Запускаем горутину для генерации метрик
	go func() {
		ticker := time.NewTicker(5 * time.Second) // Генерируем метрики каждые 5 секунд
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Остановка генерации метрик для устройства %s", deviceID)
				return
			case <-ticker.C:
				// Получаем метрику температуры из temperature-api
				metric, err := hs.fetchTemperature(deviceID)
				if err != nil {
					log.Printf("Ошибка получения температуры для устройства %s: %v", deviceID, err)
					continue
				}

				// Публикуем метрику в Kafka
				if err := hs.publishMetric(context.Background(), metric); err != nil {
					log.Printf("Ошибка публикации метрики: %v", err)
				} else {
					log.Printf("Опубликована метрика для устройства %s: %.2f°C",
						deviceID, metric.Value)
				}
			}
		}
	}()
}

// stopMetricsGeneration останавливает генерацию метрик для устройства
func (hs *HeatingService) stopMetricsGeneration(deviceID string) {
	hs.devicesMutex.Lock()
	defer hs.devicesMutex.Unlock()

	if cancel, exists := hs.activeDevices[deviceID]; exists {
		log.Printf("Остановка генерации метрик для устройства %s", deviceID)
		cancel()
		delete(hs.activeDevices, deviceID)
	}
}

// fetchTemperature получает температуру из temperature-api
func (hs *HeatingService) fetchTemperature(deviceID string) (Metric, error) {
	url := fmt.Sprintf("%s/temperature/%s", hs.temperatureAPIURL, deviceID)

	resp, err := hs.httpClient.Get(url)
	if err != nil {
		return Metric{}, fmt.Errorf("ошибка HTTP запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return Metric{}, fmt.Errorf("неожиданный статус код: %d, тело: %s", resp.StatusCode, string(body))
	}

	var tempResponse TemperatureResponse
	if err := json.NewDecoder(resp.Body).Decode(&tempResponse); err != nil {
		return Metric{}, fmt.Errorf("ошибка декодирования JSON: %w", err)
	}

	return Metric{
		DeviceID:  deviceID,
		Type:      "temperature",
		Value:     tempResponse.Value,
		Unit:      "°C",
		Timestamp: time.Now(),
	}, nil
}

// publishMetric публикует метрику в Kafka
func (hs *HeatingService) publishMetric(ctx context.Context, metric Metric) error {
	metricBytes, err := json.Marshal(metric)
	if err != nil {
		return err
	}

	message := kafka.Message{
		Topic: "metrics",
		Key:   []byte(metric.DeviceID),
		Value: metricBytes,
	}

	return hs.kafkaWriter.WriteMessages(ctx, message)
}

// Close закрывает подключения
func (hs *HeatingService) Close() {
	// Останавливаем все активные генераторы метрик
	hs.devicesMutex.Lock()
	for deviceID, cancel := range hs.activeDevices {
		log.Printf("Остановка генерации метрик для устройства %s", deviceID)
		cancel()
	}
	hs.activeDevices = make(map[string]context.CancelFunc)
	hs.devicesMutex.Unlock()

	if hs.kafkaReader != nil {
		hs.kafkaReader.Close()
	}
	if hs.kafkaWriter != nil {
		hs.kafkaWriter.Close()
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
	groupID := getEnv("KAFKA_GROUP_ID", "heating-service")
	temperatureAPIURL := getEnv("TEMPERATURE_API_URL", "http://temperature-api:8081")

	// Инициализируем сервис
	heatingService := NewHeatingService(kafkaBroker, groupID, temperatureAPIURL)
	defer heatingService.Close()

	log.Printf("Heating Service инициализирован")
	log.Printf("Kafka брокер: %s", kafkaBroker)
	log.Printf("Kafka группа: %s", groupID)
	log.Printf("Temperature API URL: %s", temperatureAPIURL)

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запускаем consumer
	heatingService.StartConsuming(ctx)

	log.Println("Heating Service запущен и ожидает события устройств...")

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Остановка сервиса...")

	cancel() // Останавливаем все горутины

	// Даем время для корректной остановки
	time.Sleep(2 * time.Second)

	log.Println("Сервис остановлен")
}

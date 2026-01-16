package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// TemperatureResponse представляет ответ API с температурой
type TemperatureResponse struct {
	Value      float64 `json:"value"`
	Location   string  `json:"location"`
	SensorID   string  `json:"sensor_id"`
	Unit       string  `json:"unit"`
	Timestamp  string  `json:"timestamp"`
	Status     string  `json:"status"`
	SensorType string  `json:"sensor_type"`
}

func main() {
	// Создание HTTP сервера
	mux := http.NewServeMux()
	mux.HandleFunc("/temperature/{id}", temperatureHandler)
	mux.HandleFunc("/health", healthHandler)

	port := getEnv("PORT", ":8081")
	srv := &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Запуск сервера в горутине
	go func() {
		log.Printf("Temperature API запущен на порту %s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска сервера: %v\n", err)
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
		log.Fatalf("Принудительная остановка сервера: %v\n", err)
	}

	log.Println("Сервер остановлен корректно")
}

// temperatureHandler обрабатывает запросы к /temperature
func temperatureHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	// Получение параметров запроса
	location := r.URL.Query().Get("location")
	sensorID := r.PathValue("id")

	// Если location не указан, определяем по sensorID
	if location == "" {
		switch sensorID {
		case "1":
			location = "Living Room"
		case "2":
			location = "Bedroom"
		case "3":
			location = "Kitchen"
		default:
			location = "Unknown"
		}
	}

	// Если sensorID не указан, определяем по location
	if sensorID == "" {
		switch location {
		case "Living Room":
			sensorID = "1"
		case "Bedroom":
			sensorID = "2"
		case "Kitchen":
			sensorID = "3"
		default:
			sensorID = "0"
		}
	}

	// Генерация случайной температуры от 18 до 26 градусов
	temperature := 18.0 + rand.Float64()*8.0

	response := TemperatureResponse{
		Value:      temperature,
		Location:   location,
		SensorID:   sensorID,
		Unit:       "celsius",
		Timestamp:  time.Now().Format(time.RFC3339),
		Status:     "ok",
		SensorType: "temperature",
	}

	// Отправка JSON ответа
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Ошибка кодирования JSON: %v\n", err)
		http.Error(w, "Внутренняя ошибка сервера", http.StatusInternalServerError)
		return
	}

	log.Printf("Запрос обработан: location=%s, sensorId=%s, temp=%.2f°C\n", location, sensorID, temperature)
}

// healthHandler обрабатывает health check запросы
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// getEnv получает значение переменной окружения или возвращает значение по умолчанию
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

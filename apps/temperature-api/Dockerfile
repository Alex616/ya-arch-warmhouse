# Стадия сборки
FROM golang:1.24-alpine AS builder

# Установка рабочей директории
WORKDIR /app

# Копирование go.mod
COPY go.mod ./

# Копирование исходного кода
COPY . .

# Сборка приложения
RUN CGO_ENABLED=0 GOOS=linux go build -o temperature-api

# Стадия runtime
FROM alpine:latest

# Установка необходимых runtime зависимостей
RUN apk --no-cache add ca-certificates tzdata

# Установка рабочей директории
WORKDIR /app

# Копирование бинарника из стадии builder
COPY --from=builder /app/temperature-api .

# Указание порта
EXPOSE 8081

# Команда запуска приложения
CMD ["./temperature-api"]

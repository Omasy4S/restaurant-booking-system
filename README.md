# 🍽️ Restaurant Booking System

Event-driven система бронирования столиков с Kafka, PostgreSQL и Node.js.


## Архитектура

```
Client → API Service → Kafka → Booking Service → PostgreSQL
```

- **API Service** — принимает HTTP-запросы, публикует события в Kafka
- **Booking Service** — слушает Kafka, проверяет доступность, обновляет статусы
- **PostgreSQL** — хранит брони с индексами
- **Kafka** — брокер сообщений

## Жизненный цикл брони

```
CREATED → CHECKING_AVAILABILITY → [CONFIRMED | REJECTED]
```

## Запуск

### 1. Установить зависимости
```bash
npm install
```

### 2. Запустить PostgreSQL и Kafka
Убедись, что PostgreSQL (порт 5432) и Kafka (порт 9092) запущены.

Если используешь Docker:
```bash
docker-compose up -d
```

### 3. Запустить сервисы

**Терминал 1:**
```bash
npm run dev:api
```

**Терминал 2:**
```bash
npm run dev:booking
```

## API

### Создать бронь
```http
POST http://localhost:3000/bookings
Content-Type: application/json

{
  "restaurant_id": "restaurant_1",
  "booking_date": "2025-12-01",
  "booking_time": "18:00:00",
  "guest_count": 4
}
```

### Получить бронь
```http
GET http://localhost:3000/bookings/:bookingId
```

### Получить все брони
```http
GET http://localhost:3000/bookings
```

## Тестирование

Открой `test-requests.http` в VS Code и используй REST Client extension — просто нажми "Send Request" над каждым запросом.

## Технологии

- Node.js + Express
- KafkaJS
- PostgreSQL
- Docker Compose (опционально)

---

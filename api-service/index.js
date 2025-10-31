const express = require('express');
const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

// ============================================
// КОНФИГУРАЦИЯ
// ============================================

// PostgreSQL connection pool
// ПОЧЕМУ POOL:
// - Создание нового подключения к БД = 100-200ms (TCP handshake, аутентификация, инициализация)
// - Pool держит пул готовых подключений и переиспользует их
// - Вместо: создать (200ms) → использовать → закрыть → создать снова (200ms)
// - Получаем: взять из пула (1ms) → использовать → вернуть в пул
// - На 100 запросах: 20 секунд → 0.1 секунды
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'booking_db',
  user: process.env.DB_USER || 'booking_user',
  password: process.env.DB_PASSWORD || 'booking_pass',
  max: 20, // максимум 20 одновременных подключений
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Kafka producer
// ПОЧЕМУ KAFKA (event-driven подход):
// 
// БЕЗ KAFKA (синхронно):
//   Client → API → проверка доступности (2-3 сек) → API → Client
//   Клиент ждет 3 секунды, плохой UX
// 
// С KAFKA (асинхронно):
//   Client → API → Client (мгновенный ответ)
//            ↓
//          Kafka → Booking Service (обработка в фоне)
// 
// Преимущества:
// 1. Быстрый ответ клиенту (не ждем обработки)
// 2. Отказоустойчивость (если Booking Service упал, события не теряются)
// 3. Масштабируемость (можем запустить 10 экземпляров Booking Service)
// 4. Разделение ответственности (API только принимает запросы, не знает про бизнес-логику)
const kafka = new Kafka({
  clientId: 'api-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer();

// ============================================
// ИНИЦИАЛИЗАЦИЯ
// ============================================

async function initKafka() {
  await producer.connect();
  console.log('✅ Kafka producer connected');
}

// ============================================
// API ENDPOINTS
// ============================================

/**
 * POST /bookings
 * Создание новой брони
 */
app.post('/bookings', async (req, res) => {
  try {
    const { restaurant_id, booking_date, booking_time, guest_count } = req.body;

    // Валидация обязательных полей
    if (!restaurant_id) {
      return res.status(400).json({ error: 'restaurant_id is required' });
    }
    if (!booking_date) {
      return res.status(400).json({ error: 'booking_date is required' });
    }
    if (!booking_time) {
      return res.status(400).json({ error: 'booking_time is required' });
    }
    if (guest_count === undefined || guest_count === null) {
      return res.status(400).json({ error: 'guest_count is required' });
    }

    // Валидация значений
    if (typeof guest_count !== 'number' || guest_count < 1) {
      return res.status(400).json({ 
        error: 'guest_count must be a number greater than or equal to 1',
        received: guest_count
      });
    }

    // Генерируем уникальный ID
    const bookingId = uuidv4();
    const status = 'CREATED';

    // Сохраняем в БД ПЕРЕД отправкой в Kafka
    // ПОЧЕМУ СНАЧАЛА БД:
    // - Если сохраним в Kafka, но БД упадет → событие обработается, но данных в БД нет (несогласованность)
    // - Если сохраним в БД, но Kafka упадет → данные есть, можем переотправить событие
    // - Принцип: БД = source of truth, Kafka = транспорт для событий
    const query = `
      INSERT INTO bookings (id, restaurant_id, booking_date, booking_time, guest_count, status)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING *
    `;
    
    const result = await pool.query(query, [
      bookingId,
      restaurant_id,
      booking_date,
      booking_time,
      guest_count,
      status
    ]);

    const booking = result.rows[0];

    // Публикуем событие в Kafka
    // ПОЧЕМУ СОБЫТИЕ, А НЕ ПРЯМОЙ ВЫЗОВ:
    // - Booking Service может быть недоступен → с Kafka событие не потеряется
    // - Можем добавить другие сервисы (например, Email Service) без изменения API
    // - Kafka гарантирует доставку и порядок событий
    await producer.send({
      topic: 'booking-events',
      messages: [
        {
          key: bookingId, // ключ для партиционирования
          // ПОЧЕМУ КЛЮЧ:
          // - Kafka распределяет сообщения по партициям на основе хеша ключа
          // - Все события с одним bookingId попадут в одну партицию
          // - Это гарантирует порядок обработки для одной брони
          // - Без ключа события могут обработаться в произвольном порядке
          value: JSON.stringify({
            event_type: 'BOOKING_CREATED',
            booking_id: bookingId,
            restaurant_id,
            booking_date,
            booking_time,
            guest_count,
            timestamp: new Date().toISOString()
          })
        }
      ]
    });

    console.log(`📝 Booking created: ${bookingId}`);

    res.status(201).json({
      message: 'Booking created successfully',
      booking: {
        id: booking.id,
        restaurant_id: booking.restaurant_id,
        booking_date: booking.booking_date,
        booking_time: booking.booking_time,
        guest_count: booking.guest_count,
        status: booking.status,
        created_at: booking.created_at
      }
    });

  } catch (error) {
    console.error('❌ Error creating booking:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /bookings/:bookingId
 * Получение информации о брони
 * 
 * ПОЧЕМУ ПОИСК ПО ID БЫСТРЫЙ:
 * - PRIMARY KEY в PostgreSQL автоматически создает уникальный индекс
 * - Поиск по индексу = прямой доступ к строке
 * - Время поиска не зависит от размера таблицы
 */
app.get('/bookings/:bookingId', async (req, res) => {
  try {
    const { bookingId } = req.params;

    const query = 'SELECT * FROM bookings WHERE id = $1';
    const result = await pool.query(query, [bookingId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Booking not found' });
    }

    const booking = result.rows[0];

    res.json({
      booking: {
        id: booking.id,
        restaurant_id: booking.restaurant_id,
        booking_date: booking.booking_date,
        booking_time: booking.booking_time,
        guest_count: booking.guest_count,
        status: booking.status,
        created_at: booking.created_at,
        updated_at: booking.updated_at
      }
    });

  } catch (error) {
    console.error('❌ Error fetching booking:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /bookings
 * Получение всех броней (для отладки)
 */
app.get('/bookings', async (req, res) => {
  try {
    const query = 'SELECT * FROM bookings ORDER BY created_at DESC LIMIT 100';
    const result = await pool.query(query);

    res.json({
      count: result.rows.length,
      bookings: result.rows
    });

  } catch (error) {
    console.error('❌ Error fetching bookings:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'api-service' });
});

// ============================================
// ЗАПУСК СЕРВЕРА
// ============================================

const PORT = process.env.PORT || 3000;

async function start() {
  try {
    await initKafka();
    
    app.listen(PORT, () => {
      console.log(`🚀 API Service running on http://localhost:${PORT}`);
      console.log(`📊 Endpoints:`);
      console.log(`   POST   /bookings`);
      console.log(`   GET    /bookings/:bookingId`);
      console.log(`   GET    /bookings`);
      console.log(`   GET    /health`);
    });
  } catch (error) {
    console.error('❌ Failed to start API service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('⏹️  SIGTERM received, shutting down gracefully...');
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

start();

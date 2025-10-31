const { Kafka } = require('kafkajs');
const { Pool } = require('pg');

// ============================================
// КОНФИГУРАЦИЯ
// ============================================

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'booking_db',
  user: process.env.DB_USER || 'booking_user',
  password: process.env.DB_PASSWORD || 'booking_pass',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

const kafka = new Kafka({
  clientId: 'booking-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'booking-service-group' });

// ============================================
// БИЗНЕС-ЛОГИКА
// ============================================

/**
 * Проверка доступности столиков
 * 
 * ПОЧЕМУ ИСПОЛЬЗУЕМ ИНДЕКС:
 * - Без индекса PostgreSQL сканирует всю таблицу построчно (медленно на больших данных)
 * - С индексом PostgreSQL использует B-tree — бинарное дерево поиска
 * - B-tree делит данные пополам на каждом шаге, находит нужную строку быстро
 * - На 10,000 броней: без индекса ~10,000 операций, с индексом ~13 операций
 */
async function checkAvailability(bookingId, restaurantId, bookingDate, bookingTime) {
  const client = await pool.connect();
  
  try {
    // Начинаем транзакцию
    // ПОЧЕМУ ТРАНЗАКЦИЯ КРИТИЧНА:
    // 
    // БЕЗ ТРАНЗАКЦИИ (race condition):
    //   Запрос 1: Проверяет доступность → свободно ✓
    //   Запрос 2: Проверяет доступность → свободно ✓
    //   Запрос 1: Подтверждает бронь
    //   Запрос 2: Подтверждает бронь
    //   Результат: Двойное бронирование! ❌
    // 
    // С ТРАНЗАКЦИЕЙ:
    //   Запрос 1: BEGIN → проверка → подтверждение → COMMIT
    //   Запрос 2: BEGIN → ждет блокировки → проверка → видит занято → REJECT
    //   Результат: Только одна бронь подтверждена ✓
    // 
    // Транзакция гарантирует атомарность (все операции или ни одна) и изоляцию (запросы не мешают друг другу)
    await client.query('BEGIN');

    // Обновляем статус на "проверяется"
    await client.query(
      'UPDATE bookings SET status = $1 WHERE id = $2',
      ['CHECKING_AVAILABILITY', bookingId]
    );

    console.log(`🔍 Checking availability for booking ${bookingId}`);

    // Ищем конфликтующие брони
    // ПОЧЕМУ ТОЛЬКО CONFIRMED:
    // - CREATED брони еще не обработаны, могут быть отклонены
    // - CHECKING_AVAILABILITY — в процессе проверки
    // - REJECTED — уже отклонены, не занимают столик
    // - Только CONFIRMED брони реально занимают столик
    // 
    // ПОЧЕМУ id != $4:
    // - Исключаем текущую бронь из поиска
    // - Иначе бронь найдет саму себя и отклонится
    // 
    // ПОЧЕМУ LIMIT 1:
    // - Нам достаточно найти хотя бы одну конфликтующую бронь
    // - Не нужно проверять все остальные → оптимизация
    const conflictQuery = `
      SELECT id FROM bookings
      WHERE restaurant_id = $1
        AND booking_date = $2
        AND booking_time = $3
        AND status = 'CONFIRMED'
        AND id != $4
      LIMIT 1
    `;

    const conflictResult = await client.query(conflictQuery, [
      restaurantId,
      bookingDate,
      bookingTime,
      bookingId
    ]);

    let newStatus;
    let message;

    if (conflictResult.rows.length > 0) {
      // Есть конфликт — отклоняем бронь
      newStatus = 'REJECTED';
      message = 'Table already booked for this time';
      console.log(`❌ Booking ${bookingId} REJECTED - time slot taken`);
    } else {
      // Конфликтов нет — подтверждаем
      newStatus = 'CONFIRMED';
      message = 'Booking confirmed';
      console.log(`✅ Booking ${bookingId} CONFIRMED`);
    }

    // Обновляем финальный статус
    await client.query(
      'UPDATE bookings SET status = $1 WHERE id = $2',
      [newStatus, bookingId]
    );

    // Коммитим транзакцию
    await client.query('COMMIT');

    return { status: newStatus, message };

  } catch (error) {
    // Откатываем транзакцию при ошибке
    await client.query('ROLLBACK');
    console.error('❌ Error checking availability:', error);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Обработчик событий из Kafka
 * 
 * ПОЧЕМУ KAFKA CONSUMER:
 * 
 * 1. АСИНХРОННОСТЬ:
 *    - API отвечает клиенту за ~50ms
 *    - Проверка доступности идет в фоне
 *    - Клиент не ждет, получает мгновенный ответ
 * 
 * 2. НАДЕЖНОСТЬ:
 *    - Kafka хранит события на диске
 *    - Если Booking Service упал → события не теряются
 *    - После перезапуска сервис продолжит обработку с того же места
 * 
 * 3. МАСШТАБИРУЕМОСТЬ:
 *    - Можем запустить 5 экземпляров Booking Service
 *    - Kafka автоматически распределит события между ними
 *    - Каждое событие обработается только один раз (благодаря consumer group)
 * 
 * 4. РАЗДЕЛЕНИЕ ОТВЕТСТВЕННОСТИ:
 *    - API Service не знает про бизнес-логику проверки
 *    - Booking Service не знает про HTTP
 *    - Легко добавить новые сервисы (Email, SMS) без изменения существующих
 */
async function handleBookingEvent(message) {
  try {
    const event = JSON.parse(message.value.toString());
    
    console.log(`📨 Received event: ${event.event_type} for booking ${event.booking_id}`);

    if (event.event_type === 'BOOKING_CREATED') {
      // Проверяем доступность
      await checkAvailability(
        event.booking_id,
        event.restaurant_id,
        event.booking_date,
        event.booking_time
      );
    }

  } catch (error) {
    console.error('❌ Error handling event:', error);
    // В production здесь можно отправить событие в Dead Letter Queue
  }
}

// ============================================
// ИНИЦИАЛИЗАЦИЯ И ЗАПУСК
// ============================================

async function start() {
  try {
    // Подключаемся к Kafka
    await consumer.connect();
    console.log('✅ Kafka consumer connected');

    // Подписываемся на топик
    await consumer.subscribe({ 
      topic: 'booking-events', 
      fromBeginning: false // читаем только новые события
      // ПОЧЕМУ fromBeginning: false:
      // - true = читаем все события с начала топика (для первого запуска или восстановления)
      // - false = читаем только новые события (для обычной работы)
      // - Kafka запоминает offset (позицию) для каждого consumer group
      // - После перезапуска продолжим с последнего обработанного события
    });

    console.log('✅ Subscribed to booking-events topic');

    // Запускаем обработку событий
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await handleBookingEvent(message);
      },
    });

    console.log('🚀 Booking Service is running and listening for events...');

  } catch (error) {
    console.error('❌ Failed to start Booking Service:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('⏹️  SIGTERM received, shutting down gracefully...');
  await consumer.disconnect();
  await pool.end();
  process.exit(0);
});

start();

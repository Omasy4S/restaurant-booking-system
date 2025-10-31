-- Таблица для хранения броней
CREATE TABLE IF NOT EXISTS bookings (
    id UUID PRIMARY KEY,
    restaurant_id VARCHAR(255) NOT NULL,
    booking_date DATE NOT NULL,
    booking_time TIME NOT NULL,
    guest_count INTEGER NOT NULL CHECK (guest_count > 0),
    status VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска броней по ресторану и времени
-- ЗАЧЕМ: когда проверяем доступность, ищем все брони на конкретное время
-- Без индекса PostgreSQL будет сканировать всю таблицу — O(n)
-- С индексом — O(log n) через B-tree
CREATE INDEX idx_restaurant_datetime ON bookings(restaurant_id, booking_date, booking_time);

-- Индекс для поиска по статусу
-- ЗАЧЕМ: часто будем искать активные (CONFIRMED) брони
CREATE INDEX idx_status ON bookings(status);

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггер для автоматического обновления updated_at при изменении записи
CREATE TRIGGER update_bookings_updated_at 
    BEFORE UPDATE ON bookings 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Вставим тестовые данные для демонстрации
INSERT INTO bookings (id, restaurant_id, booking_date, booking_time, guest_count, status)
VALUES 
    ('550e8400-e29b-41d4-a716-446655440000', 'restaurant_1', '2025-11-15', '19:00:00', 4, 'CONFIRMED'),
    ('550e8400-e29b-41d4-a716-446655440001', 'restaurant_1', '2025-11-15', '20:00:00', 2, 'CONFIRMED');

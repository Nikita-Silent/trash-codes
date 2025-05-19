import psycopg2
import redis
import time
import json
import pandas as pd
from datetime import datetime, date, timedelta
from decimal import Decimal

# Конфигурация подключений
POSTGRES_CONFIG = {
    "host": "192.168.50.24",
    "database": "demo",
    "user": "postgres",
    "password": "postgres"
}

REDIS_CONFIG = {
    "host": "192.168.50.24",
    "port": 6379,
    "db": 0,
    "decode_responses": False
}
QUERIES = {
    "booking_stats": """
    SELECT 
        b.book_ref,
        b.book_date,
        COUNT(t.ticket_no) AS tickets_count,
        SUM(tf.amount) AS total_amount,
        b.total_amount AS booking_amount
    FROM 
        bookings b
    JOIN 
        tickets t ON b.book_ref = t.book_ref
    JOIN 
        ticket_flights tf ON t.ticket_no = tf.ticket_no
    GROUP BY 
        b.book_ref, b.book_date, b.total_amount
    HAVING 
        COUNT(t.ticket_no) > 1
    ORDER BY 
        total_amount DESC
    LIMIT 10;
    """,
    
    "scheduled_flights": """
    SELECT t.passenger_name, t.contact_data, f.flight_no, f.scheduled_departure
    FROM tickets t
    INNER JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
    INNER JOIN flights f ON tf.flight_id = f.flight_id
    WHERE f.status = 'Scheduled'
    ORDER BY f.scheduled_departure
    LIMIT 10;
    """,
    
    "route_analysis": """
    WITH route_stats AS (
        SELECT 
            f.departure_airport,
            f.arrival_airport,
            COUNT(*) AS total_flights,
            AVG(f.actual_arrival - f.actual_departure) AS avg_duration,
            SUM(tf.amount) AS total_revenue
        FROM flights f
        JOIN ticket_flights tf ON f.flight_id = tf.flight_id
        WHERE f.status = 'Arrived'
        GROUP BY f.departure_airport, f.arrival_airport
    )
    SELECT 
        rs.*,
        a1.airport_name AS departure_name,
        a2.airport_name AS arrival_name,
        a1.city AS departure_city,
        a2.city AS arrival_city,
        (SELECT COUNT(*) 
         FROM flights f2 
         WHERE f2.departure_airport = rs.departure_airport 
         AND f2.arrival_airport = rs.arrival_airport
         AND f2.aircraft_code IN (
             SELECT aircraft_code 
             FROM aircrafts 
             WHERE range > 3000
         )) AS long_range_flights,
        RANK() OVER (ORDER BY rs.total_revenue DESC) AS revenue_rank
    FROM route_stats rs
    JOIN airports a1 ON rs.departure_airport = a1.airport_code
    JOIN airports a2 ON rs.arrival_airport = a2.airport_code
    ORDER BY rs.total_revenue DESC
    LIMIT 100;
    """
}
# Улучшенный JSON-энкодер для всех типов PostgreSQL
class PGDataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)

def convert_pg_data(data):
    """Конвертирует данные PostgreSQL в JSON-совместимые форматы"""
    if isinstance(data, (list, tuple)):
        return [convert_pg_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_pg_data(value) for key, value in data.items()}
    elif isinstance(data, (datetime, date)):
        return data.isoformat()
    elif isinstance(data, timedelta):
        return data.total_seconds()
    elif isinstance(data, Decimal):
        return float(data)
    return data

def execute_query(pg_conn, query):
    """Выполнение SQL-запроса и возврат результатов"""
    with pg_conn.cursor() as cursor:
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        duration = time.time() - start_time
    return result, columns, duration

def cache_to_redis(redis_conn, key, data, columns, ttl=3600):
    """Кэширование данных в Redis с полной конвертацией типов"""
    try:
        # Преобразуем в список словарей с конвертацией всех специальных типов
        dict_data = []
        for row in data:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = convert_pg_data(row[i])
            dict_data.append(row_dict)
        
        redis_conn.setex(key, ttl, json.dumps(dict_data, cls=PGDataEncoder))
        return True
    except Exception as e:
        print(f"Подробная ошибка при кэшировании: {str(e)}")
        return False

def get_from_redis(redis_conn, key):
    """Получение данных из Redis"""
    start_time = time.time()
    cached_data = redis_conn.get(key)
    duration = time.time() - start_time
    if cached_data:
        return json.loads(cached_data), duration
    return None, duration

def clear_caches(redis_conn, pg_conn):
    """Очистка кэшей"""
    redis_conn.flushdb()
    pg_conn.reset()

def run_performance_test():
    """Запуск тестирования производительности"""
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    redis_conn = redis.Redis(**REDIS_CONFIG)
    
    results = []
    
    for query_name, query in QUERIES.items():
        print(f"\n=== Тестирование запроса: {query_name} ===")
        
        # 1. Холодный запуск PostgreSQL
        clear_caches(redis_conn, pg_conn)
        pg_data, columns, pg_cold_time = execute_query(pg_conn, query)
        print(f"PostgreSQL (холодный): {pg_cold_time:.4f} сек")
        
        # 2. Кэшируем в Redis
        cache_success = cache_to_redis(redis_conn, query_name, pg_data, columns)
        
        if not cache_success:
            print("Не удалось кэшировать данные в Redis")
            results.append({
                "query": query_name,
                "pg_cold": pg_cold_time,
                "redis": None,
                "pg_warm": None,
                "redis_vs_cold": None,
                "redis_vs_warm": None
            })
            continue
        
        print("Данные успешно кэшированы в Redis")
        
        # 3. Чтение из Redis
        redis_data, redis_time = get_from_redis(redis_conn, query_name)
        print(f"Redis: {redis_time:.4f} сек")
        
        # 4. Теплый запуск PostgreSQL
        pg_data_warm, _, pg_warm_time = execute_query(pg_conn, query)
        print(f"PostgreSQL (теплый): {pg_warm_time:.4f} сек")
        
        # Сохраняем результаты
        results.append({
            "query": query_name,
            "pg_cold": pg_cold_time,
            "redis": redis_time,
            "pg_warm": pg_warm_time,
            "redis_vs_cold": pg_cold_time / redis_time if redis_time else None,
            "redis_vs_warm": pg_warm_time / redis_time if redis_time else None
        })
    
    # Выводим результаты
    print("\n=== Итоговые результаты ===")
    df = pd.DataFrame(results)
    print(df.to_string())
    
    pg_conn.close()

if __name__ == "__main__":
    run_performance_test()
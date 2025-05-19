import psycopg2
import redis
from datetime import datetime

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
    "decode_responses": True
}

def redis_to_postgres():
    """Перенос данных из Redis в PostgreSQL"""
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    redis_conn = redis.Redis(**REDIS_CONFIG)
    
    try:
        with pg_conn.cursor() as cursor:
            counters = {
                'bookings': 0,
                'tickets': 0, 
                'flights': 0,
                'errors': 0
            }

            # 1. Обработка бронирований
            for key in redis_conn.keys("booking:*"):
                try:
                    data = redis_conn.hgetall(key)
                    
                    # Проверка существующей записи
                    cursor.execute(
                        "SELECT 1 FROM bookings WHERE book_ref = %s LIMIT 1",
                        (data['book_ref'],)
                    )
                    
                    if not cursor.fetchone():
                        cursor.execute(
                            """INSERT INTO bookings 
                            (book_ref, book_date, total_amount)
                            VALUES (%s, %s, %s)""",
                            (
                                data['book_ref'],
                                datetime.strptime(data['book_date'], '%Y-%m-%d').date(),
                                float(data['total_amount'])
                            )
                        )
                        counters['bookings'] += 1
                        
                except Exception as e:
                    counters['errors'] += 1
                    print(f"Ошибка в бронировании {key}: {str(e)}")

            # 2. Обработка рейсов
            for key in redis_conn.keys("flight:*"):
                try:
                    data = redis_conn.hgetall(key)
                    
                    cursor.execute(
                        "SELECT 1 FROM flights WHERE flight_id = %s LIMIT 1",
                        (data['flight_id'],)
                    )
                    
                    if not cursor.fetchone():
                        cursor.execute(
                            """INSERT INTO flights 
                            (flight_id, flight_no, scheduled_departure, scheduled_arrival)
                            VALUES (%s, %s, %s, %s)""",
                            (
                                data['flight_id'],
                                data['flight_no'],
                                datetime.strptime(data['scheduled_departure'], '%Y-%m-%dT%H:%M:%S'),
                                datetime.strptime(data['scheduled_arrival'], '%Y-%m-%dT%H:%M:%S')
                            )
                        )
                        counters['flights'] += 1
                        
                except Exception as e:
                    counters['errors'] += 1
                    print(f"Ошибка в рейсе {key}: {str(e)}")

            # 3. Обработка билетов
            for key in redis_conn.keys("ticket:*"):
                try:
                    data = redis_conn.hgetall(key)
                    
                    # Проверка существования бронирования
                    cursor.execute(
                        "SELECT 1 FROM bookings WHERE book_ref = %s LIMIT 1",
                        (data['book_ref'],)
                    )
                    
                    if not cursor.fetchone():
                        print(f"Бронирование {data['book_ref']} не найдено для билета {key}")
                        counters['errors'] += 1
                        continue
                        
                    cursor.execute(
                        """INSERT INTO tickets 
                        (ticket_no, book_ref, passenger_name, contact_data)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (ticket_no) DO NOTHING""",
                        (
                            data['ticket_no'],
                            data['book_ref'],
                            data['passenger_name'],
                            data['contact_data']
                        )
                    )
                    if cursor.rowcount > 0:
                        counters['tickets'] += 1
                        
                except Exception as e:
                    counters['errors'] += 1
                    print(f"Ошибка в билете {key}: {str(e)}")

            pg_conn.commit()
            print("\nРезультаты переноса:")
            print(f"Добавлено бронирований: {counters['bookings']}")
            print(f"Добавлено рейсов: {counters['flights']}")
            print(f"Добавлено билетов: {counters['tickets']}")
            print(f"Всего ошибок: {counters['errors']}")

    except Exception as e:
        pg_conn.rollback()
        print(f"Критическая ошибка: {str(e)}")
    finally:
        pg_conn.close()
        redis_conn.close()

if __name__ == "__main__":
    redis_to_postgres()
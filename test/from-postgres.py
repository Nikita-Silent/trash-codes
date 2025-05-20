import psycopg2
import redis
from datetime import datetime

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

def main():
    # Подключение к БД
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    redis_conn = redis.Redis(**REDIS_CONFIG)
    
    sorted_set_key = "ticket_flights:amount"

    try:
        with pg_conn.cursor() as cursor:
            # Выбираем 10 случайных записей с ценами билетов
            cursor.execute("""
                SELECT tf.ticket_no, tf.flight_id, tf.amount, 
                       t.passenger_name, f.flight_no, f.scheduled_departure
                FROM ticket_flights tf
                JOIN tickets t ON tf.ticket_no = t.ticket_no
                JOIN flights f ON tf.flight_id = f.flight_id
                ORDER BY RANDOM()
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            
            if not results:
                print("Нет данных для обработки")
                return

            # Очищаем предыдущие данные
            redis_conn.delete(sorted_set_key)

            # Добавляем в Redis sorted set
            for row in results:
                ticket_no, flight_id, amount, passenger, flight_no, dep_time = row
                member = f"{ticket_no}:{flight_id}:{passenger}:{flight_no}:{dep_time}"
                redis_conn.zadd(sorted_set_key, {member: float(amount)})

            # Получаем экстремальные значения
            min_entries = redis_conn.zrange(
                sorted_set_key, 0, 2, withscores=True, desc=False
            )
            
            max_entries = redis_conn.zrevrange(
                sorted_set_key, 0, 2, withscores=True
            )

            print("Три самых дешевых билета:")
            for i, (member, score) in enumerate(min_entries, 1):
                parts = member.decode().split(':')
                print(f"{i}. {parts[2]} (Рейс {parts[3]}) - {score:.2f} руб. "
                      f"| Вылет: {parts[4]}")

            print("\nТри самых дорогих билета:")
            for i, (member, score) in enumerate(max_entries, 1):
                parts = member.decode().split(':')
                print(f"{i}. {parts[2]} (Рейс {parts[3]}) - {score:.2f} руб. "
                      f"| Вылет: {parts[4]}")

    except Exception as e:
        print(f"Ошибка: {str(e)}")
    finally:
        pg_conn.close()
        redis_conn.close()

if __name__ == "__main__":
    main()
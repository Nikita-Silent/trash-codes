import psycopg2
from pymongo import MongoClient
from datetime import datetime

# Конфигурации подключений
POSTGRES_CONFIG = {
    "host": "192.168.50.24",
    "database": "demo",
    "user": "postgres",
    "password": "postgres"
}

MONGO_CONFIG = {
    "host": "192.168.50.24",
    "port": 27017,
    "username": "root",      # Добавляем имя пользователя
    "password": "root",  # Добавляем пароль
    "authSource": "admin",              # Указываем базу аутентификации
    "authMechanism": "SCRAM-SHA-256"    # Механизм аутентификации
}

def create_nested_documents(pg_cursor):
    """Создание вложенных документов (бронирование -> билеты -> рейсы)"""
    pg_cursor.execute("""
        SELECT 
            b.book_ref,
            b.book_date,
            b.total_amount,
            jsonb_agg(
                jsonb_build_object(
                    'ticket_no', t.ticket_no,
                    'passenger', t.passenger_name,
                    'flights', tf.flight_data
                )
            ) AS tickets
        FROM bookings b
        JOIN tickets t ON b.book_ref = t.book_ref
        JOIN (
            SELECT 
                tf.ticket_no,
                jsonb_agg(
                    jsonb_build_object(
                        'flight_no', f.flight_no,
                        'departure_airport', f.departure_airport,
                        'arrival_airport', f.arrival_airport,
                        'scheduled_departure', f.scheduled_departure,
                        'status', f.status
                    )
                ) AS flight_data
            FROM ticket_flights tf
            JOIN flights f ON tf.flight_id = f.flight_id
            GROUP BY tf.ticket_no
        ) tf ON t.ticket_no = tf.ticket_no
        GROUP BY b.book_ref
        LIMIT 1000
    """)
    return pg_cursor.fetchall()

def create_array_collection(pg_cursor):
    """Создание коллекции с массивами значений (аэропорты -> рейсы)"""
    pg_cursor.execute("""
        SELECT 
            a.airport_code,
            a.airport_name,
            jsonb_agg(
                jsonb_build_object(
                    'flight_no', f.flight_no,
                    'departure_time', f.scheduled_departure,
                    'arrival_airport', f.arrival_airport,
                    'aircraft', ac.model
                )
            ) AS flights
        FROM airports a
        JOIN flights f ON a.airport_code = f.departure_airport
        JOIN aircrafts ac ON f.aircraft_code = ac.aircraft_code
        GROUP BY a.airport_code, a.airport_name
    """)
    return pg_cursor.fetchall()

def convert_date(obj):
    """Конвертация datetime в строку для MongoDB"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def main():
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cursor = pg_conn.cursor()
    
    # Подключение к MongoDB с аутентификацией
    mongo_client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["authSource"],
        authMechanism=MONGO_CONFIG["authMechanism"]
    )
    
    db = mongo_client.airline_database
    
    try:
        # 1. Коллекция с вложенными документами
        nested_data = create_nested_documents(pg_cursor)
        bookings_collection = db.bookings
        for item in nested_data:
            doc = {
                "booking_ref": item[0],
                "booking_date": convert_date(item[1]),
                "total_amount": float(item[2]),
                "tickets": item[3]
            }
            bookings_collection.insert_one(doc)

        # 2. Коллекция с массивами значений
        array_data = create_array_collection(pg_cursor)
        airports_collection = db.airports
        for item in array_data:
            doc = {
                "airport_code": item[0],
                "airport_name": item[1],
                "flights": item[2]
            }
            airports_collection.insert_one(doc)
            
        print("Миграция данных завершена успешно!")
        print(f"Документов в bookings: {bookings_collection.count_documents({})}")
        print(f"Документов в airports: {airports_collection.count_documents({})}")

    except Exception as e:
        print(f"Ошибка миграции: {str(e)}")
    finally:
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    main()
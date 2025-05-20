from pymongo import MongoClient

# Конфигурация подключения
MONGO_CONFIG = {
   "host": "192.168.50.24",
    "port": 27017,
    "username": "root",      # Добавляем имя пользователя
    "password": "root",  # Добавляем пароль
    "authSource": "admin",              # Указываем базу аутентификации
}

def airports_aggregation():
    client = MongoClient(**MONGO_CONFIG)
    db = client.airline_database
    airports = db.airports

    pipeline = [
        # 1. Разворачиваем массив рейсов
        {"$unwind": "$flights"},
        
        # 2. Проекция для извлечения нужных полей
        {"$project": {
            "_id": 0,
            "airport_code": 1,
            "airport_name": 1,
            "flight_no": "$flights.flight_no",
            "departure_time": "$flights.departure_time",
            "aircraft_model": "$flights.aircraft",
            "destination": "$flights.arrival_airport"
        }},
        
        # 3. Группировка по модели самолета
        {"$group": {
            "_id": "$aircraft_model",
            "total_flights": {"$sum": 1},
            "airports": {"$addToSet": "$airport_code"},
            "last_flight": {"$last": "$flight_no"}
        }},
        
        # 4. Сортировка по количеству рейсов
        {"$sort": {"total_flights": -1}},
        
        # 5. Ограничение вывода
        {"$limit": 5}
    ]

    try:
        results = airports.aggregate(pipeline)
        
        print("Топ 5 моделей самолетов по количеству рейсов:")
        print("{:<25} {:<15} {:<30} {:<10}".format(
            "Модель", "Рейсов", "Аэропорты", "Последний рейс"
        ))
        
        for doc in results:
            print("{:<25} {:<15} {:<30} {:<10}".format(
                doc['_id'],
                doc['total_flights'],
                ", ".join(doc['airports']),
                doc['last_flight']
            ))
            
    except Exception as e:
        print(f"Ошибка агрегации: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    airports_aggregation()
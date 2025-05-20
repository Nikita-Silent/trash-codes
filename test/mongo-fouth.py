from pymongo import MongoClient
from pprint import pprint

# Конфигурация подключения к MongoDB
MONGO_CONFIG = {
   "host": "192.168.50.24",
    "port": 27017,
    "username": "root",      # Добавляем имя пользователя
    "password": "root",  # Добавляем пароль
    "authSource": "admin",              # Указываем базу аутентификации
    "authMechanism": "SCRAM-SHA-256"    # Механизм аутентификации
}

def main():
    # Подключение к MongoDB
    client = MongoClient(**MONGO_CONFIG)
    db = client.airline_database
    bookings = db.bookings

    try:
        # Пайплайн агрегации
        pipeline = [
            {
                "$project": {
                    "_id": 0,
                    "booking_ref": 1,
                    "booking_year": {"$year": "$booking_date"},
                    "total_tickets": {"$size": "$tickets"},
                    "average_price": {
                        "$divide": ["$total_amount", {"$size": "$tickets"}]
                    },
                    "departure_airports": {
                        "$setUnion": ["$tickets.flights.departure_airport"]
                    },
                    "has_international": {
                        "$anyElementTrue": {
                            "$map": {
                                "input": "$tickets.flights",
                                "as": "flight",
                                "in": {"$ne": ["$$flight.status", "Scheduled"]}
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "total_tickets": {"$gt": 1},
                    "has_international": True
                }
            },
            {
                "$sort": {"average_price": -1}
            }
        ]

        # Выполнение агрегации
        results = bookings.aggregate(pipeline)

        # Вывод результатов
        print("{:<12} {:<6} {:<8} {:<12} {:<25} {}".format(
            "Booking Ref", "Year", "Tickets", "Avg Price", 
            "Departure Airports", "International"
        ))
        print("-" * 85)
        
        for doc in results:
            print("{:<12} {:<6} {:<8} {:<12.2f} {:<25} {}".format(
                doc['booking_ref'],
                doc['booking_year'],
                doc['total_tickets'],
                doc['average_price'],
                ", ".join(sorted(doc['departure_airports'])),
                "✓" if doc['has_international'] else "✗"
            ))

    except Exception as e:
        print(f"Ошибка при выполнении агрегации: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
import psycopg2
from pymongo import MongoClient, UpdateMany
from datetime import datetime
import json
from bson import json_util

# Конфигурация подключения к PostgreSQL
POSTGRES_CONFIG = {
    "host": "192.168.50.24",
    "database": "demo",
    "user": "postgres",
    "password": "postgres",
    "port": "5432"
}

# Конфигурация подключения к MongoDB
MONGO_CONFIG = {
   "host": "192.168.50.24",
    "port": 27017,
    "username": "root",      # Добавляем имя пользователя
    "password": "root",  # Добавляем пароль
    "authSource": "admin",              # Указываем базу аутентификации
    "authMechanism": "SCRAM-SHA-256"    # Механизм аутентификации
}

def convert_dates_in_collection():
    """Обновление документов: преобразование строк в даты"""
    try:
        client = MongoClient(**MONGO_CONFIG)
        db = client.airline_database
        bookings = db.bookings
        
        # Находим документы с датами в виде строк
        docs_with_string_dates = bookings.find({
            "booking_date": {"$type": "string"}
        })

        bulk_operations = []
        for doc in docs_with_string_dates:
            try:
                new_date = datetime.fromisoformat(doc['booking_date'])
                bulk_operations.append(
                    UpdateMany(
                        {"_id": doc['_id']},
                        {"$set": {"booking_date": new_date}}
                    )
                )
            except Exception as e:
                print(f"Ошибка в документе {doc['_id']}: {str(e)}")

        if bulk_operations:
            bookings.bulk_write(bulk_operations)
            print(f"Обновлено {len(bulk_operations)} документов")

        client.close()
    except Exception as e:
        print(f"Ошибка подключения: {str(e)}")

def run_aggregation():
    """Выполнение агрегации с преобразованием дат"""
    try:
        client = MongoClient(**MONGO_CONFIG)
        db = client.airline_database
        
        pipeline = [
            {
                "$addFields": {
                    "booking_date": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$booking_date"}, "string"]},
                            "then": {"$dateFromString": {"dateString": "$booking_date"}},
                            "else": "$booking_date"
                        }
                    }
                }
            },
            {
                "$project": {
                    "year": {"$year": "$booking_date"},
                    "month": {"$month": "$booking_date"},
                    "total_tickets": {"$size": "$tickets"},
                    "avg_price": {
                        "$divide": [
                            "$total_amount",
                            {"$cond": [
                                {"$eq": [{"$size": "$tickets"}, 0]},
                                1,
                                {"$size": "$tickets"}
                            ]}
                        ]
                    }
                }
            },
            {"$match": {"total_tickets": {"$gt": 1}}},
            {"$sort": {"year": 1, "month": 1}},
            {"$limit": 10}
        ]

        results = db.bookings.aggregate(pipeline)
        
        print("\nРезультаты агрегации:")
        print("{:<6} {:<6} {:<8} {:<12}".format(
            "Год", "Месяц", "Билеты", "Ср.Цена"
        ))
        for doc in results:
            print(f"{doc['year']:<6} {doc['month']:<6} {doc['total_tickets']:<8} {doc['avg_price']:<12.2f}")

        client.close()
    except Exception as e:
        print(f"Ошибка агрегации: {str(e)}")

if __name__ == "__main__":
    convert_dates_in_collection()
    run_aggregation()
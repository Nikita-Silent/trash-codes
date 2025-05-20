from pymongo import MongoClient
from pprint import pprint
import re

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
        # 1. Запрос на точное совпадение (основной документ)
        print("1. Бронирование с конкретным референсом:")
        exact_match = bookings.find_one({"booking_ref": "00000F"})
        pprint(exact_match)

        # 2. Запрос с оператором сравнения $gt (основной документ)
        print("\n2. Бронирования с суммой > 200000 руб:")
        amount_query = bookings.find({
            "total_amount": {"$gt": 200000}
        }).limit(3)
        for doc in amount_query:
            print(f"Реф: {doc['booking_ref']}, Сумма: {doc['total_amount']}")

        # 3. Запрос по вложенному документу с $elemMatch
        print("\n3. Бронирования с рейсом PG0402:")
        flight_query = bookings.find({
            "tickets.flights": {
                "$elemMatch": {
                    "flight_no": "PG0402",
                    "status": "Arrived"
                }
            }
        })
        for doc in flight_query:
            print(f"Реф: {doc['booking_ref']}")
            print("Рейсы:")
            for ticket in doc['tickets']:
                for flight in ticket['flights']:
                    if flight['flight_no'] == 'PG0402':
                        print(f"- {flight['flight_no']} {flight['departure_airport']}-{flight['arrival_airport']}")

        # 4. Запрос с регулярным выражением (вложенный документ)
        print("\n4. Бронирования с именами пассажиров на 'Иван':")
        regex_query = bookings.find({
            "tickets.passenger": {
                "$regex": r"^Иван",
                "$options": "i"
            }
        })
        for doc in regex_query:
            print(f"\nРеф: {doc['booking_ref']}")
            print("Пассажиры:")
            for ticket in doc['tickets']:
                if re.search(r'^Иван', ticket['passenger'], re.IGNORECASE):
                    print(f"- {ticket['passenger']}")

        # 5. Дополнительный запрос с комбинацией условий
        print("\n5. Бронирования с суммой 100000-200000 руб и московскими аэропортами:")
        complex_query = bookings.find({
            "$and": [
                {"total_amount": {"$gte": 100000, "$lte": 200000}},
                {"tickets.flights.departure_airport": {"$in": ["SVO", "DME", "VKO"]}}
            ]
        })
        for doc in complex_query:
            print(f"\nРеф: {doc['booking_ref']}")
            print(f"Сумма: {doc['total_amount']}")
            print("Аэропорты вылета:")
            airports = {f['departure_airport'] for t in doc['tickets'] for f in t['flights']}
            print(", ".join(airports))

    except Exception as e:
        print(f"Ошибка: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
"""
Moduł do połączenia z bazami danych MongoDB i Redis
"""

from pymongo import MongoClient
from redis import Redis
import json
from datetime import datetime

# Konfiguracja połączeń
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "meteo_db"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Globalne połączenia
mongo_client = None
mongo_db = None
redis_client = None


def connect_mongodb():
    """Nawiązuje połączenie z MongoDB"""
    global mongo_client, mongo_db
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.server_info()  # Test połączenia
        mongo_db = mongo_client[MONGO_DB_NAME]
        print("[OK] Połączono z MongoDB")
        return True
    except Exception as e:
        print(f"[ERROR] Nie można połączyć z MongoDB: {e}")
        return False


def connect_redis():
    """Nawiązuje połączenie z Redis"""
    global redis_client
    try:
        redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=5)
        redis_client.ping()
        print("[OK] Połączono z Redis")
        return True
    except Exception as e:
        print(f"[ERROR] Nie można połączyć z Redis: {e}")
        return False


def connect_all():
    """Nawiązuje połączenie z obiema bazami danych"""
    mongo_ok = connect_mongodb()
    redis_ok = connect_redis()
    return mongo_ok, redis_ok


def close_connections():
    """Zamyka wszystkie połączenia"""
    global mongo_client, redis_client
    if mongo_client:
        mongo_client.close()
    if redis_client:
        redis_client.close()


# ==================== MongoDB Operations ====================

def save_meteo_data_mongo(station_id, date, parameter_code, data):
    """Zapisuje dane meteorologiczne do MongoDB"""
    if mongo_db is None:
        return False

    collection = mongo_db["meteo_data"]
    document = {
        "station_id": station_id,
        "date": date,
        "parameter_code": parameter_code,
        "data": data,
        "created_at": datetime.now()
    }
    collection.update_one(
        {"station_id": station_id, "date": date, "parameter_code": parameter_code},
        {"$set": document},
        upsert=True
    )
    return True


def get_meteo_data_mongo(station_id=None, date=None, parameter_code=None):
    """Pobiera dane meteorologiczne z MongoDB"""
    if mongo_db is None:
        return []

    collection = mongo_db["meteo_data"]
    query = {}
    if station_id:
        query["station_id"] = station_id
    if date:
        query["date"] = date
    if parameter_code:
        query["parameter_code"] = parameter_code

    return list(collection.find(query))


def get_wojewodztwa_mongo():
    """Pobiera listę województw z MongoDB"""
    if mongo_db is None:
        return []

    collection = mongo_db["admin_units"]
    result = collection.find({"type": "wojewodztwo"})
    return [doc["name"] for doc in result]


def get_powiaty_mongo(wojewodztwo=None):
    """Pobiera listę powiatów z MongoDB"""
    if mongo_db is None:
        return []

    collection = mongo_db["admin_units"]
    query = {"type": "powiat"}
    if wojewodztwo:
        query["wojewodztwo"] = wojewodztwo
    result = collection.find(query)
    return [doc["name"] for doc in result]


def save_admin_unit_mongo(name, unit_type, parent=None):
    """Zapisuje jednostkę administracyjną do MongoDB"""
    if mongo_db is None:
        return False

    collection = mongo_db["admin_units"]
    document = {
        "name": name,
        "type": unit_type,
        "parent": parent,
        "created_at": datetime.now()
    }
    collection.update_one(
        {"name": name, "type": unit_type},
        {"$set": document},
        upsert=True
    )
    return True


def get_stations_by_admin_mongo(admin_id, admin_type="powiat"):
    """Pobiera stacje dla danej jednostki administracyjnej"""
    if mongo_db is None:
        return []

    collection = mongo_db["stations"]
    query = {admin_type: admin_id}
    result = collection.find(query)
    return [doc["station_id"] for doc in result]


def save_station_mongo(station_id, name, powiat=None, wojewodztwo=None, lat=None, lon=None):
    """Zapisuje stację do MongoDB"""
    if mongo_db is None:
        return False

    collection = mongo_db["stations"]
    document = {
        "station_id": station_id,
        "name": name,
        "powiat": powiat,
        "wojewodztwo": wojewodztwo,
        "lat": lat,
        "lon": lon,
        "created_at": datetime.now()
    }
    collection.update_one(
        {"station_id": station_id},
        {"$set": document},
        upsert=True
    )
    return True


def get_statistics_mongo(admin_id, date, parameter_code, admin_type="powiat"):
    """Pobiera statystyki dla danej jednostki administracyjnej"""
    if mongo_db is None:
        return None

    collection = mongo_db["statistics"]
    query = {
        "admin_id": admin_id,
        "admin_type": admin_type,
        "date": date,
        "parameter_code": parameter_code
    }
    return collection.find_one(query)


def save_statistics_mongo(admin_id, admin_type, date, parameter_code, stats):
    """Zapisuje statystyki do MongoDB"""
    if mongo_db is None:
        return False

    collection = mongo_db["statistics"]
    document = {
        "admin_id": admin_id,
        "admin_type": admin_type,
        "date": date,
        "parameter_code": parameter_code,
        "stats": stats,
        "created_at": datetime.now()
    }
    collection.update_one(
        {"admin_id": admin_id, "admin_type": admin_type, "date": date, "parameter_code": parameter_code},
        {"$set": document},
        upsert=True
    )
    return True


# ==================== Redis Operations ====================

def cache_set(key, value, expire_seconds=3600):
    """Zapisuje wartość do cache Redis"""
    if redis_client is None:
        return False

    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    redis_client.setex(key, expire_seconds, value)
    return True


def cache_get(key):
    """Pobiera wartość z cache Redis"""
    if redis_client is None:
        return None

    value = redis_client.get(key)
    if value:
        try:
            return json.loads(value)
        except:
            return value
    return None


def cache_delete(key):
    """Usuwa wartość z cache Redis"""
    if redis_client is None:
        return False

    redis_client.delete(key)
    return True


def cache_meteo_stats(admin_id, date, parameter_code, stats, period="dzien"):
    """Cachuje statystyki meteorologiczne"""
    key = f"meteo_stats:{admin_id}:{date}:{parameter_code}:{period}"
    return cache_set(key, stats, expire_seconds=7200)


def get_cached_meteo_stats(admin_id, date, parameter_code, period="dzien"):
    """Pobiera zcachowane statystyki meteorologiczne"""
    key = f"meteo_stats:{admin_id}:{date}:{parameter_code}:{period}"
    return cache_get(key)


def cache_admin_list(admin_type, data):
    """Cachuje listę jednostek administracyjnych"""
    key = f"admin_list:{admin_type}"
    return cache_set(key, data, expire_seconds=86400)  # 24h


def get_cached_admin_list(admin_type):
    """Pobiera zcachowaną listę jednostek administracyjnych"""
    key = f"admin_list:{admin_type}"
    return cache_get(key)


def increment_query_counter(query_type):
    """Inkrementuje licznik zapytań"""
    if redis_client is None:
        return 0

    key = f"query_counter:{query_type}"
    return redis_client.incr(key)


def get_query_counter(query_type):
    """Pobiera licznik zapytań"""
    if redis_client is None:
        return 0

    key = f"query_counter:{query_type}"
    value = redis_client.get(key)
    return int(value) if value else 0


# ==================== Utility Functions ====================

def test_connections():
    """Testuje połączenia z bazami danych"""
    results = {
        "mongodb": False,
        "redis": False
    }

    # Test MongoDB
    try:
        if mongo_client:
            mongo_client.admin.command('ping')
            results["mongodb"] = True
    except:
        pass

    # Test Redis
    try:
        if redis_client:
            redis_client.ping()
            results["redis"] = True
    except:
        pass

    return results


def get_connection_status():
    """Zwraca status połączeń"""
    status = test_connections()
    return {
        "mongodb": "Połączono" if status["mongodb"] else "Brak połączenia",
        "redis": "Połączono" if status["redis"] else "Brak połączenia"
    }

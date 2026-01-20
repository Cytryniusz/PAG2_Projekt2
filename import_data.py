"""
Skrypt do importu danych do MongoDB i Redis
- MongoDB: dane stacji, jednostki administracyjne
- Redis: dane meteorologiczne jako Time Series
"""

import os
import glob
import json
from datetime import datetime
import geopandas as gpd
import pandas as pd
from pymongo import MongoClient
from redis import Redis

# Konfiguracja
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "meteo_db"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

EFFACILITY_PATH = "Dane_administracyjne/effacility.geojson"
ADMIN_VOIV_PATH = "Dane_administracyjne/woj.shp"
ADMIN_COUNTY_PATH = "Dane_administracyjne/powiaty.shp"
DANE_METEO_DIR = "dane_meteo"

PARAMETERS = {
    "B00300S": "Temperatura powietrza",
    "B00305A": "Temperatura gruntu",
    "B00202A": "Kierunek wiatru",
    "B00702A": "Średnia prędkość wiatru 10 min",
    "B00703A": "Prędkość maksymalna",
    "B00608S": "Opad 10 min",
    "B00604S": "Opad dobowy",
    "B00606S": "Opad godzinowy",
    "B00802A": "Wilgotność względna",
    "B00714A": "Największy poryw 10 min",
    "B00910A": "Zapas wody w śniegu"
}


def connect_mongodb():
    """Łączy się z MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("[OK] Połączono z MongoDB")
        return client
    except Exception as e:
        print(f"[ERROR] MongoDB: {e}")
        return None


def connect_redis():
    """Łączy się z Redis"""
    try:
        r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print("[OK] Połączono z Redis")

        # Wyłącz błędy przy zapisie RDB (rozwiązuje MISCONF error)
        try:
            r.config_set('stop-writes-on-bgsave-error', 'no')
            print("[OK] Wyłączono stop-writes-on-bgsave-error")
        except Exception as e:
            print(f"[WARNING] Nie można zmienić config Redis: {e}")

        return r
    except Exception as e:
        print(f"[ERROR] Redis: {e}")
        return None


def import_stations_to_mongodb(mongo_client):
    """Importuje dane stacji do MongoDB"""
    print("\n--- Importowanie stacji do MongoDB ---")

    if not os.path.exists(EFFACILITY_PATH):
        print(f"[ERROR] Brak pliku: {EFFACILITY_PATH}")
        return 0

    db = mongo_client[MONGO_DB]
    collection = db["stations"]

    # Wczytaj dane stacji
    eff = gpd.read_file(EFFACILITY_PATH)
    eff = eff.to_crs(4326)  # Konwersja do WGS84

    count = 0
    for _, row in eff.iterrows():
        station_id = str(row.get('ifcid', row.get('id_localid', '')))
        if not station_id:
            continue

        # Pobierz współrzędne
        geom = row.geometry
        lat, lon = None, None
        if geom and not geom.is_empty:
            lon, lat = geom.x, geom.y

        doc = {
            "station_id": station_id,
            "name": str(row.get('name1', row.get('name', station_id))),
            "additional": str(row.get('additional', '')),
            "lat": lat,
            "lon": lon,
            "responsible": str(row.get('responsibl', '')),
            "activity_start": str(row.get('activitype', '')),
            "updated_at": datetime.now()
        }

        collection.update_one(
            {"station_id": station_id},
            {"$set": doc},
            upsert=True
        )
        count += 1

    # Utwórz indeks
    collection.create_index("station_id", unique=True)

    print(f"[OK] Zaimportowano {count} stacji")
    return count


def import_admin_units_to_mongodb(mongo_client):
    """Importuje jednostki administracyjne do MongoDB"""
    print("\n--- Importowanie jednostek administracyjnych do MongoDB ---")

    db = mongo_client[MONGO_DB]
    collection = db["admin_units"]

    count = 0

    # Województwa
    if os.path.exists(ADMIN_VOIV_PATH):
        try:
            voiv = gpd.read_file(ADMIN_VOIV_PATH)
            voiv = voiv.to_crs(4326)

            # Znajdź kolumnę z nazwą
            name_col = None
            for col in ['nazwa', 'name', 'NAME', 'JPT_NAZWA_', 'jpt_nazwa_', 'id']:
                if col in voiv.columns:
                    name_col = col
                    break

            if name_col is None and len(voiv.columns) > 1:
                # Użyj pierwszej kolumny tekstowej
                for col in voiv.columns:
                    if voiv[col].dtype == 'object' and col != 'geometry':
                        name_col = col
                        break

            if name_col:
                for _, row in voiv.iterrows():
                    name = str(row[name_col])
                    geom = row.geometry
                    centroid = geom.centroid if geom else None

                    doc = {
                        "name": name,
                        "type": "wojewodztwo",
                        "lat": centroid.y if centroid else None,
                        "lon": centroid.x if centroid else None,
                        "updated_at": datetime.now()
                    }

                    collection.update_one(
                        {"name": name, "type": "wojewodztwo"},
                        {"$set": doc},
                        upsert=True
                    )
                    count += 1

                print(f"[OK] Zaimportowano {len(voiv)} województw")
        except Exception as e:
            print(f"[WARNING] Błąd przy województwach: {e}")

    # Powiaty
    if os.path.exists(ADMIN_COUNTY_PATH):
        try:
            county = gpd.read_file(ADMIN_COUNTY_PATH)
            county = county.to_crs(4326)

            # Znajdź kolumnę z nazwą
            name_col = None
            for col in ['nazwa', 'name', 'NAME', 'JPT_NAZWA_', 'jpt_nazwa_', 'id']:
                if col in county.columns:
                    name_col = col
                    break

            if name_col is None and len(county.columns) > 1:
                for col in county.columns:
                    if county[col].dtype == 'object' and col != 'geometry':
                        name_col = col
                        break

            if name_col:
                for _, row in county.iterrows():
                    name = str(row[name_col])
                    geom = row.geometry
                    centroid = geom.centroid if geom else None

                    doc = {
                        "name": name,
                        "type": "powiat",
                        "lat": centroid.y if centroid else None,
                        "lon": centroid.x if centroid else None,
                        "updated_at": datetime.now()
                    }

                    collection.update_one(
                        {"name": name, "type": "powiat"},
                        {"$set": doc},
                        upsert=True
                    )
                    count += 1

                print(f"[OK] Zaimportowano {len(county)} powiatów")
        except Exception as e:
            print(f"[WARNING] Błąd przy powiatach: {e}")

    # Indeksy
    collection.create_index([("name", 1), ("type", 1)])
    collection.create_index("type")

    print(f"[OK] Łącznie zaimportowano {count} jednostek administracyjnych")
    return count


def download_sample_meteo_data():
    """Pobiera przykładowe dane meteo z IMGW"""
    import requests
    import zipfile
    from io import BytesIO

    year, month = 2024, 10
    ym = f"{year}-{month:02d}"
    target_dir = os.path.join(DANE_METEO_DIR, ym)

    if os.path.isdir(target_dir) and os.listdir(target_dir):
        print(f"[INFO] Dane IMGW {ym} już istnieją")
        return target_dir

    os.makedirs(target_dir, exist_ok=True)

    url = f"https://danepubliczne.imgw.pl/pl/datastore/getfiledown/Arch/Telemetria/Meteo/{year}/Meteo_{ym}.zip"

    print(f"[INFO] Pobieranie danych IMGW z {url}...")
    try:
        r = requests.get(url, timeout=300)
        r.raise_for_status()

        with zipfile.ZipFile(BytesIO(r.content)) as z:
            z.extractall(target_dir)

        print(f"[OK] Pobrano dane do {target_dir}")
        return target_dir
    except Exception as e:
        print(f"[ERROR] Nie udało się pobrać danych: {e}")
        return None


def import_meteo_to_redis(redis_client, mongo_client):
    """Importuje dane meteorologiczne do Redis (szybka wersja wektoryzowana)"""
    print("\n--- Importowanie danych meteorologicznych do Redis ---")

    # Sprawdź czy są dane
    if not os.path.exists(DANE_METEO_DIR):
        print("[INFO] Brak folderu dane_meteo, próbuję pobrać...")
        download_sample_meteo_data()

    # Znajdź wszystkie pliki CSV
    csv_files = glob.glob(os.path.join(DANE_METEO_DIR, "**", "*.csv"), recursive=True)

    if not csv_files:
        print("[WARNING] Brak plików CSV z danymi meteorologicznymi")
        print("[INFO] Tworzę przykładowe dane demonstracyjne...")
        create_demo_meteo_data(redis_client)
        return 0

    print(f"[INFO] Znaleziono {len(csv_files)} plików CSV")

    total_records = 0
    BATCH_SIZE = 10000

    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        print(f"[INFO] Przetwarzanie: {filename}...", end=" ", flush=True)

        try:
            # Wczytaj plik w kawałkach (chunks) dla oszczędności pamięci
            chunks = pd.read_csv(
                csv_file,
                header=None,
                delimiter=";",
                usecols=[0, 1, 2, 3],
                names=["KodSH", "ParametrSH", "Data", "Value"],
                dtype=str,
                on_bad_lines="skip",
                chunksize=50000
            )

            file_records = 0

            for df in chunks:
                # Filtruj znane parametry
                df = df[df["ParametrSH"].isin(PARAMETERS.keys())].copy()

                if df.empty:
                    continue

                # Konwersja wartości - wektoryzacja
                df["Value"] = df["Value"].str.replace(",", ".", regex=False)
                df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
                df = df.dropna(subset=["Value"])

                if df.empty:
                    continue

                # Konwersja timestamp - wektoryzacja
                df["Timestamp"] = pd.to_datetime(df["Data"], errors="coerce")
                df = df.dropna(subset=["Timestamp"])
                df["ts_ms"] = (df["Timestamp"].astype('int64') // 10**6).astype(int)

                # Grupuj po kluczu i zapisz batch'ami
                pipe = redis_client.pipeline()
                batch_count = 0

                for _, row in df.iterrows():
                    key = f"meteo:{row['KodSH']}:{row['ParametrSH']}"
                    ts_ms = row["ts_ms"]
                    value = row["Value"]

                    pipe.zadd(key, {f"{ts_ms}:{value}": ts_ms})
                    batch_count += 1
                    file_records += 1

                    if batch_count >= BATCH_SIZE:
                        pipe.execute()
                        pipe = redis_client.pipeline()
                        batch_count = 0

                # Zapisz pozostałe
                if batch_count > 0:
                    pipe.execute()

            total_records += file_records
            print(f"OK ({file_records} rekordów)")

        except Exception as e:
            print(f"BŁĄD: {e}")

    print(f"[OK] Zaimportowano łącznie {total_records} rekordów meteorologicznych do Redis")
    return total_records


def create_demo_meteo_data(redis_client):
    """Tworzy przykładowe dane demonstracyjne"""
    print("[INFO] Tworzenie danych demonstracyjnych...")

    import random
    from datetime import timedelta

    # Przykładowe stacje
    stations = ["249200160", "249190100", "250200090", "252220120", "250210100"]

    # Parametry i zakresy wartości
    param_ranges = {
        "B00300S": (-5, 25),    # Temperatura powietrza
        "B00305A": (-3, 20),    # Temperatura gruntu
        "B00802A": (40, 100),   # Wilgotność
        "B00702A": (0, 15),     # Prędkość wiatru
        "B00202A": (0, 360),    # Kierunek wiatru
        "B00608S": (0, 5),      # Opad 10 min
        "B00606S": (0, 20),     # Opad godzinowy
        "B00604S": (0, 50),     # Opad dobowy
    }

    # Generuj dane dla ostatnich 30 dni
    base_date = datetime(2024, 10, 1)

    count = 0
    for station_id in stations:
        for param_code, (min_val, max_val) in param_ranges.items():
            for day in range(30):
                for hour in range(24):
                    timestamp = base_date + timedelta(days=day, hours=hour)
                    ts_ms = int(timestamp.timestamp() * 1000)

                    # Wartość z pewną zmiennością
                    value = random.uniform(min_val, max_val)

                    key = f"meteo:{station_id}:{param_code}"
                    redis_client.zadd(key, {f"{ts_ms}:{value:.2f}": ts_ms})
                    count += 1

    print(f"[OK] Utworzono {count} rekordów demonstracyjnych")
    return count


def create_station_admin_mapping(mongo_client):
    """Tworzy mapowanie stacji do jednostek administracyjnych"""
    print("\n--- Tworzenie mapowania stacji do jednostek administracyjnych ---")

    db = mongo_client[MONGO_DB]

    if not os.path.exists(EFFACILITY_PATH):
        print("[ERROR] Brak pliku stacji")
        return

    eff = gpd.read_file(EFFACILITY_PATH)

    # Wczytaj województwa i powiaty
    voiv_gdf = None
    county_gdf = None

    if os.path.exists(ADMIN_VOIV_PATH):
        voiv_gdf = gpd.read_file(ADMIN_VOIV_PATH)

    if os.path.exists(ADMIN_COUNTY_PATH):
        county_gdf = gpd.read_file(ADMIN_COUNTY_PATH)

    if voiv_gdf is None and county_gdf is None:
        print("[WARNING] Brak danych administracyjnych do mapowania")
        return

    # Konwersja CRS
    if voiv_gdf is not None:
        eff_voiv = eff.to_crs(voiv_gdf.crs)
        joined_voiv = gpd.sjoin(eff_voiv, voiv_gdf, predicate="within", how="left")

    if county_gdf is not None:
        eff_county = eff.to_crs(county_gdf.crs)
        joined_county = gpd.sjoin(eff_county, county_gdf, predicate="within", how="left")

    # Aktualizuj stacje w MongoDB
    collection = db["stations"]

    count = 0
    for idx, row in eff.iterrows():
        station_id = str(row.get('ifcid', row.get('id_localid', '')))
        if not station_id:
            continue

        update_doc = {}

        # Dodaj województwo
        if voiv_gdf is not None and idx < len(joined_voiv):
            voiv_row = joined_voiv.iloc[idx]
            # Szukaj kolumny z nazwą - sjoin dodaje suffix _right
            for col in ['name_right', 'name', 'nazwa_right', 'nazwa', 'NAME_right', 'NAME']:
                if col in voiv_row.index and pd.notna(voiv_row[col]):
                    val = str(voiv_row[col])
                    # Sprawdź czy to nie jest ID numeryczne
                    if not val.replace('.', '').replace('-', '').isdigit():
                        update_doc["wojewodztwo"] = val
                        break

        # Dodaj powiat
        if county_gdf is not None and idx < len(joined_county):
            county_row = joined_county.iloc[idx]
            # Szukaj kolumny z nazwą - sjoin dodaje suffix _right
            for col in ['name_right', 'name', 'nazwa_right', 'nazwa', 'NAME_right', 'NAME']:
                if col in county_row.index and pd.notna(county_row[col]):
                    val = str(county_row[col])
                    # Sprawdź czy to nie jest ID numeryczne
                    if not val.replace('.', '').replace('-', '').isdigit():
                        update_doc["powiat"] = val
                        break

        if update_doc:
            collection.update_one(
                {"station_id": station_id},
                {"$set": update_doc}
            )
            count += 1

    print(f"[OK] Zaktualizowano mapowanie dla {count} stacji")


def setup_redis_indexes(redis_client):
    """Konfiguruje indeksy i metadane w Redis"""
    print("\n--- Konfiguracja Redis ---")

    # Zapisz listę parametrów
    for code, name in PARAMETERS.items():
        redis_client.hset("meteo:parameters", code, name)

    # Zapisz metadane
    redis_client.set("meteo:last_update", datetime.now().isoformat())

    print("[OK] Skonfigurowano Redis")


def verify_import(mongo_client, redis_client):
    """Weryfikuje import danych"""
    print("\n" + "=" * 50)
    print("WERYFIKACJA IMPORTU")
    print("=" * 50)

    db = mongo_client[MONGO_DB]

    # MongoDB
    stations_count = db["stations"].count_documents({})
    admin_count = db["admin_units"].count_documents({})

    print(f"\nMongoDB:")
    print(f"  - Stacje: {stations_count}")
    print(f"  - Jednostki administracyjne: {admin_count}")

    # Przykładowa stacja
    sample_station = db["stations"].find_one()
    if sample_station:
        print(f"  - Przykładowa stacja: {sample_station.get('station_id')} - {sample_station.get('name')}")

    # Redis
    meteo_keys = redis_client.keys("meteo:*:*")
    print(f"\nRedis:")
    print(f"  - Klucze meteo: {len(meteo_keys)}")

    # Przykładowe dane
    if meteo_keys:
        sample_key = meteo_keys[0]
        sample_data = redis_client.zrange(sample_key, 0, 2, withscores=True)
        print(f"  - Przykładowy klucz: {sample_key}")
        if sample_data:
            print(f"  - Przykładowe dane: {sample_data[:3]}")

    print("\n" + "=" * 50)


def main():
    print("=" * 60)
    print("IMPORT DANYCH DO MONGODB I REDIS")
    print("=" * 60)

    # Połącz z bazami
    mongo_client = connect_mongodb()
    redis_client = connect_redis()

    if not mongo_client or not redis_client:
        print("[ERROR] Nie można połączyć z bazami danych")
        print("Uruchom najpierw: .\\setup_databases.ps1")
        return

    try:
        # Import do MongoDB
        import_stations_to_mongodb(mongo_client)
        import_admin_units_to_mongodb(mongo_client)
        create_station_admin_mapping(mongo_client)

        # Import do Redis
        import_meteo_to_redis(redis_client, mongo_client)
        setup_redis_indexes(redis_client)

        # Weryfikacja
        verify_import(mongo_client, redis_client)

        print("\n[SUCCESS] Import zakończony pomyślnie!")
        print("Możesz teraz uruchomić aplikację: python main_gui.py")

    finally:
        mongo_client.close()
        redis_client.close()


if __name__ == "__main__":
    main()

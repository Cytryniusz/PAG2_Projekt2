import os
import io
import requests
import zipfile

# ------------------------------
# Konfiguracja
# ------------------------------

BASE_DIR = "dane_meteo"

# Generowanie poprawnych URL-i IMGW
VALID_URLS = {
    f"2024-{str(m).zfill(2)}":
        f"https://danepubliczne.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/2024/Meteo_2024-{str(m).zfill(2)}.zip"
    for m in range(1, 13)
}

# ------------------------------
# Funkcja pobierająca i rozpakowująca
# ------------------------------

def download_and_extract(url, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    print(f"\nPobieram: {url}")

    # Pobranie pliku
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print("[ERROR] Błąd pobierania:", e)
        return

    # Diagnostyka
    print("Content-Type:", resp.headers.get("Content-Type"))
    print("Rozmiar (B):", len(resp.content))

    # Sprawdzenie czy to jest poprawny ZIP
    bio = io.BytesIO(resp.content)
    if not zipfile.is_zipfile(bio):
        print("[ERROR] Ten URL nie zwrócił ZIP-a -> pomijam")
        return

    # Rozpakowanie
    try:
        with zipfile.ZipFile(bio) as z:
            z.extractall(target_dir)
        print("[OK] Rozpakowano do:", target_dir)
    except Exception as e:
        print("[ERROR] Błąd rozpakowywania:", e)


# ------------------------------
# Główna logika
# ------------------------------

def main():
    os.makedirs(BASE_DIR, exist_ok=True)

    print("=== START POBIERANIA IMGW ===")
    print(f"Dane będą zapisane w folderze: {BASE_DIR}")

    for month, url in VALID_URLS.items():
        subdir = os.path.join(BASE_DIR, month)   # np. dane_meteo/2024-01
        download_and_extract(url, subdir)

    print("\n=== ZAKOŃCZONO ===")
    print("Sprawdź zawartość folderu:", BASE_DIR)


if __name__ == "__main__":
    main()

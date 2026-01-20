# PAG - Projekt 2
**Autorzy:** Piotr Pawlus, Bartosz Ziółkowski, Szymon Ziędalski

Aplikacja do przetwarzania, wczytywania, analizowania i modelowania danych meteorologicznych, administracyjnych oraz astronomicznych z wykorzystaniem baz danych MongoDB i Redis.

## Wymagania

- Python 3.10+
- MongoDB (portable - automatycznie pobierana)
- Redis (portable - automatycznie pobierany)

## Szybki start

### 1. Zainstaluj zależności Python:
```bash
pip install -r requirements.txt
```

### 2. Uruchom bazy danych (portable - bez uprawnień administratora):
```powershell
.\setup_databases.ps1
```
Skrypt automatycznie pobierze i uruchomi MongoDB oraz Redis.

### 3. Zaimportuj dane:
```bash
python import_data.py
```
Skrypt:
- Zaimportuje stacje meteorologiczne do MongoDB
- Zaimportuje jednostki administracyjne (województwa, powiaty)
- Pobierze dane IMGW z ostatniego miesiąca
- Zaimportuje dane meteorologiczne do Redis

### 4. Uruchom aplikację GUI:
```bash
python main_gui.py
```

## Struktura projektu

```
PAG2_Projekt2/
├── main_gui.py           # Główna aplikacja GUI (tkinter)
├── db_connection.py      # Moduł połączeń z MongoDB i Redis
├── import_data.py        # Import danych do baz
├── app1.py               # Skrypt analizy danych IMGW
├── setup_databases.ps1   # Instalacja i uruchomienie portable MongoDB/Redis
├── stop_databases.ps1    # Zatrzymanie baz danych
├── requirements.txt      # Zależności Python
├── Dane_administracyjne/ # Pliki shapefile (województwa, powiaty, stacje)
│   ├── effacility.geojson
│   ├── woj.*             # Dane województw
│   └── powiaty.*         # Dane powiatów
├── dane_meteo/           # Pobrane dane meteorologiczne IMGW
├── img/                  # Obrazki dla GUI (moon, sun, thermometer, rain, wind)
└── portable_databases/   # Portable MongoDB i Redis
    ├── mongodb/
    ├── redis/
    └── data/
```

## Funkcjonalności

### GUI (`main_gui.py`)
- Nowoczesny interfejs z ciemnym motywem
- Wybór daty z kalendarza
- Wybór województwa lub powiatu z listy rozwijanej
- Obliczanie statystyk meteorologicznych (średnia, mediana)
- Podział na dzień/noc (na podstawie wschodu/zachodu słońca)
- Wizualizacja wyników w trzech kolumnach: Temperatura, Opad, Wiatr
- Cache wyników w Redis z możliwością czyszczenia
- Pasek statusu informujący o aktualnych operacjach

### Parametry meteorologiczne
- **Temperatura powietrza** (B00300S) - średnia i mediana [°C]
- **Temperatura gruntu** (B00305A) - średnia i mediana [°C]
- **Wilgotność względna** (B00802A) - średnia i mediana [%]
- **Prędkość wiatru** (B00702A) - średnia i mediana [m/s]
- **Kierunek wiatru** (B00202A) - średnia i mediana [°]
- **Opad dobowy** (B00608S) - suma [mm]
- **Opad godzinowy** (B00606S) - średnia i mediana [mm]
- **Opad 10-minutowy** (B00604S) - średnia i mediana [mm]
- **Maksymalna prędkość wiatru** (B00703A) - średnia i mediana [m/s]
- **Największy poryw wiatru** (B00714A) - wartość maksymalna [m/s]

### Bazy danych
- **MongoDB**: stacje meteorologiczne, jednostki administracyjne, statystyki
- **Redis**: dane pomiarowe (time series), cache wyników

## Zatrzymanie baz danych

```powershell
.\stop_databases.ps1
```

## Schemat kolorów GUI

Aplikacja wykorzystuje spójny ciemny motyw:
- Tło główne: `#1a1a2e`
- Tło ramek: `#1e2a4a`
- Akcenty: `#e94560` (czerwony/różowy)
- Temperatura: `#ff6b6b`
- Opad: `#4facfe`
- Wiatr: `#a8edea`

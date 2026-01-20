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
├── check_databases.py    # Test połączeń z bazami
├── setup_databases.ps1   # Instalacja i uruchomienie portable MongoDB/Redis
├── stop_databases.ps1    # Zatrzymanie baz danych
├── requirements.txt      # Zależności Python
├── Dane_administracyjne/ # Pliki shapefile (województwa, powiaty, stacje)
├── dane_meteo/           # Pobrane dane meteorologiczne IMGW
├── img/                  # Obrazki dla GUI
└── portable_databases/   # Portable MongoDB i Redis
    ├── mongodb/
    ├── redis/
    └── data/
```

## Funkcjonalności

### GUI (`main_gui.py`)
- Wybór daty z kalendarza
- Wybór województwa lub powiatu
- Obliczanie statystyk meteorologicznych (średnia, mediana)
- Podział na dzień/noc (na podstawie wschodu/zachodu słońca)
- Cache wyników w Redis

### Parametry meteorologiczne
- Temperatura powietrza (B00300S)
- Temperatura gruntu (B00305A)
- Wilgotność względna (B00802A)
- Prędkość wiatru (B00702A)
- Kierunek wiatru (B00202A)
- Opad dobowy, godzinowy, 10-minutowy
- Maksymalna prędkość wiatru i porywy

### Bazy danych
- **MongoDB**: stacje meteorologiczne, jednostki administracyjne, statystyki
- **Redis**: dane pomiarowe (time series), cache wyników

## Zatrzymanie baz danych

```powershell
.\stop_databases.ps1
```

## Funkcjonalności

- Połączenie z MongoDB do przechowywania danych meteorologicznych
- Cache Redis do przyspieszenia zapytań
- GUI do wyboru województwa/powiatu i daty
- Wyświetlanie statystyk: średnia, mediana (dzień/noc)
- Parametry: temperatura, opad, wiatr, wilgotność

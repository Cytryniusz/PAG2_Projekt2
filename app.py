# app.py
import os
import glob
import warnings
from datetime import datetime, date, timedelta

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from astral import LocationInfo
from astral.sun import sun

# --- CONFIG: dostosuj ścieżki/parametry ---
DOWNLOAD_MODULE_NAME = "download"   # Twój skrypt pobierający: download.py (z funkcją main())
DANE_METEO_DIR = "dane_meteo"       # katalog utworzony przez download.py (z podfolderami 2024-01 ...)
EFFACILITY_PATH = "Dane_administracyjne/effacility.geojson"   # plik z położeniem stacji (punktowy)
ADMIN_VOIV_PATH = "Dane_administracyjne/woj.shp"  # adaptuj do własnych ścieżek
ADMIN_COUNTY_PATH = "Dane_administracyjne/powiaty.shp"
OUTPUT_DIR = "wyniki_analizy"
TRIM_PERCENT = 0.1  # 10% obcięcie z obu końców dla średniej obciętej

# Parametry (kody) do analizy — uzupełnij/zmodyfikuj wg potrzeb
PARAMETERS = {
    "B00300S": "air_temperature",
    "B00305A": "ground_temperature",
    "B00202A": "wind_direction",
    "B00702A": "wind_speed_10min",
    "B00703A": "wind_max",
    "B00608S": "precip_10min",
    "B00604S": "precip_daily",
    "B00606S": "precip_hour",
    "B00802A": "relative_humidity",
    "B00714A": "max_gust_10min",
    "B00910A": "snow_water_equivalent"
}

# ---------------- utility ----------------
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_downloader(year=None, months=None):
    """
    Wywołuje funkcję main() z download.py aby pobrać i rozpakować pliki.
    Zakładamy że download.py implementuje funkcję main() lub inne publiczne API.
    """
    try:
        import importlib
        downloader = importlib.import_module(DOWNLOAD_MODULE_NAME)
    except Exception as e:
        raise RuntimeError(f"Nie udało się zaimportować modułu pobierającego '{DOWNLOAD_MODULE_NAME}': {e}")

    # jeśli download.py udostępnia funkcję, wywołujemy ją; większość prostych wersji ma main()
    if hasattr(downloader, "main"):
        # downloader.main() może pobierać docale; domyślna implementacja powinna zapisać do DANE_METEO_DIR
        print("Uruchamiam pobieranie przez download.main() ...")
        downloader.main()
    else:
        raise RuntimeError("download.py nie ma funkcji main(). Edytuj download.py aby udostępniał publiczną funkcję uruchamiającą pobieranie.")

# ---------------- reading & parsing ----------------
def read_parameter_csvs(year_months, parameter_code):
    """
    Wczytuje wszystkie CSV-y dla podanych folderów miesiąca (np. ['2024-01','2024-02'])
    filtruje linie dla danego parameter_code (np. 'B00300S') i zwraca DataFrame z kolumnami:
    KodSH, datetime (pd.Timestamp), value (float)
    """
    rows = []
    for ym in year_months:
        folder = os.path.join(DANE_METEO_DIR, ym)
        if not os.path.isdir(folder):
            print(f"[WARN] Brak folderu: {folder}")
            continue
        csv_files = glob.glob(os.path.join(folder, "*.csv"))
        for f in csv_files:
            try:
                # pliki IMGW zwykle: ; jako delimiter, decimal=','; ale bywa róznie
                df = pd.read_csv(f, header=None, delimiter=';', decimal=',',
                                 usecols=[0,1,2,3], names=['KodSH','ParametrSH','Data','Value'],
                                 dtype={'KodSH': str}, engine='python')
            except Exception:
                try:
                    df = pd.read_csv(f, header=None, delimiter=';', decimal='.',
                                     usecols=[0,1,2,3], names=['KodSH','ParametrSH','Data','Value'],
                                     dtype={'KodSH': str}, engine='python')
                except Exception as e:
                    print(f"[ERROR] Nie wczytano pliku {f}: {e}")
                    continue
            # filtr
            df = df[df['ParametrSH'] == parameter_code].copy()
            if df.empty:
                continue
            # parsowanie daty: IMGW ma format 'YYYY-MM-DD HH:MM' lub podobny; spróbuj parse
            df['datetime'] = pd.to_datetime(df['Data'], errors='coerce', dayfirst=False)

            # UWAGA — BEZPIECZNA KONWERSJA Value, niezależnie od formatu
            df['Value'] = (
                df['Value']
                .astype(str)  # na string
                .str.replace(',', '.', regex=False)  # zamiana przecinka na kropkę
            )

            df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

            df = df[['KodSH', 'datetime', 'Value']].dropna(subset=['datetime', 'Value'])

            rows.append(df)
    if not rows:
        return pd.DataFrame(columns=['KodSH','datetime','Value'])
    result = pd.concat(rows, ignore_index=True)
    return result

# ---------------- astronomical day/night ----------------
def compute_day_night(df_obs, effacility_gdf):
    """
    df_obs: DataFrame with KodSH, datetime, Value
    effacility_gdf: GeoDataFrame zawierający stacje; musi mieć kolumny 'ifcid' (lub 'KodSH') i geometry (Point)
    Zwraca df_obs z dodatkową kolumną 'daynight' z wartościami 'day'/'night'
    """
    # map code -> (lat, lon)
    eff = effacility_gdf.copy()
    # ensure geometry in lat/lon
    if eff.crs and eff.crs.is_projected:
        eff = eff.to_crs(epsg=4326)
    eff['lat'] = eff.geometry.y
    eff['lon'] = eff.geometry.x

    # map KodSH -> lat/lon; w effifility kolumna może być 'ifcid' lub 'KodSH' - inspekcja
    code_field = None
    for candidate in ['ifcid','KodSH','kod','kod_stacji','IFCID']:
        if candidate in eff.columns:
            code_field = candidate
            break
    if code_field is None:
        raise RuntimeError("Nie znaleziono pola identyfikującego KodSH w pliku effacility.geojson")

    loc_map = eff.set_index(code_field)[['lat','lon']].to_dict('index')

    # funkcja określająca dla pojedynczego wiersza day/night
    def row_daynight(row):
        code = row['KodSH']
        dt = row['datetime']
        if pd.isna(code) or code not in loc_map:
            return 'unknown'
        loc = loc_map[code]
        try:
            # Astral potrzebuje LocationInfo (nazwa, region, tz, lat, lon). TZ nie jest krytyczne do obliczeń Słońca,
            # astral użyje daty i współrzędnych; używamy tz='UTC' i operujemy na dt in UTC if naive => assume local
            li = LocationInfo(name=str(code), region="", timezone="UTC", latitude=float(loc['lat']), longitude=float(loc['lon']))
            s = sun(li.observer, date=dt.date())
            # s zawiera 'sunrise' i 'sunset' (timezone-aware). Porównujemy w UTC; dt może być naive - porównamy datetimes na das podstawie dat.
            sunrise = s['sunrise']
            sunset = s['sunset']
            # if dt tz naive -> make it naive comparable by removing tzinfo from sunrise/sunset
            if dt.tzinfo is None:
                sunrise = sunrise.replace(tzinfo=None)
                sunset = sunset.replace(tzinfo=None)
            if sunrise <= dt <= sunset:
                return 'day'
            else:
                return 'night'
        except Exception:
            return 'unknown'

    # Apply: dla dużych zbiorów wydajność będzie problemem (astral per row). Dlatego zastosujemy grupowanie po stacji i dacie.
    df = df_obs.copy()
    df['date'] = df['datetime'].dt.date

    # przygotuj słownik day/night per station+date (cache)
    daynight_cache = {}

    results = []
    for (kod, d), group in df.groupby(['KodSH','date']):
        key = (kod, d)
        if key in daynight_cache:
            val = daynight_cache[key]
            group = group.copy()
            group['daynight'] = val
            results.append(group)
            continue
        # potrzebujemy dostępu do lokalizacji
        if kod not in loc_map:
            val = 'unknown'
            daynight_cache[key] = val
            group = group.copy()
            group['daynight'] = val
            results.append(group)
            continue
        loc = loc_map[kod]
        try:
            li = LocationInfo(name=str(kod), region="", timezone="UTC", latitude=float(loc['lat']), longitude=float(loc['lon']))
            s = sun(li.observer, date=d)
            sunrise = s['sunrise'].replace(tzinfo=None)
            sunset = s['sunset'].replace(tzinfo=None)
            # dla całej grupy: przypisz 'day' dla godziny między sunrise a sunset, inaczej 'night'
            group = group.copy()
            group['daynight'] = group['datetime'].apply(lambda dt: 'day' if (sunrise <= dt.replace(tzinfo=None) <= sunset) else 'night')
            # cache unique value? not uniform across day (może zawierać i day i night) -> cache default None
            daynight_cache[key] = None
            results.append(group)
        except Exception:
            group = group.copy()
            group['daynight'] = 'unknown'
            results.append(group)
            daynight_cache[key] = 'unknown'
    df_out = pd.concat(results, ignore_index=True) if results else df
    return df_out

# ---------------- statistical computations ----------------
def trimmed_mean(series, trim=TRIM_PERCENT):
    ser = series.dropna().sort_values()
    n = len(ser)
    if n == 0:
        return float('nan')
    k = int(n * trim)
    if 2*k >= n:
        # zbyt mało danych, zwróć zwykłą mean
        return ser.mean()
    return ser.iloc[k: n - k].mean()

def compute_daily_stats(df_obs_with_dn):
    """
    df_obs_with_dn: KodSH, datetime, Value, daynight
    Zwraca DataFrame z agregacją: KodSH, date, daynight, mean, median, trimmed_mean, count
    """
    df = df_obs_with_dn.copy()
    df['date'] = df['datetime'].dt.date
    agg = df.groupby(['KodSH','date','daynight'])['Value'].agg([
        ('mean','mean'),
        ('median','median'),
        ('count','count')
    ]).reset_index()
    # compute trimmed mean per group manually
    tm_list = []
    for (kod,d,dn), group in df.groupby(['KodSH','date','daynight']):
        tm = trimmed_mean(group['Value'])
        tm_list.append({'KodSH':kod,'date':d,'daynight':dn,'trimmed_mean':tm})
    tm_df = pd.DataFrame(tm_list)
    res = agg.merge(tm_df, on=['KodSH','date','daynight'], how='left')
    return res

# ---------------- spatial aggregations ----------------
def aggregate_by_admin(stats_df, effacility_gdf, admin_gdf, admin_id_field, output_prefix):
    """
    Łączy statystyki (stats_df zawiera KodSH) z geometrią stacji i agreguje średnie/mediany po admin unit.
    Zapisuje plik CSV z wynikami.
    """
    # przygotuj stacje z KodSH jako string i geometrią
    eff = effacility_gdf.copy()
    if eff.crs and eff.crs.is_projected:
        eff = eff.to_crs(epsg=4326)
    # odnajdź id pola kodu w eff
    code_field = None
    for c in ['ifcid','KodSH','kod','IFCID']:
        if c in eff.columns:
            code_field = c
            break
    if code_field is None:
        raise RuntimeError("Nie znaleziono pola identyfikującego KodSH w effacility")
    eff[code_field] = eff[code_field].astype(str)
    # scal statystyki z geometrią (join left)
    stat = stats_df.copy()
    stat['KodSH'] = stat['KodSH'].astype(str)
    merged = stat.merge(eff[[code_field,'geometry']], left_on='KodSH', right_on=code_field, how='left')
    merged_gdf = gpd.GeoDataFrame(merged, geometry='geometry', crs=eff.crs)
    # spatial join: punkty -> admin polygons
    admin = admin_gdf.copy()
    if admin.crs != merged_gdf.crs:
        admin = admin.to_crs(merged_gdf.crs)
    joined = gpd.sjoin(merged_gdf, admin[[admin_id_field,'geometry']], how='left', predicate='within')
    # agregacja: dla każdego admin_id + date + daynight: mean(mean), median(median)
    grp = joined.groupby([admin_id_field,'date','daynight']).agg({
        'mean':'mean',
        'median':'median',
        'trimmed_mean':'mean',
        'count':'sum'
    }).reset_index()
    out_csv = os.path.join(OUTPUT_DIR, f"{output_prefix}_{admin_id_field}_agg.csv")
    grp.to_csv(out_csv, index=False)
    print(f"[OK] Zapisano agregacje administracyjne: {out_csv}")
    return grp

# ---------------- change over interval ----------------
def compute_change_over_interval(agg_df, start_date, end_date, value_field='mean'):
    """
    agg_df: tabela z kolumnami [admin_id_field, date, daynight, mean,...]
    start_date, end_date: date or str
    Zwraca różnicę (end - start) per admin and daynight
    """
    sd = pd.to_datetime(start_date).date()
    ed = pd.to_datetime(end_date).date()
    a = agg_df[agg_df['date']==sd].set_index(['admin_id_field','daynight'])
    b = agg_df[agg_df['date']==ed].set_index(['admin_id_field','daynight'])
    # align indices
    idx = sorted(set(a.index).union(set(b.index)))
    rows = []
    for key in idx:
        admin_id, dn = key
        val_a = a.loc[key, value_field] if key in a.index else float('nan')
        val_b = b.loc[key, value_field] if key in b.index else float('nan')
        diff = val_b - val_a if pd.notna(val_a) and pd.notna(val_b) else float('nan')
        rows.append({'admin_id_field':admin_id,'daynight':dn,'start':sd,'end':ed,'change':diff})
    return pd.DataFrame(rows)

# ---------------- main workflow ----------------
def run_analysis(year_months=None, parameter_codes=None):
    """
    year_months: list like ['2024-01','2024-02'] or None => we will scan DANE_METEO_DIR
    parameter_codes: list of parameter codes to analyze (keys from PARAMETERS)
    """
    ensure_dirs()

    # 1. jeśli katalog z danymi pusty, wywołaj downloader
    if not os.path.isdir(DANE_METEO_DIR) or not any(os.scandir(DANE_METEO_DIR)):
        print("[INFO] Wywołanie modułu pobierającego dane...")
        run_downloader()

    # 2. wybierz foldery miesięczne do analizy
    if year_months is None:
        # lista folderów w DANE_METEO_DIR zaczynających się od '2024-' lub ogólnie foldery
        year_months = sorted([d for d in os.listdir(DANE_METEO_DIR) if os.path.isdir(os.path.join(DANE_METEO_DIR,d))])
        print("[INFO] Wybrane miesiące:", year_months)

    # 3. wczytaj effacility (stacje)
    if not os.path.exists(EFFACILITY_PATH):
        raise FileNotFoundError(f"Nie znaleziono pliku effacility: {EFFACILITY_PATH}")
    eff = gpd.read_file(EFFACILITY_PATH)
    print(f"[INFO] Wczytano {len(eff)} stacji z {EFFACILITY_PATH}")

    # 4. wczytaj warstwy administracyjne
    admin_voiv = gpd.read_file(ADMIN_VOIV_PATH)
    admin_county = gpd.read_file(ADMIN_COUNTY_PATH)
    print(f"[INFO] Wczytano warstwy administracyjne (woj: {len(admin_voiv)}, powiaty: {len(admin_county)})")

    # Dla każdego parametru: wczytanie, day/night, statystyki, agregacje admin, zapis wyników
    if parameter_codes is None:
        parameter_codes = list(PARAMETERS.keys())

    for code in parameter_codes:
        print(f"\n=== ANALIZA PARAMETRU: {code} ({PARAMETERS.get(code)}) ===")
        obs = read_parameter_csvs(year_months, code)
        if obs.empty:
            print("[WARN] Brak obserwacji dla parametru:", code)
            continue
        # compute day/night labels
        obs_dn = compute_day_night(obs, eff)
        # per-station daily stats
        stats = compute_daily_stats(obs_dn)
        # zapisz per-station stats
        out_path = os.path.join(OUTPUT_DIR, f"station_stats_{code}.csv")
        stats.to_csv(out_path, index=False)
        print(f"[OK] Zapisano statystyki stacyjne: {out_path}")

        # agregacje admin: wojewodztwa
        # need admin id field - adapt to your admin data column names, here assume 'woj_id' and 'powiat_id' or 'NOM' etc.
        # user may need to change admin id field names to actual fields in their shapefiles
        voiv_id_field = 'id'
        county_id_field = 'id'

        agg_voiv = aggregate_by_admin(stats, eff, admin_voiv, voiv_id_field, output_prefix=f"{code}_voiv")
        agg_county = aggregate_by_admin(stats, eff, admin_county, county_id_field, output_prefix=f"{code}_county")

    print("\n=== ANALIZA ZAKOŃCZONA ===")
    print("Wyniki w folderze:", OUTPUT_DIR)

# ---------------- run as script ----------------
if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    # przykładowe uruchomienie: wszystkie dostępne miesiące i wszystkie parametry
    run_analysis()

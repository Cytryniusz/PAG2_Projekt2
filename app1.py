import os
import glob
import zipfile
import warnings
import requests
from io import BytesIO
import argparse

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from scipy.stats import trim_mean
from astral import LocationInfo
from astral.sun import sun
import pytz

# ================== KONFIGURACJA ==================

DANE_METEO_DIR = "dane_meteo"
OUTPUT_DIR = "wyniki_analizy"

EFFACILITY_PATH = "Dane_administracyjne/effacility.geojson"
ADMIN_VOIV_PATH = "Dane_administracyjne/woj.shp"
ADMIN_COUNTY_PATH = "Dane_administracyjne/powiaty.shp"

TRIM_PROP = 0.1
CHANGE_FREQ = "7D"

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

# ================== UTILS ==================

def ensure_dirs():
    os.makedirs(DANE_METEO_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================== 1. IMGW ==================

def download_imgw_data(year, month):
    ym = f"{year}-{month:02d}"
    target_dir = os.path.join(DANE_METEO_DIR, ym)

    if os.path.isdir(target_dir) and os.listdir(target_dir):
        print(f"[INFO] Dane IMGW {ym} już istnieją – pomijam pobieranie")
        return

    os.makedirs(target_dir, exist_ok=True)

    url = (
        "https://danepubliczne.imgw.pl/pl/datastore/getfiledown/"
        f"Arch/Telemetria/Meteo/{year}/Meteo_{year}-{month:02d}.zip"
    )

    r = requests.get(url, timeout=120)
    r.raise_for_status()

    with zipfile.ZipFile(BytesIO(r.content)) as z:
        z.extractall(target_dir)

# ================== 2. PANDAS ==================

def read_parameter_csvs(year_months, parameter_code):
    data = []

    for ym in year_months:
        for f in glob.glob(os.path.join(DANE_METEO_DIR, ym, "*.csv")):
            df = pd.read_csv(
                f, header=None, delimiter=";",
                usecols=[0,1,2,3],
                names=["KodSH","ParametrSH","Data","Value"],
                dtype=str, on_bad_lines="skip"
            )

            df = df[df["ParametrSH"] == parameter_code]
            if df.empty:
                continue

            df["Value"] = df["Value"].str.replace(",", ".").astype(float)
            df["datetime"] = pd.to_datetime(df["Data"], errors="coerce")
            df = df.dropna()

            data.append(df[["KodSH","datetime","Value"]])

    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

# ================== 3. DZIEŃ / NOC ==================

def add_day_night_astral(df, eff):
    eff = eff.to_crs(4326)
    eff["KodSH"] = eff.iloc[:,0].astype(str)
    df["KodSH"] = df["KodSH"].astype(str)

    df = df.merge(eff[["KodSH","geometry"]], on="KodSH", how="left")
    tz = pytz.timezone("Europe/Warsaw")

    df["date"] = df["datetime"].dt.date
    df["dt_local"] = df["datetime"].apply(tz.localize)

    cache = {}

    def get_sun(kod, geom, d):
        key = (kod, d)
        if key in cache:
            return cache[key]

    # BRAK GEOMETRII → brak możliwości wyznaczenia dnia/nocy
        if geom is None or geom.is_empty:   
            cache[key] = (None, None)
            return cache[key]

        loc = LocationInfo(latitude=geom.y, longitude=geom.x)
        s = sun(loc.observer, date=d, tzinfo=tz)

        cache[key] = (s["sunrise"], s["sunset"])
        return cache[key]

    period = []
    for k, g, d, dt in zip(df.KodSH, df.geometry, df.date, df.dt_local):
        sunrise, sunset = get_sun(k, g, d)

        if sunrise is None:
            period.append("noc")
        elif sunrise <= dt <= sunset:
            period.append("dzien")
        else:
            period.append("noc")

    df["period"] = period
    return df.drop(columns=["geometry","dt_local"])

# ================== 4. STATYSTYKI ==================

def compute_stats(df):
    g = df.groupby(["KodSH","date","period"])["Value"]

    stats = g.agg(mean="mean", median="median", count="count").reset_index()
    trimmed = g.apply(lambda x: trim_mean(x, TRIM_PROP)).rename("trimmed_mean").reset_index()

    return stats.merge(trimmed, on=["KodSH","date","period"])

# ================== 5. GEOANALIZA ==================

def aggregate_by_admin(stats, eff, admin, admin_id, prefix):
    """
    Agregacja statystyk stacji do jednostek administracyjnych
    z automatycznym wykrywaniem pola ID stacji w effacility.
    """

    # === 1. Wykryj pole ID stacji w eff ===
    code_field = None
    for c in ["KodSH", "ifcid", "IFCID", "kod", "station_id", "id"]:
        if c in eff.columns:
            code_field = c
            break

    if code_field is None:
        raise KeyError("Nie znaleziono pola ID stacji w effacility.geojson")

    # === 2. Przygotuj geometrie stacji ===
    eff2 = eff[[code_field, "geometry"]].copy()
    eff2 = eff2.to_crs(admin.crs)

    eff2[code_field] = eff2[code_field].astype(str)
    stats["KodSH"] = stats["KodSH"].astype(str)

    # === 3. Merge statystyki + geometria stacji ===
    gdf = stats.merge(
        eff2,
        left_on="KodSH",
        right_on=code_field,
        how="left"
    )

    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=admin.crs)

    # === 4. Przygotuj jednostki administracyjne ===
    admin2 = admin[[admin_id, "geometry"]].copy()
    if admin2.crs != gdf.crs:
        admin2 = admin2.to_crs(gdf.crs)

    # === 5. Spatial join ===
    joined = gpd.sjoin(gdf, admin2, predicate="within")

    # === 6. Agregacja ===
    agg = joined.groupby(
        [admin_id, "date", "period"], as_index=False
    ).agg(
        mean=("mean", "mean"),
        median=("median", "median"),
        trimmed_mean=("trimmed_mean", "mean"),
        count=("count", "sum")
    )

    # === 7. Zapis ===
    agg.to_csv(
        os.path.join(OUTPUT_DIR, f"{prefix}.csv"),
        index=False
    )

    return agg

def compute_changes(df, admin_id):
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    res = (
        df.groupby([admin_id,"period"])
        .resample(CHANGE_FREQ)
        .agg(mean=("mean","mean"), median=("median","median"))
        .reset_index()
    )

    res["mean_change"] = res.groupby([admin_id,"period"])["mean"].diff()
    res["median_change"] = res.groupby([admin_id,"period"])["median"].diff()
    return res

# ================== 6. WIZUALIZACJA ==================

def plot_changes(changes, admin_id, fname):
    sample = changes.dropna().head(1)[admin_id].iloc[0]
    d = changes[changes[admin_id] == sample]

    plt.plot(d["date"], d["mean_change"], label="Zmiana średniej")
    plt.plot(d["date"], d["median_change"], label="Zmiana mediany")
    plt.legend()
    plt.title("Zmiany wartości w czasie")
    plt.savefig(os.path.join(OUTPUT_DIR, fname), dpi=150)
    plt.close()

# ================== MAIN ==================

def run(year, month):
    ensure_dirs()
    download_imgw_data(year, month)

    ym = f"{year}-{month:02d}"
    eff = gpd.read_file(EFFACILITY_PATH)
    voiv = gpd.read_file(ADMIN_VOIV_PATH)
    county = gpd.read_file(ADMIN_COUNTY_PATH)

    for code, name in PARAMETERS.items():
        obs = read_parameter_csvs([ym], code)
        if obs.empty:
            continue

        obs = add_day_night_astral(obs, eff)
        stats = compute_stats(obs)

        stats.to_csv(os.path.join(OUTPUT_DIR, f"station_{code}.csv"), index=False)

        v = aggregate_by_admin(stats, eff, voiv, "id", f"{code}_voiv")
        p = aggregate_by_admin(stats, eff, county, "id", f"{code}_county")

        vc = compute_changes(v, "id")
        pc = compute_changes(p, "id")

        vc.to_csv(os.path.join(OUTPUT_DIR, f"{code}_voiv_changes.csv"), index=False)
        pc.to_csv(os.path.join(OUTPUT_DIR, f"{code}_county_changes.csv"), index=False)

        plot_changes(vc, "id", f"{code}_voiv_changes.png")

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    run(args.year, args.month)

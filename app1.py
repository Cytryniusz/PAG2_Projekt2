import os
import glob
import zipfile
import requests
from io import BytesIO
import argparse
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from scipy.stats import trim_mean
from astral import LocationInfo
from astral.sun import sun
import pytz
import matplotlib
matplotlib.use("Agg")


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
    files = []
    for ym in year_months:
        files.extend(glob.glob(os.path.join(DANE_METEO_DIR, ym, "*.csv")))

    if not files:
        return pd.DataFrame()

    df = pd.concat(
        (
            pd.read_csv(
                f,
                header=None,
                delimiter=";",
                usecols=[0, 1, 2, 3],
                names=["KodSH", "ParametrSH", "Data", "Value"],
                dtype=str,
                on_bad_lines="skip"
            )
            for f in files
        ),
        ignore_index=True
    )

    df = df[df["ParametrSH"] == parameter_code]
    if df.empty:
        return pd.DataFrame()

    df["Value"] = df["Value"].str.replace(",", ".").astype(float)
    df["datetime"] = pd.to_datetime(df["Data"], errors="coerce")

    return df[["KodSH", "datetime", "Value"]].dropna()

# ================== 3. DZIEŃ / NOC ==================

def add_day_night_astral(df, eff):
    eff = eff.to_crs(4326)
    eff["KodSH"] = eff.iloc[:, 0].astype(str)
    df["KodSH"] = df["KodSH"].astype(str)

    df = df.merge(eff[["KodSH", "geometry"]], on="KodSH", how="left")
    tz = pytz.timezone("Europe/Warsaw")

    df["date"] = df["datetime"].dt.date

    sun_cache = {}

    def get_sun_times(kod, geom, d):
        key = (kod, d)
        if key in sun_cache:
            return sun_cache[key]

        if geom is None or geom.is_empty:
            sun_cache[key] = (None, None)
        else:
            loc = LocationInfo(latitude=geom.y, longitude=geom.x)
            s = sun(loc.observer, date=d, tzinfo=tz)
            sun_cache[key] = (s["sunrise"], s["sunset"])
        return sun_cache[key]

    for _, r in df[["KodSH", "date", "geometry"]].drop_duplicates().iterrows():
        get_sun_times(r["KodSH"], r["geometry"], r["date"])

    df["sun_key"] = list(zip(df["KodSH"], df["date"]))
    sun_df = pd.DataFrame(
        [{"sun_key": k, "sunrise": v[0], "sunset": v[1]} for k, v in sun_cache.items()]
    )

    df = df.merge(sun_df, on="sun_key", how="left")

    df["dt_local"] = (
        df["datetime"]
        .dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
        .dt.tz_convert(tz)
    )

    df["period"] = "noc"
    mask = (
        df["sunrise"].notna()
        & (df["dt_local"] >= df["sunrise"])
        & (df["dt_local"] <= df["sunset"])
    )
    df.loc[mask, "period"] = "dzien"

    return df.drop(columns=["geometry", "dt_local", "sun_key", "sunrise", "sunset"])

# ================== 4. STATYSTYKI ==================

def compute_stats(df):
    g = df.groupby(["KodSH", "date", "period"])["Value"]
    stats = g.agg(mean="mean", median="median", count="count").reset_index()
    trimmed = g.apply(lambda x: trim_mean(x, TRIM_PROP)).rename("trimmed_mean").reset_index()
    return stats.merge(trimmed, on=["KodSH", "date", "period"])

# ================== 5. GEOANALIZA ==================

def aggregate_by_admin(stats, eff, admin, admin_id, prefix):
    code_field = next((c for c in ["KodSH", "ifcid", "IFCID", "kod", "station_id", "id"] if c in eff.columns), None)
    if code_field is None:
        raise KeyError("Brak pola ID stacji w effacility")

    eff2 = eff[[code_field, "geometry"]].copy().to_crs(admin.crs)
    eff2[code_field] = eff2[code_field].astype(str)
    stats["KodSH"] = stats["KodSH"].astype(str)

    gdf = gpd.GeoDataFrame(
        stats.merge(eff2, left_on="KodSH", right_on=code_field, how="left"),
        geometry="geometry",
        crs=admin.crs
    )

    admin2 = admin[[admin_id, "geometry"]].to_crs(gdf.crs)
    joined = gpd.sjoin(gdf, admin2, predicate="within")

    agg = joined.groupby([admin_id, "date", "period"], as_index=False).agg(
        mean=("mean", "mean"),
        median=("median", "median"),
        trimmed_mean=("trimmed_mean", "mean"),
        count=("count", "sum")
    )

    agg.to_csv(os.path.join(OUTPUT_DIR, f"{prefix}.csv"), index=False)
    return agg

def compute_changes(df, admin_id):
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")

    res = (
        df.groupby([admin_id, "period"])
        .resample(CHANGE_FREQ)
        .agg(mean=("mean", "mean"), median=("median", "median"))
        .reset_index()
    )

    res["mean_change"] = res.groupby([admin_id, "period"])["mean"].diff()
    res["median_change"] = res.groupby([admin_id, "period"])["median"].diff()
    return res

# ================== 6. WIZUALIZACJA ==================

def plot_changes(changes, admin_id, fname):
    sample = changes.dropna().iloc[0][admin_id]
    d = changes[changes[admin_id] == sample]

    plt.plot(d["date"], d["mean_change"], label="Zmiana średniej")
    plt.plot(d["date"], d["median_change"], label="Zmiana mediany")
    plt.legend()
    plt.title("Zmiany wartości w czasie")
    plt.savefig(os.path.join(OUTPUT_DIR, fname), dpi=150)
    plt.close()

def process_parameter(code, name, ym, eff, voiv, county):
    obs = read_parameter_csvs([ym], code)
    if obs.empty:
        return

    obs = add_day_night_astral(obs, eff.copy())
    stats = compute_stats(obs)
    stats.to_csv(os.path.join(OUTPUT_DIR, f"station_{code}.csv"), index=False)

    v = aggregate_by_admin(stats, eff, voiv, "id", f"{code}_voiv")
    vc = compute_changes(v, "id")
    vc.to_csv(os.path.join(OUTPUT_DIR, f"{code}_voiv_changes.csv"), index=False)

    if not vc.empty:
        plot_changes(vc, "id", f"{code}_voiv_changes.png")

# ================== MAIN ==================

def run(year, month):
    ensure_dirs()
    download_imgw_data(year, month)

    ym = f"{year}-{month:02d}"
    eff = gpd.read_file(EFFACILITY_PATH)
    voiv = gpd.read_file(ADMIN_VOIV_PATH)
    county = gpd.read_file(ADMIN_COUNTY_PATH)

    with ThreadPoolExecutor(max_workers=4) as ex:
        for f in [
            ex.submit(process_parameter, code, name, ym, eff, voiv, county)
            for code, name in PARAMETERS.items()
        ]:
            f.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analiza danych IMGW")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    run(args.year, args.month)

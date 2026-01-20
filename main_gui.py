"""
Aplikacja GUI do analizy danych meteorologicznych
z wykorzystaniem MongoDB i Redis
"""

import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import Calendar
from PIL import Image, ImageTk, ImageDraw
import os
import threading
from datetime import datetime
import geopandas as gpd

# Import modułów baz danych i analizy
import db_connection as db
from app1 import (
    PARAMETERS, EFFACILITY_PATH, ADMIN_VOIV_PATH, ADMIN_COUNTY_PATH,
    read_parameter_csvs, add_day_night_astral, compute_stats
)

# Kolory motywu
COLORS = {
    "bg_dark": "#1a1a2e",        # Ciemne tło
    "bg_medium": "#16213e",      # Średnie tło
    "bg_light": "#0f3460",       # Jaśniejsze tło
    "accent": "#e94560",         # Akcent (czerwony/różowy)
    "accent_hover": "#ff6b6b",   # Akcent hover
    "text_light": "#ffffff",     # Jasny tekst
    "text_dim": "#a0a0a0",       # Przyciemniony tekst
    "success": "#00d26a",        # Zielony sukces
    "warning": "#ffc107",        # Żółty ostrzeżenie
    "error": "#ff4757",          # Czerwony błąd
    "temp_color": "#ff6b6b",     # Kolor dla temperatury
    "rain_color": "#4facfe",     # Kolor dla opadu
    "wind_color": "#a8edea",     # Kolor dla wiatru
    "frame_bg": "#1e2a4a",       # Tło ramek
}


class MeteoApp:
    def __init__(self, root):
        self.root = root
        self.root.geometry("1300x800")
        self.root.title("🌤️ Analiza danych meteo - MongoDB & Redis")
        self.root.configure(bg=COLORS["bg_dark"])

        # Konfiguracja stylu ttk
        self.setup_styles()

        # Zmienne
        self.selected_date = tk.StringVar()
        self.selected_wojewodztwo = tk.StringVar(value="Wybierz")
        self.selected_powiat = tk.StringVar(value="Wybierz")
        self.db_status_var = tk.StringVar(value="Łączenie...")

        # Dane administracyjne
        self.wojewodztwa = []
        self.powiaty = []
        self.voiv_gdf = None
        self.county_gdf = None
        self.eff_gdf = None

        # Obrazki (będą utworzone jako placeholder)
        self.images = {}

        # Inicjalizacja
        self.create_placeholder_images()
        self.setup_ui()
        self.connect_databases()
        self.load_admin_data()

    def setup_styles(self):
        """Konfiguruje style ttk"""
        style = ttk.Style()
        style.theme_use('clam')

        # Styl dla Combobox
        style.configure("Custom.TCombobox",
                       fieldbackground=COLORS["bg_light"],
                       background=COLORS["bg_medium"],
                       foreground=COLORS["text_light"],
                       arrowcolor=COLORS["accent"],
                       bordercolor=COLORS["accent"],
                       lightcolor=COLORS["bg_light"],
                       darkcolor=COLORS["bg_dark"])

        style.map("Custom.TCombobox",
                 fieldbackground=[('readonly', COLORS["bg_light"])],
                 selectbackground=[('readonly', COLORS["accent"])],
                 selectforeground=[('readonly', COLORS["text_light"])])

    def create_placeholder_images(self):
        """Tworzy placeholder obrazki jeśli nie istnieją"""
        img_dir = "img"
        os.makedirs(img_dir, exist_ok=True)

        # Definicje obrazków z nowymi kolorami
        image_defs = {
            "moon": (COLORS["bg_light"], "#F4D03F"),      # Żółty księżyc
            "sun": ("#FFD700", "#FF8C00"),                 # Złote słońce
            "thermometer": (COLORS["temp_color"], "#C0392B"),  # Czerwony termometr
            "rain": (COLORS["rain_color"], "#2980B9"),     # Niebieski deszcz
            "wind": (COLORS["wind_color"], "#7F8C8D")      # Szary wiatr
        }

        for name, (bg_color, fg_color) in image_defs.items():
            path = os.path.join(img_dir, f"{name}.png")
            if not os.path.exists(path):
                # Tworzenie prostego obrazka
                img = Image.new("RGBA", (50, 50), (255, 255, 255, 0))
                draw = ImageDraw.Draw(img)
                draw.ellipse([5, 5, 45, 45], fill=fg_color, outline=bg_color, width=2)
                img.save(path)

        # Ładowanie obrazków
        for name in image_defs.keys():
            path = os.path.join(img_dir, f"{name}.png")
            try:
                img = Image.open(path)
                if name in ["moon", "sun"]:
                    img = img.resize((24, 24))
                else:
                    img = img.resize((35, 35))
                self.images[name] = ImageTk.PhotoImage(img)
            except Exception as e:
                print(f"[WARNING] Nie można załadować {name}.png: {e}")
                self.images[name] = None

    def setup_ui(self):
        """Tworzy interfejs użytkownika"""
        # Główny kontener
        main_frame = tk.Frame(self.root, bg=COLORS["bg_dark"])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Panel opcji (lewy)
        self.options_frame = tk.LabelFrame(main_frame, text="⚙️ Opcje",
                                           bg=COLORS["bg_medium"],
                                           fg=COLORS["text_light"],
                                           font=("Segoe UI", 11, "bold"),
                                           bd=2, relief=tk.GROOVE)
        self.options_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=5, pady=5)

        # Panel wyników (prawy)
        self.results_frame = tk.LabelFrame(main_frame, text="📊 Wyniki",
                                           bg=COLORS["bg_medium"],
                                           fg=COLORS["text_light"],
                                           font=("Segoe UI", 11, "bold"),
                                           bd=2, relief=tk.GROOVE)
        self.results_frame.pack(side=tk.LEFT, expand=True, fill=tk.BOTH, padx=5, pady=5)

        # Tworzenie panelu opcji
        self.create_options_panel()

        # Tworzenie panelu wyników
        self.create_results_panel()

        # Status bar
        self.create_status_bar()

    def create_options_panel(self):
        """Tworzy panel opcji"""
        # Status baz danych
        db_status_frame = tk.LabelFrame(self.options_frame, text="💾 Status baz danych",
                                        bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                        font=("Segoe UI", 9))
        db_status_frame.pack(fill=tk.X, padx=10, pady=8)

        self.mongo_status_label = tk.Label(db_status_frame, text="● MongoDB: Łączenie...",
                                           bg=COLORS["bg_medium"], fg=COLORS["warning"],
                                           font=("Segoe UI", 9))
        self.mongo_status_label.pack(anchor=tk.W, padx=5, pady=2)

        self.redis_status_label = tk.Label(db_status_frame, text="● Redis: Łączenie...",
                                           bg=COLORS["bg_medium"], fg=COLORS["warning"],
                                           font=("Segoe UI", 9))
        self.redis_status_label.pack(anchor=tk.W, padx=5, pady=2)

        reconnect_btn = tk.Button(db_status_frame, text="🔄 Połącz ponownie",
                                  command=self.connect_databases,
                                  bg=COLORS["bg_light"], fg=COLORS["text_light"],
                                  font=("Segoe UI", 8),
                                  activebackground=COLORS["accent"],
                                  relief=tk.FLAT, cursor="hand2")
        reconnect_btn.pack(pady=5)

        # Kalendarz
        calendar_frame = tk.LabelFrame(self.options_frame, text="📅 Wybierz datę",
                                       bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                       font=("Segoe UI", 9))
        calendar_frame.pack(fill=tk.X, padx=10, pady=8)

        self.kalendarz = Calendar(calendar_frame, selectmode='day',
                                   year=2024, month=10, day=15,
                                   date_pattern="yyyy-mm-dd",
                                   background=COLORS["bg_light"],
                                   foreground=COLORS["text_light"],
                                   headersbackground=COLORS["accent"],
                                   headersforeground=COLORS["text_light"],
                                   selectbackground=COLORS["accent"],
                                   selectforeground=COLORS["text_light"],
                                   normalbackground=COLORS["bg_medium"],
                                   normalforeground=COLORS["text_light"],
                                   weekendbackground=COLORS["bg_light"],
                                   weekendforeground=COLORS["text_dim"],
                                   font=("Segoe UI", 9))
        self.kalendarz.pack(pady=10, padx=10)

        self.date_button = tk.Button(calendar_frame, text="✓ Wybierz datę",
                                      command=self.wybierz_date,
                                      bg=COLORS["accent"], fg=COLORS["text_light"],
                                      font=("Segoe UI", 9, "bold"),
                                      activebackground=COLORS["accent_hover"],
                                      relief=tk.FLAT, cursor="hand2")
        self.date_button.pack(pady=5)

        self.selected_date_label = tk.Label(calendar_frame, text="Wybrana data: -",
                                             bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                             font=("Segoe UI", 9))
        self.selected_date_label.pack(pady=5)

        # Województwo
        woj_frame = tk.LabelFrame(self.options_frame, text="Województwo",
                                   bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                   font=("Segoe UI", 9))
        woj_frame.pack(fill=tk.X, padx=10, pady=5)

        self.wojewodztwo_dropdown = ttk.Combobox(woj_frame,
                                                   textvariable=self.selected_wojewodztwo,
                                                   state="readonly", width=25,
                                                   style="Custom.TCombobox")
        self.wojewodztwo_dropdown.pack(pady=5, padx=5)
        self.wojewodztwo_dropdown.bind("<<ComboboxSelected>>", self.on_wojewodztwo_selected)

        self.licz_woj_button = tk.Button(woj_frame, text="Oblicz dla województwa",
                                          command=self.licz_wojewodztwo,
                                          bg=COLORS["accent"], fg=COLORS["text_light"],
                                          font=("Segoe UI", 9, "bold"),
                                          activebackground=COLORS["accent_hover"],
                                          relief=tk.FLAT, cursor="hand2")
        self.licz_woj_button.pack(pady=5)

        # Powiat
        powiat_frame = tk.LabelFrame(self.options_frame, text="Powiat",
                                       bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                       font=("Segoe UI", 9))
        powiat_frame.pack(fill=tk.X, padx=10, pady=5)

        self.powiat_dropdown = ttk.Combobox(powiat_frame,
                                             textvariable=self.selected_powiat,
                                             state="readonly", width=25,
                                             style="Custom.TCombobox")
        self.powiat_dropdown.pack(pady=5, padx=5)
        self.powiat_dropdown.bind("<<ComboboxSelected>>", self.on_powiat_selected)

        self.licz_powiat_button = tk.Button(powiat_frame, text="Oblicz dla powiatu",
                                             command=self.licz_powiat,
                                             bg=COLORS["accent"], fg=COLORS["text_light"],
                                             font=("Segoe UI", 9, "bold"),
                                             activebackground=COLORS["accent_hover"],
                                             relief=tk.FLAT, cursor="hand2")
        self.licz_powiat_button.pack(pady=5)

        # Przyciski dodatkowe
        extra_frame = tk.Frame(self.options_frame, bg=COLORS["bg_medium"])
        extra_frame.pack(fill=tk.X, padx=10, pady=10)


        self.cache_clear_button = tk.Button(extra_frame, text="Wyczyść cache",
                                             command=self.clear_cache,
                                             bg=COLORS["accent"], fg=COLORS["text_light"],
                                             font=("Segoe UI", 9, "bold"),
                                             activebackground=COLORS["accent_hover"],
                                             relief=tk.FLAT, cursor="hand2")
        self.cache_clear_button.pack(pady=5, fill=tk.X)

    def create_results_panel(self):
        """Tworzy panel wyników"""
        # Ikony dzień/noc
        self.days_night_frame = tk.Frame(self.results_frame, bg=COLORS["bg_medium"])
        self.days_night_frame.pack(fill=tk.X, pady=5)

        for _ in range(3):
            if self.images.get("moon"):
                moon_label = tk.Label(self.days_night_frame, image=self.images["moon"],
                                      bg=COLORS["bg_medium"])
                moon_label.pack(side=tk.LEFT, pady=10, padx=60, expand=True)
            if self.images.get("sun"):
                sun_label = tk.Label(self.days_night_frame, image=self.images["sun"],
                                     bg=COLORS["bg_medium"])
                sun_label.pack(side=tk.LEFT, pady=10, padx=60, expand=True)

        # Kontener na 3 kolumny wyników
        results_container = tk.Frame(self.results_frame, bg=COLORS["bg_medium"])
        results_container.pack(expand=True, fill=tk.BOTH, padx=5)

        # Kolumna temperatura
        self.temp_frame = tk.LabelFrame(results_container, text="🌡️ Temperatura",
                                        bg=COLORS["frame_bg"], fg=COLORS["temp_color"],
                                        font=("Segoe UI", 10, "bold"))
        self.temp_frame.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.BOTH)
        self.create_temperature_section()

        # Kolumna opad
        self.opad_frame = tk.LabelFrame(results_container, text="🌧️ Opad",
                                        bg=COLORS["frame_bg"], fg=COLORS["rain_color"],
                                        font=("Segoe UI", 10, "bold"))
        self.opad_frame.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.BOTH)
        self.create_opad_section()

        # Kolumna wiatr
        self.wiatr_frame = tk.LabelFrame(results_container, text="💨 Wiatr",
                                         bg=COLORS["frame_bg"], fg=COLORS["wind_color"],
                                         font=("Segoe UI", 10, "bold"))
        self.wiatr_frame.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.BOTH)
        self.create_wind_section()

    def create_stat_row(self, parent, label_type="srednia"):
        """Tworzy wiersz ze statystykami (noc | typ | dzień)"""
        frame = tk.Frame(parent, bg=COLORS["frame_bg"])
        frame.pack(fill=tk.X)

        night_label = tk.Label(frame, text="-", width=10,
                               bg=COLORS["frame_bg"], fg=COLORS["text_light"],
                               font=("Segoe UI", 10, "bold"))
        night_label.pack(side=tk.LEFT, pady=5, padx=5, expand=True)

        sep_label = tk.Label(frame, text="śr." if label_type == "srednia" else "med.", width=5,
                             bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                             font=("Segoe UI", 9))
        sep_label.pack(side=tk.LEFT, pady=5, padx=5)

        day_label = tk.Label(frame, text="-", width=10,
                             bg=COLORS["frame_bg"], fg=COLORS["text_light"],
                             font=("Segoe UI", 10, "bold"))
        day_label.pack(side=tk.LEFT, pady=5, padx=5, expand=True)

        return night_label, day_label

    def create_temperature_section(self):
        """Tworzy sekcję temperatury"""
        # Ikona
        if self.images.get("thermometer"):
            icon_frame = tk.Frame(self.temp_frame, bg=COLORS["frame_bg"])
            icon_frame.pack(fill=tk.X)
            tk.Label(icon_frame, image=self.images["thermometer"],
                     bg=COLORS["frame_bg"]).pack(pady=5)

        # Temperatura powietrza
        t_pow_frame = tk.LabelFrame(self.temp_frame, text="Powietrza [°C]",
                                    bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                    font=("Segoe UI", 9))
        t_pow_frame.pack(fill=tk.X, padx=5, pady=5)

        self.t_pow_night_sr, self.t_pow_day_sr = self.create_stat_row(t_pow_frame, "srednia")
        self.t_pow_night_med, self.t_pow_day_med = self.create_stat_row(t_pow_frame, "mediana")

        # Temperatura gruntu
        t_grunt_frame = tk.LabelFrame(self.temp_frame, text="Gruntu [°C]",
                                      bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                      font=("Segoe UI", 9))
        t_grunt_frame.pack(fill=tk.X, padx=5, pady=5)

        self.t_grunt_night_sr, self.t_grunt_day_sr = self.create_stat_row(t_grunt_frame, "srednia")
        self.t_grunt_night_med, self.t_grunt_day_med = self.create_stat_row(t_grunt_frame, "mediana")

        # Wilgotność
        wilg_frame = tk.LabelFrame(self.temp_frame, text="Wilgotność [%]",
                                   bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                   font=("Segoe UI", 9))
        wilg_frame.pack(fill=tk.X, padx=5, pady=5)

        self.wilg_night_sr, self.wilg_day_sr = self.create_stat_row(wilg_frame, "srednia")
        self.wilg_night_med, self.wilg_day_med = self.create_stat_row(wilg_frame, "mediana")

    def create_opad_section(self):
        """Tworzy sekcję opadu"""
        # Ikona
        if self.images.get("rain"):
            icon_frame = tk.Frame(self.opad_frame, bg=COLORS["frame_bg"])
            icon_frame.pack(fill=tk.X)
            tk.Label(icon_frame, image=self.images["rain"],
                     bg=COLORS["frame_bg"]).pack(pady=5)

        # Opad dobowy
        opad_dob_frame = tk.LabelFrame(self.opad_frame, text="Dobowy [mm]",
                                       bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                       font=("Segoe UI", 9))
        opad_dob_frame.pack(fill=tk.X, padx=5, pady=5)

        self.opad_dobowy_label = tk.Label(opad_dob_frame, text="-",
                                          bg=COLORS["frame_bg"], fg=COLORS["rain_color"],
                                          font=("Segoe UI", 14, "bold"))
        self.opad_dobowy_label.pack(pady=10)

        # Opad godzinowy
        opad_godz_frame = tk.LabelFrame(self.opad_frame, text="Godzinowy [mm]",
                                        bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                        font=("Segoe UI", 9))
        opad_godz_frame.pack(fill=tk.X, padx=5, pady=5)

        self.opad_godz_night_sr, self.opad_godz_day_sr = self.create_stat_row(opad_godz_frame, "srednia")
        self.opad_godz_night_med, self.opad_godz_day_med = self.create_stat_row(opad_godz_frame, "mediana")

        # Opad 10-minutowy
        opad_10_frame = tk.LabelFrame(self.opad_frame, text="10-min [mm]",
                                      bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                      font=("Segoe UI", 9))
        opad_10_frame.pack(fill=tk.X, padx=5, pady=5)

        self.opad_10_night_sr, self.opad_10_day_sr = self.create_stat_row(opad_10_frame, "srednia")
        self.opad_10_night_med, self.opad_10_day_med = self.create_stat_row(opad_10_frame, "mediana")

    def create_wind_section(self):
        """Tworzy sekcję wiatru"""
        # Ikona
        if self.images.get("wind"):
            icon_frame = tk.Frame(self.wiatr_frame, bg=COLORS["frame_bg"])
            icon_frame.pack(fill=tk.X)
            tk.Label(icon_frame, image=self.images["wind"],
                     bg=COLORS["frame_bg"]).pack(pady=5)

        # Prędkość wiatru
        pred_frame = tk.LabelFrame(self.wiatr_frame, text="Prędkość wiatru",
                                   bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                   font=("Segoe UI", 9))
        pred_frame.pack(fill=tk.X, padx=5, pady=5)

        self.pred_wiatr_night_sr, self.pred_wiatr_day_sr = self.create_stat_row(pred_frame, "srednia")
        self.pred_wiatr_night_med, self.pred_wiatr_day_med = self.create_stat_row(pred_frame, "mediana")

        # Kierunek wiatru
        kier_frame = tk.LabelFrame(self.wiatr_frame, text="Kierunek wiatru",
                                   bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                   font=("Segoe UI", 9))
        kier_frame.pack(fill=tk.X, padx=5, pady=5)

        self.kier_wiatr_night_sr, self.kier_wiatr_day_sr = self.create_stat_row(kier_frame, "srednia")
        self.kier_wiatr_night_med, self.kier_wiatr_day_med = self.create_stat_row(kier_frame, "mediana")

        # Maks prędkość
        maks_frame = tk.LabelFrame(self.wiatr_frame, text="Maks. prędkość",
                                   bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                   font=("Segoe UI", 9))
        maks_frame.pack(fill=tk.X, padx=5, pady=5)

        self.maks_pred_night_sr, self.maks_pred_day_sr = self.create_stat_row(maks_frame, "srednia")
        self.maks_pred_night_med, self.maks_pred_day_med = self.create_stat_row(maks_frame, "mediana")

        # Największy poryw
        poryw_frame = tk.LabelFrame(self.wiatr_frame, text="Największy poryw",
                                    bg=COLORS["frame_bg"], fg=COLORS["text_dim"],
                                    font=("Segoe UI", 9))
        poryw_frame.pack(fill=tk.X, padx=5, pady=5)

        self.poryw_label = tk.Label(poryw_frame, text="-",
                                    bg=COLORS["frame_bg"], fg=COLORS["wind_color"],
                                    font=("Segoe UI", 14, "bold"))
        self.poryw_label.pack(pady=10)

    def create_status_bar(self):
        """Tworzy pasek statusu"""
        self.status_bar = tk.Label(self.root, text="Gotowy", bd=1, relief=tk.SUNKEN, anchor=tk.W,
                                   bg=COLORS["bg_medium"], fg=COLORS["text_light"],
                                   font=("Segoe UI", 9))
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def set_status(self, message):
        """Ustawia status"""
        self.status_bar.config(text=message)
        self.root.update_idletasks()

    def connect_databases(self):
        """Łączy się z bazami danych"""
        def connect_thread():
            mongo_ok, redis_ok = db.connect_all()

            self.root.after(0, lambda: self.update_db_status(mongo_ok, redis_ok))

        threading.Thread(target=connect_thread, daemon=True).start()

    def update_db_status(self, mongo_ok, redis_ok):
        """Aktualizuje status połączeń"""
        if mongo_ok:
            self.mongo_status_label.config(text="MongoDB: Połączono ✓", fg="green")
        else:
            self.mongo_status_label.config(text="MongoDB: Brak połączenia ✗", fg="red")

        if redis_ok:
            self.redis_status_label.config(text="Redis: Połączono ✓", fg="green")
        else:
            self.redis_status_label.config(text="Redis: Brak połączenia ✗", fg="red")

    def load_admin_data(self):
        """Ładuje dane administracyjne"""
        def load_thread():
            try:
                # Próba pobrania z cache
                cached_woj = db.get_cached_admin_list("wojewodztwa")
                cached_pow = db.get_cached_admin_list("powiaty")

                if cached_woj and cached_pow:
                    self.wojewodztwa = cached_woj
                    self.powiaty = cached_pow
                else:
                    # Ładowanie z plików shapefile
                    self.load_from_shapefiles()

                self.root.after(0, self.update_dropdowns)

            except Exception as e:
                print(f"[ERROR] Błąd ładowania danych: {e}")
                self.root.after(0, lambda: self.set_status(f"Błąd ładowania danych: {e}"))

        threading.Thread(target=load_thread, daemon=True).start()

    def load_from_shapefiles(self):
        """Ładuje dane z plików shapefile"""
        try:
            if os.path.exists(ADMIN_VOIV_PATH):
                self.voiv_gdf = gpd.read_file(ADMIN_VOIV_PATH)
                # Próba znalezienia kolumny z nazwą
                name_cols = ['nazwa', 'name', 'NAME', 'JPT_NAZWA_', 'jpt_nazwa_']
                for col in name_cols:
                    if col in self.voiv_gdf.columns:
                        self.wojewodztwa = sorted(self.voiv_gdf[col].dropna().unique().tolist())
                        break

                if not self.wojewodztwa and len(self.voiv_gdf.columns) > 1:
                    # Użyj pierwszej kolumny tekstowej
                    for col in self.voiv_gdf.columns:
                        if self.voiv_gdf[col].dtype == 'object':
                            self.wojewodztwa = sorted(self.voiv_gdf[col].dropna().unique().tolist())
                            break

                # Cache
                db.cache_admin_list("wojewodztwa", self.wojewodztwa)

            if os.path.exists(ADMIN_COUNTY_PATH):
                self.county_gdf = gpd.read_file(ADMIN_COUNTY_PATH)
                name_cols = ['nazwa', 'name', 'NAME', 'JPT_NAZWA_', 'jpt_nazwa_']
                for col in name_cols:
                    if col in self.county_gdf.columns:
                        self.powiaty = sorted(self.county_gdf[col].dropna().unique().tolist())
                        break

                if not self.powiaty and len(self.county_gdf.columns) > 1:
                    for col in self.county_gdf.columns:
                        if self.county_gdf[col].dtype == 'object':
                            self.powiaty = sorted(self.county_gdf[col].dropna().unique().tolist())
                            break

                db.cache_admin_list("powiaty", self.powiaty)

            if os.path.exists(EFFACILITY_PATH):
                self.eff_gdf = gpd.read_file(EFFACILITY_PATH)

        except Exception as e:
            print(f"[ERROR] Błąd ładowania shapefiles: {e}")
            # Ustaw przykładowe dane
            self.wojewodztwa = ["dolnośląskie", "kujawsko-pomorskie", "lubelskie",
                               "lubuskie", "łódzkie", "małopolskie", "mazowieckie",
                               "opolskie", "podkarpackie", "podlaskie", "pomorskie",
                               "śląskie", "świętokrzyskie", "warmińsko-mazurskie",
                               "wielkopolskie", "zachodniopomorskie"]
            self.powiaty = ["Przykładowy powiat 1", "Przykładowy powiat 2"]

    def update_dropdowns(self):
        """Aktualizuje listy rozwijane"""
        self.wojewodztwo_dropdown['values'] = self.wojewodztwa
        self.powiat_dropdown['values'] = self.powiaty
        self.set_status("Dane załadowane")

    def wybierz_date(self):
        """Obsługuje wybór daty"""
        selected = self.kalendarz.get_date()
        self.selected_date.set(selected)
        self.selected_date_label.config(text=f"Wybrana data: {selected}")
        db.increment_query_counter("date_selection")

    def on_wojewodztwo_selected(self, event=None):
        """Obsługuje wybór województwa"""
        woj = self.selected_wojewodztwo.get()
        self.set_status(f"Wybrano województwo: {woj}")
        db.increment_query_counter("wojewodztwo_selection")

    def on_powiat_selected(self, event=None):
        """Obsługuje wybór powiatu"""
        powiat = self.selected_powiat.get()
        self.set_status(f"Wybrano powiat: {powiat}")
        db.increment_query_counter("powiat_selection")

    def licz_wojewodztwo(self):
        """Oblicza statystyki dla województwa"""
        woj = self.selected_wojewodztwo.get()
        date = self.selected_date.get()

        if woj == "Wybierz":
            messagebox.showwarning("Uwaga", "Wybierz województwo")
            return

        if not date:
            messagebox.showwarning("Uwaga", "Wybierz datę")
            return

        self.set_status(f"Obliczanie dla województwa: {woj}...")

        def calc_thread():
            try:
                # Sprawdź cache
                cached = db.get_cached_meteo_stats(woj, date, "all", "wojewodztwo")
                if cached:
                    self.root.after(0, lambda: self.display_results(cached))
                    self.root.after(0, lambda: self.set_status("Wyniki z cache"))
                    return

                # Oblicz nowe statystyki
                results = self.calculate_statistics(woj, date, "wojewodztwo")

                # Cache wyniki
                db.cache_meteo_stats(woj, date, "all", results, "wojewodztwo")

                # Zapisz do MongoDB
                db.save_statistics_mongo(woj, "wojewodztwo", date, "all", results)

                self.root.after(0, lambda: self.display_results(results))
                self.root.after(0, lambda: self.set_status("Obliczenia zakończone"))

            except Exception as ex:
                error_msg = str(ex)
                print(f"[ERROR] licz_wojewodztwo: {error_msg}")
                self.root.after(0, lambda msg=error_msg: self.set_status(f"Błąd: {msg}"))
                self.root.after(0, lambda msg=error_msg: messagebox.showerror("Błąd", msg))

        threading.Thread(target=calc_thread, daemon=True).start()

    def licz_powiat(self):
        """Oblicza statystyki dla powiatu"""
        powiat = self.selected_powiat.get()
        date = self.selected_date.get()

        if powiat == "Wybierz":
            messagebox.showwarning("Uwaga", "Wybierz powiat")
            return

        if not date:
            messagebox.showwarning("Uwaga", "Wybierz datę")
            return

        self.set_status(f"Obliczanie dla powiatu: {powiat}...")

        def calc_thread():
            try:
                # Sprawdź cache
                cached = db.get_cached_meteo_stats(powiat, date, "all", "powiat")
                if cached:
                    self.root.after(0, lambda: self.display_results(cached))
                    self.root.after(0, lambda: self.set_status("Wyniki z cache"))
                    return

                # Oblicz nowe statystyki
                results = self.calculate_statistics(powiat, date, "powiat")

                # Cache wyniki
                db.cache_meteo_stats(powiat, date, "all", results, "powiat")

                # Zapisz do MongoDB
                db.save_statistics_mongo(powiat, "powiat", date, "all", results)

                self.root.after(0, lambda: self.display_results(results))
                self.root.after(0, lambda: self.set_status("Obliczenia zakończone"))

            except Exception as ex:
                error_msg = str(ex)
                print(f"[ERROR] licz_powiat: {error_msg}")
                self.root.after(0, lambda msg=error_msg: self.set_status(f"Błąd: {msg}"))
                self.root.after(0, lambda msg=error_msg: messagebox.showerror("Błąd", msg))

        threading.Thread(target=calc_thread, daemon=True).start()

    def calculate_statistics(self, admin_id, date, admin_type):
        """Oblicza statystyki dla danych parametrów pobierając dane z Redis"""
        import numpy as np
        from datetime import datetime, timedelta
        from astral import LocationInfo
        from astral.sun import sun

        results = {
            "temp_powietrza": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "temp_gruntu": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "wilgotnosc": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "opad_dobowy": None,
            "opad_godzinowy": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "opad_10min": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "predkosc_wiatru": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "kierunek_wiatru": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "maks_predkosc": {"dzien": {"mean": None, "median": None}, "noc": {"mean": None, "median": None}},
            "poryw": None
        }

        # Mapowanie kodów parametrów na klucze wyników
        param_mapping = {
            "B00300S": "temp_powietrza",
            "B00305A": "temp_gruntu",
            "B00802A": "wilgotnosc",
            "B00604S": "opad_dobowy",
            "B00606S": "opad_godzinowy",
            "B00608S": "opad_10min",
            "B00702A": "predkosc_wiatru",
            "B00202A": "kierunek_wiatru",
            "B00703A": "maks_predkosc",
            "B00714A": "poryw"
        }

        if db.mongo_db is None or db.redis_client is None:
            print("[ERROR] Brak połączenia z bazami danych")
            return results

        # Pobierz stacje dla danej jednostki administracyjnej
        stations_collection = db.mongo_db["stations"]

        # Najpierw pobierz wszystkie stacje z Redis które mają dane
        all_meteo_keys = db.redis_client.keys("meteo:*:B00300S")
        redis_station_ids = set([k.split(":")[1] for k in all_meteo_keys])
        print(f"[INFO] Stacje z danymi w Redis: {len(redis_station_ids)}")

        # Znajdź stacje które są w MongoDB z tym województwem/powiatem i mają dane w Redis
        stations_with_data = []
        for redis_sid in redis_station_ids:
            station = stations_collection.find_one({"station_id": redis_sid})
            if station:
                woj = station.get("wojewodztwo", "")
                pow_name = station.get("powiat", "")
                if admin_type == "powiat" and pow_name:
                    if admin_id.lower() in pow_name.lower():
                        stations_with_data.append(redis_sid)
                elif admin_type == "wojewodztwo" and woj:
                    if admin_id.lower() in woj.lower():
                        stations_with_data.append(redis_sid)


        if not stations_with_data:
            print(f"[WARNING] Brak stacji z danymi dla {admin_id}")
            return results

        print(f"[INFO] Stacje z danymi w Redis dla {admin_id}: {len(stations_with_data)}")

        # Parsuj datę
        try:
            selected_date = datetime.strptime(date, "%Y-%m-%d")
        except:
            print(f"[ERROR] Nieprawidłowy format daty: {date}")
            return results

        # Oblicz wschód i zachód słońca (przybliżenie dla Polski - centrum)
        try:
            location = LocationInfo("Poland", "Poland", "Europe/Warsaw", 52.0, 19.0)
            s = sun(location.observer, date=selected_date)
            sunrise = s["sunrise"].replace(tzinfo=None)
            sunset = s["sunset"].replace(tzinfo=None)
        except:
            # Domyślne wartości
            sunrise = selected_date.replace(hour=6, minute=0)
            sunset = selected_date.replace(hour=18, minute=0)

        # Zakres czasowy dla dnia (timestamp w ms)
        day_start = int(selected_date.timestamp() * 1000)
        day_end = int((selected_date + timedelta(days=1)).timestamp() * 1000)

        print(f"[INFO] Zakres dat: {selected_date.date()} ({day_start} - {day_end})")

        # Dla każdego parametru
        for param_code, result_key in param_mapping.items():
            day_values = []
            night_values = []
            all_values = []

            for station_id in stations_with_data:
                key = f"meteo:{station_id}:{param_code}"

                try:
                    # Pobierz dane z Redis dla danego dnia
                    data = db.redis_client.zrangebyscore(key, day_start, day_end, withscores=True)

                    for item, score in data:
                        try:
                            # Format: "timestamp:value"
                            parts = item.split(":")
                            if len(parts) >= 2:
                                value = float(parts[1])
                                ts = datetime.fromtimestamp(score / 1000)

                                all_values.append(value)

                                # Podział na dzień/noc
                                if sunrise <= ts <= sunset:
                                    day_values.append(value)
                                else:
                                    night_values.append(value)
                        except:
                            continue
                except Exception as e:
                    continue

            # Oblicz statystyki
            if result_key in ["opad_dobowy", "poryw"]:
                # Dla tych parametrów zwracamy tylko jedną wartość
                if all_values:
                    if result_key == "opad_dobowy":
                        results[result_key] = sum(all_values) / len(stations_with_data) if stations_with_data else 0
                    else:
                        results[result_key] = max(all_values)
            else:
                # Dla pozostałych - dzień/noc ze średnią i medianą
                if day_values:
                    results[result_key]["dzien"]["mean"] = float(np.mean(day_values))
                    results[result_key]["dzien"]["median"] = float(np.median(day_values))
                if night_values:
                    results[result_key]["noc"]["mean"] = float(np.mean(night_values))
                    results[result_key]["noc"]["median"] = float(np.median(night_values))

        print(f"[INFO] Obliczenia zakończone, temp_powietrza dzień: {results['temp_powietrza']['dzien']}")
        return results

    def display_results(self, results):
        """Wyświetla wyniki"""
        def format_val(val):
            if val is None:
                return "-"
            return f"{val:.1f}"

        # Temperatura powietrza
        if "temp_powietrza" in results:
            tp = results["temp_powietrza"]
            self.t_pow_night_sr.config(text=format_val(tp.get("noc", {}).get("mean")))
            self.t_pow_day_sr.config(text=format_val(tp.get("dzien", {}).get("mean")))
            self.t_pow_night_med.config(text=format_val(tp.get("noc", {}).get("median")))
            self.t_pow_day_med.config(text=format_val(tp.get("dzien", {}).get("median")))

        # Temperatura gruntu
        if "temp_gruntu" in results:
            tg = results["temp_gruntu"]
            self.t_grunt_night_sr.config(text=format_val(tg.get("noc", {}).get("mean")))
            self.t_grunt_day_sr.config(text=format_val(tg.get("dzien", {}).get("mean")))
            self.t_grunt_night_med.config(text=format_val(tg.get("noc", {}).get("median")))
            self.t_grunt_day_med.config(text=format_val(tg.get("dzien", {}).get("median")))

        # Wilgotność
        if "wilgotnosc" in results:
            w = results["wilgotnosc"]
            self.wilg_night_sr.config(text=format_val(w.get("noc", {}).get("mean")))
            self.wilg_day_sr.config(text=format_val(w.get("dzien", {}).get("mean")))
            self.wilg_night_med.config(text=format_val(w.get("noc", {}).get("median")))
            self.wilg_day_med.config(text=format_val(w.get("dzien", {}).get("median")))

        # Opad dobowy
        if "opad_dobowy" in results:
            self.opad_dobowy_label.config(text=format_val(results["opad_dobowy"]))

        # Opad godzinowy
        if "opad_godzinowy" in results:
            og = results["opad_godzinowy"]
            self.opad_godz_night_sr.config(text=format_val(og.get("noc", {}).get("mean")))
            self.opad_godz_day_sr.config(text=format_val(og.get("dzien", {}).get("mean")))
            self.opad_godz_night_med.config(text=format_val(og.get("noc", {}).get("median")))
            self.opad_godz_day_med.config(text=format_val(og.get("dzien", {}).get("median")))

        # Opad 10-min
        if "opad_10min" in results:
            o10 = results["opad_10min"]
            self.opad_10_night_sr.config(text=format_val(o10.get("noc", {}).get("mean")))
            self.opad_10_day_sr.config(text=format_val(o10.get("dzien", {}).get("mean")))
            self.opad_10_night_med.config(text=format_val(o10.get("noc", {}).get("median")))
            self.opad_10_day_med.config(text=format_val(o10.get("dzien", {}).get("median")))

        # Prędkość wiatru
        if "predkosc_wiatru" in results:
            pw = results["predkosc_wiatru"]
            self.pred_wiatr_night_sr.config(text=format_val(pw.get("noc", {}).get("mean")))
            self.pred_wiatr_day_sr.config(text=format_val(pw.get("dzien", {}).get("mean")))
            self.pred_wiatr_night_med.config(text=format_val(pw.get("noc", {}).get("median")))
            self.pred_wiatr_day_med.config(text=format_val(pw.get("dzien", {}).get("median")))

        # Kierunek wiatru
        if "kierunek_wiatru" in results:
            kw = results["kierunek_wiatru"]
            self.kier_wiatr_night_sr.config(text=format_val(kw.get("noc", {}).get("mean")))
            self.kier_wiatr_day_sr.config(text=format_val(kw.get("dzien", {}).get("mean")))
            self.kier_wiatr_night_med.config(text=format_val(kw.get("noc", {}).get("median")))
            self.kier_wiatr_day_med.config(text=format_val(kw.get("dzien", {}).get("median")))

        # Maks prędkość
        if "maks_predkosc" in results:
            mp = results["maks_predkosc"]
            self.maks_pred_night_sr.config(text=format_val(mp.get("noc", {}).get("mean")))
            self.maks_pred_day_sr.config(text=format_val(mp.get("dzien", {}).get("mean")))
            self.maks_pred_night_med.config(text=format_val(mp.get("noc", {}).get("median")))
            self.maks_pred_day_med.config(text=format_val(mp.get("dzien", {}).get("median")))

        # Poryw
        if "poryw" in results:
            self.poryw_label.config(text=format_val(results["poryw"]))


    def clear_cache(self):
        """Czyści cache wyników (nie dane meteorologiczne!)"""
        try:
            if db.redis_client is not None:
                # Usuń tylko klucze cache (meteo_stats, admin_list), NIE dane pomiarowe (meteo:*)
                cache_keys = db.redis_client.keys("meteo_stats:*")
                cache_keys += db.redis_client.keys("admin_list:*")
                cache_keys += db.redis_client.keys("query_counter:*")

                if cache_keys:
                    db.redis_client.delete(*cache_keys)
                    messagebox.showinfo("Sukces", f"Wyczyszczono {len(cache_keys)} kluczy cache")
                else:
                    messagebox.showinfo("Info", "Cache jest pusty")
                self.set_status("Cache wyczyszczony")
            else:
                messagebox.showwarning("Uwaga", "Brak połączenia z Redis")
        except Exception as e:
            messagebox.showerror("Błąd", f"Nie można wyczyścić cache: {e}")


def main():
    """Główna funkcja uruchamiająca aplikację"""
    root = tk.Tk()
    app = MeteoApp(root)

    # Zamknij połączenia przy zamykaniu aplikacji
    def on_closing():
        db.close_connections()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()

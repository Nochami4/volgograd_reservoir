
# -*- coding: utf-8 -*-
"""
Прогноз отступания берегов с добавлением погодных признаков из Excel-файла
(в том числе скорости и направления ветра).

Что делает скрипт:
1) Читает все xlsx-файлы профилей в папке results_models/profiles.
2) Сводит погодный файл "Камышин скорость и направление ветра.xlsx" по годам:
   - скорость ветра,
   - направление ветра,
   - температура, давление, влажность, осадки, видимость и др. числовые поля,
   - все числовые признаки агрегируются в годовые статистики.
3) Объединяет профили по году с погодой.
4) Строит прогноз отступания берегов на основе:
   - лагов по ИПН,
   - номера года,
   - годовых погодных признаков (через PCA, чтобы использовать всю погодную информацию и не переобучаться).
5) Сохраняет таблицы и графики в results_models/profile_analysis.

Важно:
- Для будущих лет погодные признаки неизвестны, поэтому в прогнозе используется сценарий
  "последнее доступное погодное состояние" (carry-forward) — это стандартная и прозрачная
  допущенная замена для экзогенных признаков.
- Если в каких-то годах в погодном файле отсутствуют отдельные поля, они будут аккуратно
  заполнены интерполяцией/forward-fill/back-fill по годам.
"""

from __future__ import annotations

import re
import warnings
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
sns.set(style="whitegrid")

# =========================
# ПУТИ И НАСТРОЙКИ
# =========================
PROFILES_DIR = Path("results_models/profiles")
WEATHER_FILE = Path("Камышин скорость и направление ветра.xlsx")  # при необходимости поменяйте путь

OUT_DIR = Path("results_models/profile_analysis")
PLOTS_DIR = OUT_DIR / "plots"
PROFILE_PLOTS_DIR = PLOTS_DIR / "profiles"
SITE_PLOTS_DIR = PLOTS_DIR / "sites"
CORR_PLOTS_DIR = PLOTS_DIR / "corr"
CORR_MATRICES_DIR = OUT_DIR / "corr_matrices"

for p in [PROFILE_PLOTS_DIR, SITE_PLOTS_DIR, CORR_PLOTS_DIR, CORR_MATRICES_DIR]:
    p.mkdir(parents=True, exist_ok=True)

assert PROFILES_DIR.exists(), f"Папка с профилями не найдена: {PROFILES_DIR}"
assert WEATHER_FILE.exists(), f"Погодный файл не найден: {WEATHER_FILE}"

print("Reading profile files from:", PROFILES_DIR.resolve())
print("Reading weather file:", WEATHER_FILE.resolve())
print("Outputs will be saved to:", OUT_DIR.resolve())


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================
def safe_str(x) -> str:
    return "" if pd.isna(x) else str(x).strip()


def safe_filename(s: str) -> str:
    return re.sub(r"[^\w\d\-]+", "_", str(s), flags=re.UNICODE)[:160]


def clean_num_series(s: pd.Series) -> pd.Series:
    """Преобразует серию в числа, аккуратно убирая пробелы, запятые, лишние символы."""
    return pd.to_numeric(
        s.astype(str)
         .str.replace(r"\s+", "", regex=True)
         .str.replace("*", "", regex=False)
         .str.replace(",", ".", regex=False)
         .str.replace("−", "-", regex=False)
         .str.replace(r"[^0-9\.\-]", "", regex=True),
        errors="coerce"
    )


def extract_first_number(x):
    """Достаёт первое число из значения. Для дат '1900-01-13' возвращает 13."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (pd.Timestamp, datetime)):
        # Excel иногда хранит обычное число как дату вида 1900-01-13
        if x.year == 1900 and x.month == 1:
            return float(x.day)
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", ".").replace("−", "-")
    if s in ("", "nan", "None"):
        return np.nan
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else np.nan


def parse_year_from_sheet_name(name: str) -> int | None:
    m = re.match(r"^(\d{4})$", str(name).strip())
    return int(m.group(1)) if m else None


def parse_date_with_year(val, year: int):
    """Парсит дату из ячейки. Поддерживает datetime, строку и формат вида 1.03 -> 1 марта."""
    if pd.isna(val):
        return pd.NaT
    if isinstance(val, pd.Timestamp):
        return val
    if isinstance(val, datetime):
        return pd.Timestamp(val)
    s = str(val).strip().replace(",", ".")
    # Полноценная дата
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if not pd.isna(dt):
        # Если дата распарсилась адекватно, используем её
        return dt
    # Формат "1.03" = 1 марта
    m = re.match(r"^(\d{1,2})\.(\d{1,2})$", s)
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        try:
            return pd.Timestamp(year=year, month=mo, day=d)
        except Exception:
            return pd.NaT
    return pd.NaT


def detect_header_row(raw: pd.DataFrame) -> int:
    """Ищет строку, где начинается шапка таблицы."""
    for i in range(min(6, len(raw))):
        row = " ".join(str(x) for x in raw.iloc[i].tolist() if pd.notna(x))
        if "Ветер" in row or "Время" in row:
            return i
    return 0


def build_headers(raw: pd.DataFrame):
    """
    Создаёт заголовки по двум строкам шапки:
    - main row: верхний уровень,
    - sub row: нижний уровень,
    с аккуратным протягиванием названия группы по горизонтали.
    """
    hr = detect_header_row(raw)
    main_raw = raw.iloc[hr].tolist()
    sub_raw = raw.iloc[hr + 1].tolist()

    main_labels = []
    sub_labels = []
    current_main = None
    current_sub = None

    for m, s in zip(main_raw, sub_raw):
        if pd.notna(m) and str(m).strip() != "":
            current_main = str(m).strip()
            current_sub = None
        if pd.notna(s) and str(s).strip() != "":
            current_sub = str(s).strip()

        main_labels.append(current_main if current_main else f"col_{len(main_labels)}")
        sub_labels.append(current_sub if current_sub else "")

    return hr, main_labels, sub_labels


def infer_site_profile_from_filename(p: Path):
    """Из имени файла пытается извлечь site и profile."""
    name = p.stem
    parts = name.split("__")
    if len(parts) >= 2:
        site = parts[0]
        profile = "__".join(parts[1:])
    else:
        m = re.match(r"(.+?)__?ПРОФИЛЬ_?(.+)", name, flags=re.I)
        if m:
            site = m.group(1)
            profile = m.group(2)
        else:
            if "профиль" in name.lower():
                site = name.split("профиль")[0].strip("_- ")
                profile = name
            else:
                site = name
                profile = name
    return safe_str(site), safe_str(profile)


# =========================
# 1) ЗАГРУЗКА ПРОФИЛЬНЫХ ДАННЫХ
# =========================
files = list(PROFILES_DIR.rglob("*.xls*"))
print("Found profile files:", len(files))

rows = []
file_table = []

for f in files:
    try:
        df = pd.read_excel(f, engine=None)
    except Exception as e:
        print("Failed to read", f, ":", e)
        continue

    site, profile = infer_site_profile_from_filename(f)

    col_map = {c: safe_str(c).lower() for c in df.columns}
    date_col = None
    ipn_col = None
    igp_col = None
    rgp_col = None
    gp_col = None
    punkt_col = None

    for c in df.columns:
        low = col_map[c]
        if "дат" in low:
            date_col = c
        if "ипн" in low or "бров" in low:
            ipn_col = c
        if "игп" in low or "измер" in low:
            igp_col = c
        if "ргп" in low:
            rgp_col = c
        if low.strip() in ("гп", "gp"):
            gp_col = c
        if "пункт" in low:
            punkt_col = c

    if date_col is None and df.shape[1] >= 1:
        date_col = df.columns[0]

    for _, r in df.iterrows():
        try:
            date_val = pd.to_datetime(r[date_col], dayfirst=True, errors="coerce") if date_col in df.columns else pd.NaT
        except Exception:
            date_val = pd.NaT

        ipn_v = clean_num_series(pd.Series([r[ipn_col]]))[0] if ipn_col in df.columns else np.nan
        igp_v = clean_num_series(pd.Series([r[igp_col]]))[0] if igp_col in df.columns else np.nan
        rgp_v = clean_num_series(pd.Series([r[rgp_col]]))[0] if rgp_col in df.columns else np.nan
        gp_v = safe_str(r[gp_col]) if gp_col in df.columns else None
        punkt_v = safe_str(r[punkt_col]) if punkt_col in df.columns else None

        # Пропускаем пустые строки
        if (pd.isna(date_val) and pd.isna(ipn_v) and pd.isna(igp_v) and pd.isna(rgp_v)):
            continue

        rows.append({
            "file": str(f),
            "site": site,
            "profile": profile,
            "date_raw": r[date_col] if date_col in df.columns else None,
            "Дата": date_val,
            "Год": int(date_val.year) if not pd.isna(date_val) else np.nan,
            "Пункт": punkt_v,
            "ГП": gp_v,
            "ИПН_м": ipn_v,
            "ИГП_м": igp_v,
            "РГП_ПН_м": rgp_v,
        })

    file_table.append({"file": str(f), "site": site, "profile": profile, "rows": df.shape[0]})

df_all = pd.DataFrame(rows)
print("Combined profile rows:", len(df_all))

pd.DataFrame(file_table).to_excel(OUT_DIR / "files_table.xlsx", index=False)
df_all.to_excel(OUT_DIR / "observations_from_files.xlsx", index=False)


# =========================
# 2) ЧТЕНИЕ И СВОДКА ПОГОДНОГО EXCEL ПО ГОДАМ
# =========================
DIRECTIONS_8 = ["С", "СВ", "В", "ЮВ", "Ю", "ЮЗ", "З", "СЗ"]


def parse_weather_year_sheet(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Читает одну годовую погодную таблицу и строит годовые признаки.
    Возвращает DataFrame с одной строкой на год.
    """
    hr, main_labels, sub_labels = build_headers(raw)

    # Данные начинаются сразу после двух строк шапки
    data = raw.iloc[hr + 2:].copy().reset_index(drop=True)
    data.columns = pd.MultiIndex.from_tuples(list(zip(main_labels, sub_labels)))

    # Берём только строки, где первый столбец похож на время/час
    first_col = data.iloc[:, 0]
    mask = first_col.apply(lambda x: isinstance(x, (int, float, np.integer, np.floating)) and not pd.isna(x))
    data = data.loc[mask].copy().reset_index(drop=True)

    if data.empty:
        return pd.DataFrame()

    level0 = data.columns.get_level_values(0).astype(str)
    level1 = data.columns.get_level_values(1).astype(str)

    # Индексы колонок ветра
    dir_cols = [i for i, (m, s) in enumerate(zip(level0, level1)) if ("Ветер" in m and "направление" in s)]
    spd_cols = [i for i, (m, s) in enumerate(zip(level0, level1)) if ("Ветер" in m and "Скорость" in s)]

    # Набор прочих числовых полей, если они присутствуют в шапке
    numeric_groups = [
        "Видим.", "Т", "Тd", "f", "Тe", "Тes", "P", "Po", "Тmin", "Tmax", "R", "R24", "S"
    ]

    row_records = []
    for _, row in data.iterrows():
        date_val = parse_date_with_year(row.iloc[1] if len(row) > 1 else pd.NaT, year)
        hour_val = extract_first_number(row.iloc[0])

        dirs = [
            str(row.iloc[i]).strip()
            for i in dir_cols
            if pd.notna(row.iloc[i]) and str(row.iloc[i]).strip() not in ("", "nan", "None")
        ]
        spds = [extract_first_number(row.iloc[i]) for i in spd_cols]
        spds = [x for x in spds if pd.notna(x)]

        rec = {
            "Год": year,
            "Дата": date_val,
            "Час": hour_val,
            "wind_dir_row": Counter(dirs).most_common(1)[0][0] if dirs else np.nan,
            "wind_speed_mean_row": float(np.nanmean(spds)) if spds else np.nan,
            "wind_speed_max_row": float(np.nanmax(spds)) if spds else np.nan,
            "wind_speed_min_row": float(np.nanmin(spds)) if spds else np.nan,
            "wind_speed_std_row": float(np.nanstd(spds)) if len(spds) > 1 else np.nan,
        }

        # Числовые параметры
        for field in numeric_groups:
            cols_field = [i for i, (m, s) in enumerate(zip(level0, level1)) if m.startswith(field)]
            vals = [extract_first_number(row.iloc[i]) for i in cols_field]
            vals = [v for v in vals if pd.notna(v)]
            rec[f"{field}_mean_row"] = float(np.nanmean(vals)) if vals else np.nan
            rec[f"{field}_max_row"] = float(np.nanmax(vals)) if vals else np.nan
            rec[f"{field}_min_row"] = float(np.nanmin(vals)) if vals else np.nan

        row_records.append(rec)

    df = pd.DataFrame(row_records)

    # Годовая сводка
    out = {"Год": year, "n_obs": len(df)}

    # Агрегируем все числовые row-поля
    num_cols = [c for c in df.columns if c.endswith("_row") and c != "wind_dir_row"]
    for c in num_cols:
        base = c[:-4]  # убрать "_row"
        out[f"{base}_mean"] = df[c].mean()
        out[f"{base}_std"] = df[c].std()
        out[f"{base}_min"] = df[c].min()
        out[f"{base}_max"] = df[c].max()

    # Доли направлений ветра
    for d in DIRECTIONS_8:
        out[f"wind_dir_share_{d}"] = (df["wind_dir_row"] == d).mean()
    out["wind_dir_share_other"] = (~df["wind_dir_row"].isin(DIRECTIONS_8) & df["wind_dir_row"].notna()).mean()
    out["wind_dir_mode"] = df["wind_dir_row"].mode().iloc[0] if not df["wind_dir_row"].mode().empty else np.nan

    return pd.DataFrame([out])


def load_weather_features(weather_file: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(weather_file)
    yearly_frames = []

    for sh in xls.sheet_names:
        year = parse_year_from_sheet_name(sh)
        if year is None:
            # Лист-агрегат "2011-2020" здесь не используем как годовой ряд,
            # но его можно сохранить отдельно при необходимости.
            continue
        try:
            raw = pd.read_excel(weather_file, sheet_name=sh, header=None)
            yr = parse_weather_year_sheet(raw, year)
            if not yr.empty:
                yearly_frames.append(yr)
                print(f"Weather sheet {sh}: parsed {yr.shape[1]} features")
        except Exception as e:
            print(f"Failed to parse weather sheet {sh}: {e}")

    if not yearly_frames:
        raise RuntimeError("Не удалось извлечь погодные признаки ни из одного годового листа.")

    weather_yearly = pd.concat(yearly_frames, ignore_index=True).sort_values("Год").reset_index(drop=True)

    # Убираем признаки, которые полностью пустые
    all_nan_cols = [c for c in weather_yearly.columns if weather_yearly[c].isna().all()]
    weather_yearly = weather_yearly.drop(columns=all_nan_cols)

    # Заполняем пропуски по времени: сначала интерполяция, затем forward/back fill
    weather_yearly = weather_yearly.sort_values("Год").reset_index(drop=True)
    numeric_cols = [c for c in weather_yearly.columns if c != "Год" and weather_yearly[c].dtype != "object"]
    # Для object-колонок (например, mode направления) отдельно
    object_cols = [c for c in weather_yearly.columns if weather_yearly[c].dtype == "object" and c not in ["Год"]]

    if numeric_cols:
        weather_yearly[numeric_cols] = (
            weather_yearly[numeric_cols]
            .interpolate(method="linear", limit_direction="both")
            .ffill()
            .bfill()
        )

    for c in object_cols:
        weather_yearly[c] = weather_yearly[c].ffill().bfill()

    weather_yearly.to_excel(OUT_DIR / "weather_yearly_features.xlsx", index=False)
    return weather_yearly


weather_yearly = load_weather_features(WEATHER_FILE)
print("Weather yearly feature table shape:", weather_yearly.shape)
display_cols = [c for c in weather_yearly.columns if c.startswith("wind_speed") or c.startswith("wind_dir_share_")][:20]
print(weather_yearly[["Год"] + display_cols].head())


# =========================
# 3) БАЗОВЫЕ ВЫХОДНЫЕ ТАБЛИЦЫ И СВОДКИ ПО ПРОФИЛЯМ
# =========================
df_all.to_csv(OUT_DIR / "observations_combined.csv", index=False)

profile_summary = df_all.groupby(["site", "profile"]).agg(
    n_obs=("ИПН_м", "count"),
    year_first=("Год", "min"),
    year_last=("Год", "max"),
    mean_IPN=("ИПН_м", "mean"),
    std_IPN=("ИПН_м", "std"),
).reset_index()

profile_summary.to_excel(OUT_DIR / "profile_summary.xlsx", index=False)
print("Saved combined CSV and profile summary.")


# =========================
# 4) ФУНКЦИИ ДЛЯ ГРАФИКОВ И МОДЕЛИ
# =========================
def plot_profile_ipn_forecast(dfp, out_png, ycol="ИПН_м", title=None, future_years=None, future_pred=None,
                              model_name="Model", metric_text=""):
    d = dfp.dropna(subset=["Год", ycol]).sort_values("Год").copy()
    if d.shape[0] < 3:
        return False

    plt.figure(figsize=(11, 6))
    plt.plot(d["Год"], d[ycol], "o-", color="black", linewidth=1.8, markersize=4, label="Факт")

    if future_years is not None and future_pred is not None and len(future_years) == len(future_pred):
        plt.plot(future_years, future_pred, "s--", linewidth=2.2, label=f"{model_name} {metric_text}".strip())

    plt.xlabel("Год", fontsize=12)
    plt.ylabel("ИПН, м", fontsize=12)
    if title:
        plt.title(title, fontsize=14, fontweight="bold")
    plt.grid(True, linestyle=":", alpha=0.7)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def save_corr_heatmap(corr_df, out_png, title=None):
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_df, cmap="coolwarm", center=0, annot=True, fmt=".2f", square=True, linewidths=.5)
    if title:
        plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def shorten_profile_name(name, max_len=15):
    name = re.sub(r"ПРОФИЛЬ[_\s]*", "", name, flags=re.I)
    name = re.sub(r"профиль[_\s]*", "", name, flags=re.I)
    if len(name) > max_len:
        name = name[:max_len] + "..."
    return name


def make_profile_year_table(profile_df: pd.DataFrame, weather_yearly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Объединяет профильный ряд с погодными признаками по году.
    Возвращает таблицу с одной строкой на год.
    """
    prof = profile_df.dropna(subset=["Год", "ИПН_м"]).groupby("Год", as_index=False).agg(
        ИПН_м=("ИПН_м", "mean"),
        n_points=("ИПН_м", "count")
    )

    merged = prof.merge(weather_yearly_df, on="Год", how="left").sort_values("Год").reset_index(drop=True)

    # Заполняем погодные признаки, если годовые данные частично отсутствуют
    weather_cols = [c for c in merged.columns if c not in ["Год", "ИПН_м", "n_points"]]
    if weather_cols:
        num_weather_cols = [c for c in weather_cols if merged[c].dtype != "object"]
        obj_weather_cols = [c for c in weather_cols if merged[c].dtype == "object"]

        if num_weather_cols:
            merged[num_weather_cols] = (
                merged[num_weather_cols]
                .interpolate(method="linear", limit_direction="both")
                .ffill()
                .bfill()
            )
        for c in obj_weather_cols:
            merged[c] = merged[c].ffill().bfill()

    return merged


def build_feature_matrix(profile_year_df: pd.DataFrame, pca_components: int = 4):
    """
    Строит матрицу признаков.
    Возвращает:
      X_full  - DataFrame с признаками,
      y       - целевая переменная,
      info    - словарь с имьютацией/PCA, чтобы использовать их при прогнозе.
    """
    df = profile_year_df.sort_values("Год").reset_index(drop=True).copy()

    # Лаги по целевой переменной
    df["IPN_lag1"] = df["ИПН_м"].shift(1)
    df["IPN_lag2"] = df["ИПН_м"].shift(2)
    df["IPN_roll3"] = df["ИПН_м"].shift(1).rolling(3).mean()

    # Базовый временной тренд
    df["year_num"] = df["Год"].astype(float)

    # Погодные признаки
    weather_cols = [c for c in df.columns if c not in ["Год", "ИПН_м", "n_points", "IPN_lag1", "IPN_lag2", "IPN_roll3", "year_num"]]
    weather_df = df[weather_cols].copy() if weather_cols else pd.DataFrame(index=df.index)

    if weather_df.empty:
        X = df[["year_num", "IPN_lag1", "IPN_lag2", "IPN_roll3"]].copy()
        X = X.dropna()
        y = df.loc[X.index, "ИПН_м"].copy()
        return X, y, {"weather_cols": [], "imputer": None, "scaler": None, "pca": None}

    # Импьютер и PCA для погодных признаков
    # Сначала заполним только на основании погодных данных (без целевой)
    imputer = SimpleImputer(strategy="median")
    weather_imp = imputer.fit_transform(weather_df)

    # PCA: не больше 4 компонент и не больше (n_samples - 1)
    n_samples, n_features = weather_imp.shape
    n_comp = min(pca_components, n_features, max(1, n_samples - 1))
    # Если погодных признаков совсем мало, PCA можно не делать
    if n_comp >= 1 and n_features >= 2 and n_samples >= 3:
        weather_scaler = StandardScaler()
        weather_scaled = weather_scaler.fit_transform(weather_imp)

        pca = PCA(n_components=n_comp, random_state=42)
        weather_pcs = pca.fit_transform(weather_scaled)
        pc_cols = [f"weather_pc{i+1}" for i in range(weather_pcs.shape[1])]
        weather_pc_df = pd.DataFrame(weather_pcs, columns=pc_cols, index=df.index)
    else:
        weather_scaler = StandardScaler()
        weather_scaled = weather_scaler.fit_transform(weather_imp) if n_samples >= 2 else weather_imp
        pca = None
        pc_cols = [f"weather_f{i+1}" for i in range(weather_imp.shape[1])]
        weather_pc_df = pd.DataFrame(weather_scaled, columns=pc_cols, index=df.index)

    X = pd.concat(
        [df[["year_num", "IPN_lag1", "IPN_lag2", "IPN_roll3"]], weather_pc_df],
        axis=1
    )

    # Удаляем строки, где лаги не сформировались
    valid = X.notna().all(axis=1)
    X = X.loc[valid].copy()
    y = df.loc[valid, "ИПН_м"].copy()

    info = {
        "weather_cols": weather_cols,
        "imputer": imputer,
        "weather_scaler": weather_scaler,
        "pca": pca,
        "pc_cols": pc_cols,
        "weather_year_values": weather_df.copy(),
    }
    return X, y, info


def fit_profile_model(profile_year_df: pd.DataFrame, pca_components: int = 4, test_size: float = 0.2):
    """
    Обучает регрессионную модель на профильном ряду с погодой.
    Возвращает словарь с метриками и прогнозом.
    """
    df = profile_year_df.sort_values("Год").reset_index(drop=True).copy()
    if df.shape[0] < 5:
        return None

    X, y, info = build_feature_matrix(df, pca_components=pca_components)
    if len(X) < 4:
        return None

    # Хронологическое разделение train/test
    split = max(1, int(len(X) * (1 - test_size)))
    if split >= len(X):
        split = len(X) - 1

    X_train, X_test = X.iloc[:split].copy(), X.iloc[split:].copy()
    y_train, y_test = y.iloc[:split].copy(), y.iloc[split:].copy()

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("reg", Ridge(alpha=1.0, random_state=42))
    ])
    model.fit(X_train, y_train)

    y_pred_test = model.predict(X_test)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
    mae = float(mean_absolute_error(y_test, y_pred_test))
    r2 = float(r2_score(y_test, y_pred_test)) if len(y_test) >= 2 else np.nan

    # Для прогноза вперёд используем последнее погодное состояние (carry-forward)
    hist = df.dropna(subset=["ИПН_м"]).sort_values("Год").reset_index(drop=True).copy()
    if hist.shape[0] < 4:
        return None

    # Готовим погодные признаки для трансформации
    weather_cols = info["weather_cols"]
    if weather_cols:
        weather_hist = hist[weather_cols].copy()
        # Заполняем как при обучении
        weather_hist_imp = info["imputer"].transform(weather_hist)
        if info["pca"] is not None:
            weather_hist_scaled = info["weather_scaler"].transform(weather_hist_imp)
            weather_hist_pcs = info["pca"].transform(weather_hist_scaled)
            pc_hist_cols = [f"weather_pc{i+1}" for i in range(weather_hist_pcs.shape[1])]
            weather_hist_df = pd.DataFrame(weather_hist_pcs, columns=pc_hist_cols)
        else:
            weather_hist_df = pd.DataFrame(info["weather_scaler"].transform(weather_hist_imp), columns=info["pc_cols"])
    else:
        weather_hist_df = pd.DataFrame(index=hist.index)

    # Прогноз на future years
    future_years = np.arange(int(hist["Год"].max()) + 1, int(hist["Год"].max()) + 21)

    # "Сценарий" погоды: берём последнее доступное годовое состояние
    if weather_cols:
        last_weather_vec = hist[weather_cols].iloc[-1:].copy()
        future_weather_raw = pd.concat([last_weather_vec] * len(future_years), ignore_index=True)
        future_weather_imp = info["imputer"].transform(future_weather_raw)
        if info["pca"] is not None:
            future_weather_scaled = info["weather_scaler"].transform(future_weather_imp)
            future_weather_pc = info["pca"].transform(future_weather_scaled)
            future_weather_df = pd.DataFrame(future_weather_pc, columns=[f"weather_pc{i+1}" for i in range(future_weather_pc.shape[1])])
        else:
            future_weather_df = pd.DataFrame(info["weather_scaler"].transform(future_weather_imp), columns=info["pc_cols"])
    else:
        future_weather_df = pd.DataFrame(index=range(len(future_years)))

    # Рекурсивный прогноз
    history_values = hist["ИПН_м"].tolist()
    forecast_values = []

    # Для удобства получим последние лаги из истории
    last_year = int(hist["Год"].iloc[-1])

    for i, year in enumerate(future_years):
        lag1 = history_values[-1] if len(history_values) >= 1 else np.nan
        lag2 = history_values[-2] if len(history_values) >= 2 else np.nan
        roll3 = np.mean(history_values[-3:]) if len(history_values) >= 3 else np.nan

        # weather pcs for this future year
        if not future_weather_df.empty:
            weather_row = future_weather_df.iloc[[i]].copy().reset_index(drop=True)
        else:
            weather_row = pd.DataFrame(index=[0])

        feat = pd.DataFrame({
            "year_num": [float(year)],
            "IPN_lag1": [lag1],
            "IPN_lag2": [lag2],
            "IPN_roll3": [roll3],
        })

        feat = pd.concat([feat, weather_row], axis=1)

        # Прогноз
        pred = float(model.predict(feat)[0])
        forecast_values.append(pred)
        history_values.append(pred)

    return {
        "model": model,
        "metrics": {"RMSE": rmse, "MAE": mae, "R2": r2},
        "years_hist": hist["Год"].values,
        "values_hist": hist["ИПН_м"].values,
        "future_years": future_years,
        "forecast": np.array(forecast_values),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "feature_columns": list(X.columns),
        "weather_columns": weather_cols,
    }


# =========================
# 5) КОРРЕЛЯЦИОННЫЕ ГРАФИКИ ПО ПРОФИЛЯМ (КАК В БАЗОВОМ КОДЕ)
# =========================
n_corr = 0
for site, g in df_all.groupby("site"):
    pivot = g.pivot_table(index="Год", columns="profile", values="ИПН_м", aggfunc="mean")
    pivot = pivot.dropna(axis=1, thresh=3)
    if pivot.shape[1] <= 1:
        continue
    corr = pivot.corr()
    corr.to_excel(CORR_MATRICES_DIR / f"{safe_filename(site)}_corr_matrix.xlsx")
    out_png = CORR_PLOTS_DIR / f"{safe_filename(site)}_corr_heatmap.png"
    save_corr_heatmap(corr, out_png, title=f"Корреляции профилей — {site} (по ИПН)")
    n_corr += 1

print("Correlation matrices/heatmaps created for sites:", n_corr)


# =========================
# 6) ПРОГНОЗ ДЛЯ КАЖДОГО ПРОФИЛЯ С УЧЁТОМ ПОГОДНЫХ ПРИЗНАКОВ
# =========================
n_future = 20
results = []

for (site, profile), grp in df_all.groupby(["site", "profile"]):
    print(f"Processing: {site} / {profile}")

    profile_year_df = make_profile_year_table(grp, weather_yearly)
    profile_year_df.to_excel(OUT_DIR / f"{safe_filename(site)}__{safe_filename(profile)}_merged_yearly.xlsx", index=False)

    if profile_year_df.shape[0] < 5:
        print(f"  Skipped: too few yearly points ({profile_year_df.shape[0]})")
        continue

    res = fit_profile_model(profile_year_df, pca_components=4, test_size=0.2)
    if res is None:
        print("  Skipped: model training failed")
        continue

    # Сохраняем таблицу прогноза
    forecast_df = pd.DataFrame({
        "Год": np.concatenate([res["years_hist"], res["future_years"]]),
        "ИПН_факт": np.concatenate([res["values_hist"], np.repeat(np.nan, len(res["future_years"]))]),
        "ИПН_прогноз": np.concatenate([np.repeat(np.nan, len(res["years_hist"])), res["forecast"]]),
    })
    forecast_df.to_excel(OUT_DIR / f"{safe_filename(site)}__{safe_filename(profile)}_forecast.xlsx", index=False)

    results.append({
        "site": site,
        "profile": profile,
        "rmse": res["metrics"]["RMSE"],
        "mae": res["metrics"]["MAE"],
        "r2": res["metrics"]["R2"],
        "n_train": res["train_rows"],
        "n_test": res["test_rows"],
    })

    metric_text = f"(RMSE={res['metrics']['RMSE']:.2f}, R2={res['metrics']['R2']:.2f})"
    out_png = PROFILE_PLOTS_DIR / f"{safe_filename(site)}__{safe_filename(profile)}__weather_forecast.png"
    title = f"{site} / {profile} — прогноз с погодными признаками"
    plot_profile_ipn_forecast(
        profile_year_df,
        out_png,
        ycol="ИПН_м",
        title=title,
        future_years=res["future_years"],
        future_pred=res["forecast"],
        model_name="Ridge+PCA",
        metric_text=metric_text
    )

    print(f"  Saved plot: {out_png.name}")

results_df = pd.DataFrame(results).sort_values(["site", "profile"])
results_df.to_excel(OUT_DIR / "model_results.xlsx", index=False)
print("\nВсего успешно обработано профилей:", len(results_df))


# =========================
# 7) САЙТОВЫЕ ГРАФИКИ ПО СРЕДНЕМУ ИПН
# =========================
n_site_plots = 0
if not df_all.empty:
    site_year = df_all.groupby(["site", "Год"])["ИПН_м"].mean().reset_index()

    for site, grp in site_year.groupby("site"):
        grp = grp.dropna(subset=["Год", "ИПН_м"]).sort_values("Год")
        if grp.shape[0] < 3:
            continue

        # Для наглядности тоже дадим простой тренд-прогноз
        # (здесь не добавляем погоду, только отдельный сайтовый обзор)
        z = np.polyfit(grp["Год"], grp["ИПН_м"], 1)
        p_lin = np.poly1d(z)
        future_years = np.arange(int(grp["Год"].max()) + 1, int(grp["Год"].max()) + 21)
        future_pred = p_lin(future_years)

        out_name = SITE_PLOTS_DIR / f"site_forecast_{safe_filename(site)}.png"
        plt.figure(figsize=(11, 6))
        plt.plot(grp["Год"], grp["ИПН_м"], "o-", color="black", linewidth=1.8, markersize=4, label="Факт")
        plt.plot(future_years, future_pred, "--", linewidth=2, label="Линейный тренд")
        plt.xlabel("Год", fontsize=12)
        plt.ylabel("ИПН, м", fontsize=12)
        plt.title(f"Участок: {site} (среднее ИПН по году)", fontsize=14, fontweight="bold")
        plt.grid(True, linestyle=":", alpha=0.7)
        plt.legend(fontsize=10)
        plt.tight_layout()
        plt.savefig(out_name, dpi=150, bbox_inches="tight")
        plt.close()
        n_site_plots += 1

print("Site-level plots saved:", n_site_plots)


# =========================
# 8) ФИНАЛЬНАЯ СВОДКА
# =========================
summary = df_all.groupby(["site", "profile"]).agg(
    n_obs=("ИПН_м", "count"),
    year_first=("Год", "min"),
    year_last=("Год", "max"),
    mean_IPN=("ИПН_м", "mean"),
    std_IPN=("ИПН_м", "std"),
).reset_index()

summary.to_excel(OUT_DIR / "profile_summary_final.xlsx", index=False)
print("\nГотово.")
print("Results saved to:", OUT_DIR.resolve())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт: сбор данных профилей + литология -> обучение MLPRegressor с нормализацией и one-hot кодированием ->
прогноз 5 лет вперёд (по каждому профилю) -> вычисление прогнозной скорости (м/год) ->
сохранение прогнозов, сравнения факта/прогноза и метрик в Excel.
Графики факта и предсказания для каждого профиля.
"""

import pandas as pd
import numpy as np
import re
import os
import warnings
warnings.simplefilter("ignore")

import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ---------------------------
# Настройки файлов / папок
# ---------------------------
profiles_file = "240716 БРОВКИ Баранова.xls"
lithology_file = "Ориентация берега участки.xlsx"
output_dir = "results_models"
os.makedirs(output_dir, exist_ok=True)

# ---------------------------
# Утилиты
# ---------------------------
def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def detect_profiles(df):
    profile_columns = []
    for i, col in enumerate(df.columns):
        col_str = safe_str(col)
        if re.search(r'ПРОФИЛЬ\s*№?\s*\d+', col_str, re.IGNORECASE):
            profile_columns.append(i)
    if not profile_columns and len(df)>0:
        first_row = df.iloc[0].astype(str).tolist()
        for i, cell in enumerate(first_row):
            if isinstance(cell, str) and re.search(r'ПРОФИЛЬ\s*№?\s*\d+', cell, re.IGNORECASE):
                profile_columns.append(i)
    profiles = []
    if profile_columns:
        profile_columns = sorted(set(profile_columns))
        profile_columns.append(len(df.columns))
        for i in range(len(profile_columns)-1):
            start_col = profile_columns[i]
            end_col = profile_columns[i+1]-1
            header_text = ""
            try:
                header_text = str(df.iloc[0, start_col])
            except Exception:
                header_text = str(df.columns[start_col])
            match = re.search(r'№\s*(\d+)', header_text)
            if match:
                profile_name = f"Профиль №{match.group(1)}"
            else:
                profile_name = f"Профиль_{i+1}"
            profiles.append((profile_name, start_col, end_col))
    return profiles

def extract_profile_data(df, start_col, end_col, header_rows_to_skip=3):
    block = df.iloc[header_rows_to_skip:, start_col:end_col+1].copy().reset_index(drop=True)
    if block.empty:
        return None
    block.columns = [f"c{i}" for i in range(block.shape[1])]
    # столбец даты
    date_col = None
    for c in block.columns[:min(3, block.shape[1])]:
        parsed = pd.to_datetime(block[c], errors='coerce')
        if parsed.notna().sum() >= max(1, int(0.3*len(parsed))):
            date_col = c
            block[c] = parsed
            break
    if date_col is None:
        try:
            block['c0'] = pd.to_datetime(block['c0'], errors='coerce')
            if block['c0'].notna().sum() > 0:
                date_col = 'c0'
        except Exception:
            date_col = None
    # столбец расстояния
    dist_col = None
    for c in reversed(block.columns):
        if c == date_col:
            continue
        if pd.to_numeric(block[c], errors='coerce').notna().any():
            dist_col = c
            break
    if dist_col is None:
        if block.shape[1] >= 2:
            dist_col = 'c1'
        else:
            return None
    if date_col is None:
        return None
    block['Дата'] = pd.to_datetime(block[date_col], errors='coerce')
    block['Расстояние_до_ПН'] = pd.to_numeric(block[dist_col], errors='coerce')
    res = block[['Дата','Расстояние_до_ПН']].dropna(subset=['Дата','Расстояние_до_ПН']).reset_index(drop=True)
    if res.empty:
        return None
    return res

def load_and_classify_lithology(path):
    try:
        ldf = pd.read_excel(path)
    except Exception as e:
        print("Ошибка чтения файла литологии:", e)
        return None
    ldf.columns = [safe_str(c) for c in ldf.columns]
    name_col = None
    lit_col = None
    low = [c.lower() for c in ldf.columns]
    for c, cl in zip(ldf.columns, low):
        if 'назван' in cl and 'участ' in cl:
            name_col = c
        if 'литолог' in cl or 'состав' in cl:
            lit_col = c
    if name_col is None:
        name_col = ldf.columns[0]
    if lit_col is None:
        lit_col = ldf.columns[-1]
    ldf = ldf[[name_col, lit_col]].rename(columns={name_col: 'Название_участка', lit_col: 'Литология_описание'})
    ldf['Название_участка'] = ldf['Название_участка'].astype(str).str.strip()
    ldf['Литология_описание'] = ldf['Литология_описание'].fillna("").astype(str)
    def classify_safe(text):
        t = str(text).lower()
        if any(x in t for x in ['опок','песчаник','кремнист']): return 'Твёрдые породы'
        if any(x in t for x in ['глин','хвалынск']): return 'Глины'
        if 'суглин' in t: return 'Суглинки'
        if any(x in t for x in ['супес','песок','песк']): return 'Пески/супеси'
        if t.strip()=='' : return 'Не указано'
        return 'Другое/смешанное'
    ldf['Литология_класс'] = ldf['Литология_описание'].apply(classify_safe)
    ldf['Key'] = ldf['Название_участка'].astype(str).str.strip().str.lower()
    return ldf

# ---------------------------
# Загрузка данных профилей
# ---------------------------
xls = pd.ExcelFile(profiles_file)
rows_all = []

for sheet in xls.sheet_names:
    raw = pd.read_excel(profiles_file, sheet_name=sheet, header=None)
    header_row_idx = 0
    for i in range(min(6,len(raw))):
        text = " ".join([str(x) for x in raw.iloc[i].values]).lower()
        if 'дата' in text or 'профиль' in text:
            header_row_idx = i
            break
    df = pd.read_excel(profiles_file, sheet_name=sheet, header=header_row_idx)
    if df.empty: continue
    profiles_info = detect_profiles(df)
    if not profiles_info:
        ncols = df.shape[1]
        if ncols >= 6:
            block = ncols // 3
            profiles_info = []
            for i in range(3):
                s = i*block
                e = (i+1)*block-1 if i<2 else ncols-1
                profiles_info.append((f"Профиль_{i+1}", s, e))
        else:
            continue
    for pname, scol, ecol in profiles_info:
        data = extract_profile_data(df, scol, ecol, header_rows_to_skip=header_row_idx+1)
        if data is None: continue
        data['Год'] = data['Дата'].dt.year
        data['Участок'] = sheet
        data['Профиль'] = pname
        data = data.dropna(subset=['Год','Расстояние_до_ПН'])
        data['Год'] = data['Год'].astype(int)
        data['Расстояние_до_ПН'] = pd.to_numeric(data['Расстояние_до_ПН'], errors='coerce')
        rows_all.append(data[['Год','Участок','Профиль','Расстояние_до_ПН']])

if not rows_all:
    raise SystemExit("Не найдено данных профилей в файле.")
df_all = pd.concat(rows_all, ignore_index=True)

# ---------------------------
# Литология
# ---------------------------
litho = load_and_classify_lithology(lithology_file)
df_all['Key'] = df_all['Участок'].astype(str).str.strip().str.lower()
if litho is not None:
    df_all = df_all.merge(litho[['Key','Литология_класс']], on='Key', how='left')
else:
    df_all['Литология_класс'] = 'Не указано'
df_all['Литология_класс'] = df_all['Литология_класс'].fillna('Не указано').astype(str)

# ---------------------------
# Подготовка признаков
# ---------------------------
cat_features = ['Участок','Профиль','Литология_класс']
num_features = ['Год']
y = df_all['Расстояние_до_ПН'].values

preprocessor = ColumnTransformer([
    ('num', StandardScaler(), num_features),
    ('cat', OneHotEncoder(sparse_output=False), cat_features)
])

mlp_model = Pipeline([
    ('pre', preprocessor),
    ('mlp', MLPRegressor(hidden_layer_sizes=(32,16),
                         activation='relu', solver='adam',
                         max_iter=2000, random_state=42))
])

mlp_model.fit(df_all[['Год','Участок','Профиль','Литология_класс']], y)
y_pred_in = mlp_model.predict(df_all[['Год','Участок','Профиль','Литология_класс']])

# ---------------------------
# Метрики
# ---------------------------
def metrics(y_true, y_pred):
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return {'MSE':mse, 'RMSE':rmse, 'MAE':mae, 'R2':r2}

metrics_mlp = metrics(y, y_pred_in)
pd.DataFrame([metrics_mlp], index=['MLPRegressor']).to_excel(os.path.join(output_dir,"model_metrics.xlsx"))

# ---------------------------
# Графики факта и предсказания
# ---------------------------
for (uch, prof), g in df_all.groupby(['Участок','Профиль']):
    plt.figure(figsize=(8,5))
    g_sorted = g.sort_values('Год')
    X_plot = g_sorted[['Год','Участок','Профиль','Литология_класс']]
    y_pred_plot = mlp_model.predict(X_plot)
    plt.plot(g_sorted['Год'], g_sorted['Расстояние_до_ПН'], 'o-', label='Факт')
    plt.plot(g_sorted['Год'], y_pred_plot, 's--', label='MLPPred')
    plt.title(f"{uch} - {prof}")
    plt.xlabel('Год')
    plt.ylabel('Расстояние до ПН (м)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir,f"{uch}_{prof}_plot.png"))
    plt.close()

# ---------------------------
# Прогноз на 5 лет
# ---------------------------
future_horizon = 5
pred_rows = []

for (uch, prof), g in df_all.groupby(['Участок','Профиль']):
    g_sorted = g.sort_values('Год')
    last_year = int(g_sorted['Год'].max())
    lito_val = g_sorted['Литология_класс'].iloc[0]
    last_distance = float(g_sorted.loc[g_sorted['Год']==last_year,'Расстояние_до_ПН'].iloc[-1])

    future_years = np.arange(last_year+1,last_year+1+future_horizon)
    X_future = pd.DataFrame({
        'Год': future_years,
        'Участок': [uch]*future_horizon,
        'Профиль': [prof]*future_horizon,
        'Литология_класс': [lito_val]*future_horizon
    })
    preds_future = mlp_model.predict(X_future)
    speed = (preds_future[-1]-last_distance)/future_horizon

    for fy, p in zip(future_years, preds_future):
        pred_rows.append({
            'Участок': uch,
            'Профиль': prof,
            'Год': int(fy),
            'Прогноз_MLP': float(p),
            'Литология_класс': lito_val
        })
    pred_rows.append({
        'Участок': uch,
        'Профиль': prof,
        'Год': f"{last_year+1}-{last_year+future_horizon}",
        'Прогноз_MLP': float(preds_future[-1]),
        'Литология_класс': lito_val,
        'Прогнозная_скорость_MLP_м_в_год': speed
    })

pred_df = pd.DataFrame(pred_rows)
pred_df.to_excel(os.path.join(output_dir,"predictions_5yrs_per_profile.xlsx"), index=False)
print("Готово. Результаты находятся в папке:", output_dir)

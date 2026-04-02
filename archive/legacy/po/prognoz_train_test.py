#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
import os
import warnings
warnings.simplefilter("ignore")

import matplotlib
matplotlib.use('Agg')  # для сохранения файлов без GUI
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ---------------------------
# Настройки файлов и папки результатов
# ---------------------------
profiles_file = "БРОВКИ.xlsx"
lithology_file = "Ориентация берега участки.xlsx"
output_dir = os.path.abspath("results_models")
os.makedirs(output_dir, exist_ok=True)
print("Результаты будут сохраняться в:", output_dir)

# ---------------------------
# Утилиты
# ---------------------------
def safe_str(x):
    return "" if pd.isna(x) else str(x).strip()

def safe_name(s):
    s = re.sub(r'[^\w\d-]', '_', s)
    return s.encode('ascii', errors='ignore').decode()

def detect_profiles(df):
    profile_columns = []
    for i, col in enumerate(df.columns):
        if re.search(r'ПРОФИЛЬ\s*№?\s*\d+', safe_str(col), re.IGNORECASE):
            profile_columns.append(i)
    if not profile_columns and len(df) > 0:
        for i, cell in enumerate(df.iloc[0].astype(str).tolist()):
            if isinstance(cell, str) and re.search(r'ПРОФИЛЬ\s*№?\s*\d+', cell, re.IGNORECASE):
                profile_columns.append(i)
    profiles = []
    if profile_columns:
        profile_columns = sorted(set(profile_columns))
        profile_columns.append(len(df.columns))
        for i in range(len(profile_columns)-1):
            s = profile_columns[i]
            e = profile_columns[i+1]-1
            header_text = str(df.iloc[0, s]) if s<len(df) else str(df.columns[s])
            match = re.search(r'№\s*(\d+)', header_text)
            profile_name = f"Профиль №{match.group(1)}" if match else f"Профиль_{i+1}"
            profiles.append((profile_name, s, e))
    return profiles

def extract_profile_data(df, start_col, end_col, header_rows_to_skip=3):
    block = df.iloc[header_rows_to_skip:, start_col:end_col+1].copy().reset_index(drop=True)
    if block.empty: return None
    block.columns = [f"c{i}" for i in range(block.shape[1])]
    date_col = None
    for c in block.columns[:min(3, block.shape[1])]:
        parsed = pd.to_datetime(block[c], errors='coerce')
        if parsed.notna().sum() >= max(1,int(0.3*len(parsed))):
            date_col = c
            block[c] = parsed
            break
    if date_col is None:
        block['c0'] = pd.to_datetime(block['c0'], errors='coerce')
        date_col = 'c0' if block['c0'].notna().sum()>0 else None
    dist_col = None
    for c in reversed(block.columns):
        if c != date_col and pd.to_numeric(block[c], errors='coerce').notna().any():
            dist_col = c
            break
    if dist_col is None: dist_col = 'c1' if block.shape[1]>=2 else None
    if date_col is None or dist_col is None: return None
    block['Дата'] = pd.to_datetime(block[date_col], errors='coerce')
    block['Расстояние_до_ПН'] = pd.to_numeric(block[dist_col], errors='coerce')
    res = block[['Дата','Расстояние_до_ПН']].dropna(subset=['Дата','Расстояние_до_ПН']).reset_index(drop=True)
    return res if not res.empty else None

def load_and_classify_lithology(path):
    try: ldf = pd.read_excel(path)
    except: return None
    ldf.columns = [safe_str(c) for c in ldf.columns]
    name_col = ldf.columns[0]
    lit_col = ldf.columns[-1]
    ldf = ldf[[name_col, lit_col]].rename(columns={name_col:'Название_участка', lit_col:'Литология_описание'})
    ldf['Название_участка'] = ldf['Название_участка'].astype(str).str.strip()
    ldf['Литология_описание'] = ldf['Литология_описание'].fillna("").astype(str)
    def classify_safe(t):
        t = str(t).lower()
        if any(x in t for x in ['опок','песчаник','кремнист']): return 'Твёрдые породы'
        if any(x in t for x in ['глин','хвалынск']): return 'Глины'
        if 'суглин' in t: return 'Суглинки'
        if any(x in t for x in ['супес','песок','песк']): return 'Пески/супеси'
        if t.strip()=='': return 'Не указано'
        return 'Другое/смешанное'
    ldf['Литология_класс'] = ldf['Литология_описание'].apply(classify_safe)
    ldf['Key'] = ldf['Название_участка'].astype(str).str.strip().str.lower()
    return ldf

# ---------------------------
# Загрузка профилей
# ---------------------------
xls = pd.ExcelFile(profiles_file)
rows_all = []

for sheet in xls.sheet_names:
    raw = pd.read_excel(profiles_file, sheet_name=sheet, header=None)
    header_row_idx = 0
    for i in range(min(6,len(raw))):
        text = " ".join([str(x) for x in raw.iloc[i].values]).lower()
        if 'дата' in text or 'профиль' in text: header_row_idx=i; break
    df = pd.read_excel(profiles_file, sheet_name=sheet, header=header_row_idx)
    if df.empty: continue
    profiles_info = detect_profiles(df)
    if not profiles_info:
        ncols = df.shape[1]
        if ncols>=6:
            block = ncols//3
            profiles_info=[]
            for i in range(3):
                s=i*block
                e=(i+1)*block-1 if i<2 else ncols-1
                profiles_info.append((f"Профиль_{i+1}",s,e))
        else: continue
    for pname,scol,ecol in profiles_info:
        data = extract_profile_data(df,scol,ecol,header_rows_to_skip=header_row_idx+1)
        if data is None: continue
        data['Год']=data['Дата'].dt.year
        data['Участок']=sheet
        data['Профиль']=pname
        data = data.dropna(subset=['Год','Расстояние_до_ПН'])
        data['Год']=data['Год'].astype(int)
        data['Расстояние_до_ПН']=pd.to_numeric(data['Расстояние_до_ПН'], errors='coerce')
        rows_all.append(data[['Год','Участок','Профиль','Расстояние_до_ПН']])

df_all=pd.concat(rows_all,ignore_index=True)
df_all['Key']=df_all['Участок'].astype(str).str.strip().str.lower()

# ---------------------------
# Литология
# ---------------------------
litho = load_and_classify_lithology(lithology_file)
if litho is not None:
    df_all = df_all.merge(litho[['Key','Литология_класс']], on='Key', how='left')
else:
    df_all['Литология_класс'] = 'Не указано'
df_all['Литология_класс'] = df_all['Литология_класс'].fillna('Не указано')

# ---------------------------
# Признаки и цель
# ---------------------------
X = df_all[['Год','Участок','Профиль','Литология_класс']]
y = df_all['Расстояние_до_ПН']

# ---------------------------
# Preprocessing + MLP на всех данных
# ---------------------------
num_features = ['Год']
cat_features = ['Участок','Профиль','Литология_класс']

preprocessor = ColumnTransformer([
    ('num', StandardScaler(), num_features),
    ('cat', OneHotEncoder(sparse_output=False), cat_features)
])

mlp_model = Pipeline([
    ('pre', preprocessor),
    ('mlp', MLPRegressor(hidden_layer_sizes=(32,16),activation='relu',
                         solver='adam',max_iter=500,random_state=42))
])

mlp_model.fit(X, y)
y_pred_in = mlp_model.predict(X)

# ---------------------------
# Метрики in-sample
# ---------------------------
def metrics(y_true,y_pred):
    mse = mean_squared_error(y_true,y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_true,y_pred)
    r2 = r2_score(y_true,y_pred)
    return {'MSE':mse,'RMSE':rmse,'MAE':mae,'R2':r2}

metrics_df = pd.DataFrame([metrics(y,y_pred_in)], index=['In-sample'])
metrics_df.to_excel(os.path.join(output_dir,"model_metrics_in_sample.xlsx"))
print(metrics_df)

# ---------------------------
# Прогноз на 5 лет вперед
# ---------------------------
future_horizon = 20
pred_rows = []

for (uch, prof), g in df_all.groupby(['Участок','Профиль']):
    g_sorted = g.sort_values('Год')
    last_year = int(g_sorted['Год'].max())
    lito_val = g_sorted['Литология_класс'].iloc[0]
    last_distance = float(g_sorted.loc[g_sorted['Год']==last_year,'Расстояние_до_ПН'].iloc[-1])

    # создаём будущие годы
    future_years = np.arange(last_year+1,last_year+1+future_horizon)
    X_future = pd.DataFrame({
        'Год': future_years,
        'Участок':[uch]*future_horizon,
        'Профиль':[prof]*future_horizon,
        'Литология_класс':[lito_val]*future_horizon
    })
    preds_future = mlp_model.predict(X_future)
    speed = (preds_future[-1]-last_distance)/future_horizon

    # записываем прогноз на каждый год
    for fy,p in zip(future_years,preds_future):
        pred_rows.append({'Участок':uch,'Профиль':prof,'Год':int(fy),
                          'Прогноз_MLP':float(p),'Литология_класс':lito_val})
    pred_rows.append({'Участок':uch,'Профиль':prof,'Год':f"{last_year+1}-{last_year+future_horizon}",
                      'Прогноз_MLP':float(preds_future[-1]),
                      'Литология_класс':lito_val,'Прогнозная_скорость_MLP_м_в_год':speed})

pred_df = pd.DataFrame(pred_rows)
pred_df.to_excel(os.path.join(output_dir,"predictions_5yrs_per_profile.xlsx"), index=False)

# ---------------------------
# Графики in-sample + прогноз (без ограничения ASCII)
# ---------------------------
for (uch, prof), g in df_all.groupby(['Участок','Профиль']):
    # безопасное имя файла: заменяем только запрещённые символы для пути
    file_uch = uch.replace('/','_').replace('\\','_')
    file_prof = prof.replace('/','_').replace('\\','_')
    print(file_uch, file_prof)
    g_sorted = g.sort_values('Год')
    last_year = int(g_sorted['Год'].max())
    lito_val = g_sorted['Литология_класс'].iloc[0]

    # In-sample предсказания
    X_plot = g_sorted[['Год','Участок','Профиль','Литология_класс']]
    y_in_pred = mlp_model.predict(X_plot)

    # Прогноз на 5 лет вперед
    future_years = np.arange(last_year+1,last_year+1+future_horizon)
    X_future = pd.DataFrame({
        'Год': future_years,
        'Участок':[uch]*future_horizon,
        'Профиль':[prof]*future_horizon,
        'Литология_класс':[lito_val]*future_horizon
    })
    preds_future = mlp_model.predict(X_future)

    # Построение графика
    plt.figure(figsize=(10,5))
    plt.plot(g_sorted['Год'], g_sorted['Расстояние_до_ПН'], 'o-', label='Факт', markersize=5)
    plt.plot(g_sorted['Год'], y_in_pred, 's--', color='orange', label='In-sample MLP_pred')
    plt.plot(future_years, preds_future, 'd--', color='red', label='Прогноз 20 лет')
    plt.title(f"{uch} - {prof} (In-sample + прогноз)")
    plt.xlabel('Год')
    plt.ylabel('Расстояние до ПН (м)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{file_uch}{file_prof}in_sample_forecast.png"))
    plt.show()
    plt.close()


# ---------------------------
# 1️⃣ Влияние литологии на прогноз (bar chart)
# ---------------------------
litho_classes = df_all['Литология_класс'].unique()
litho_avg_pred = []

for lito in litho_classes:
    subset = df_all[df_all['Литология_класс']==lito]
    last_records = subset.groupby(['Участок','Профиль']).apply(lambda x: x.loc[x['Год'].idxmax()])
    X_lito = last_records[['Год','Участок','Профиль','Литология_класс']]
    preds = mlp_model.predict(X_lito)
    litho_avg_pred.append({'Литология_класс': lito, 'Средний_прогноз': preds.mean()})

litho_pred_df = pd.DataFrame(litho_avg_pred).sort_values('Средний_прогноз', ascending=False)

plt.figure(figsize=(10,6))
positions = np.arange(len(litho_pred_df))
plt.bar(positions, litho_pred_df['Средний_прогноз'], color='skyblue')
plt.xticks(positions, litho_pred_df['Литология_класс'], rotation=45, ha='right')
plt.ylabel('Среднее расстояние до ПН (м)')
plt.title('Влияние литологии на прогноз MLP')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(os.path.join(output_dir,'lithology_influence.png'), dpi=150)
plt.close()
print("График влияния литологии сохранён.")

# ---------------------------
# 2️⃣ Факт vs предсказание (scatter plot)
# ---------------------------
plt.figure(figsize=(8,8))
plt.scatter(df_all['Расстояние_до_ПН'], y_pred_in, alpha=0.6, c='green', edgecolor='k')
plt.plot([df_all['Расстояние_до_ПН'].min(), df_all['Расстояние_до_ПН'].max()],
         [df_all['Расстояние_до_ПН'].min(), df_all['Расстояние_до_ПН'].max()],
         'r--', lw=2)  # линия y=x
plt.xlabel('Факт (м)')
plt.ylabel('MLP предсказание (м)')
plt.title('Факт vs предсказание (in-sample)')
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(output_dir,'fact_vs_pred_in_sample.png'), dpi=150)
plt.close()
print("График факт vs предсказание сохранён.")


print("Прогнозы и графики сохранены в папке:", output_dir)

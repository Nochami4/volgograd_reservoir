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
import seaborn as sns

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
# УЛУЧШЕННЫЙ ПРОГНОЗ С ВОЗМОЖНОСТЬЮ ОТРИЦАТЕЛЬНЫХ ЗНАЧЕНИЙ
# ---------------------------
future_horizon = 20
pred_rows = []

for (uch, prof), g in df_all.groupby(['Участок','Профиль']):
    g_sorted = g.sort_values('Год')
    last_year = int(g_sorted['Год'].max())
    lito_val = g_sorted['Литология_класс'].iloc[0]
    last_distance = float(g_sorted.loc[g_sorted['Год']==last_year,'Расстояние_до_ПН'].iloc[-1])

    # Анализ исторического тренда
    historical_years = g_sorted['Год'].values
    historical_distances = g_sorted['Расстояние_до_ПН'].values
    
    # Вычисляем тренд
    if len(historical_years) >= 2:
        try:
            # Линейная регрессия для определения направления тренда
            trend_coef = np.polyfit(historical_years - historical_years.min(), historical_distances, 1)[0]
        except:
            trend_coef = 0
    else:
        trend_coef = 0

    # создаём будущие годы
    future_years = np.arange(last_year+1,last_year+1+future_horizon)
    X_future = pd.DataFrame({
        'Год': future_years,
        'Участок':[uch]*future_horizon,
        'Профиль':[prof]*future_horizon,
        'Литология_класс':[lito_val]*future_horizon
    })
    
    # Получаем прогноз от модели
    preds_future_raw = mlp_model.predict(X_future)
    
    # ФИЗИЧЕСКИЕ ОГРАНИЧЕНИЯ: ЕСЛИ ТРЕНД ОТРИЦАТЕЛЬНЫЙ - НЕ ДАЕМ РОСТА
    preds_future = preds_future_raw.copy()
    
    # Если исторический тренд отрицательный и модель предсказывает рост - корректируем
    if trend_coef < -0.1:  # Явный тренд отступания
        # Не позволяем прогнозу быть выше последнего значения
        max_allowed = last_distance
        preds_future = np.minimum(preds_future, max_allowed)
        
        # Если модель все равно предсказывает рост, заменяем на продолжение тренда
        if preds_future[-1] > last_distance:
            # Продолжаем исторический тренд
            for i in range(len(preds_future)):
                years_from_now = i + 1
                trend_based_pred = last_distance + trend_coef * years_from_now
                preds_future[i] = trend_based_pred
    
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
    
    # Получаем и корректируем прогноз как выше
    preds_future_raw = mlp_model.predict(X_future)
    preds_future = preds_future_raw.copy()
    
    # Анализ тренда для графика
    historical_years = g_sorted['Год'].values
    historical_distances = g_sorted['Расстояние_до_ПН'].values
    if len(historical_years) >= 2:
        try:
            trend_coef = np.polyfit(historical_years - historical_years.min(), historical_distances, 1)[0]
        except:
            trend_coef = 0
    else:
        trend_coef = 0
        
    # Применяем те же ограничения для графика
    if trend_coef < -0.1:
        max_allowed = g_sorted['Расстояние_до_ПН'].iloc[-1]
        preds_future = np.minimum(preds_future, max_allowed)
        if preds_future[-1] > g_sorted['Расстояние_до_ПН'].iloc[-1]:
            for i in range(len(preds_future)):
                years_from_now = i + 1
                trend_based_pred = g_sorted['Расстояние_до_ПН'].iloc[-1] + trend_coef * years_from_now
                preds_future[i] = trend_based_pred

    # Построение графика
    plt.figure(figsize=(10,6))
    plt.plot(g_sorted['Год'], g_sorted['Расстояние_до_ПН'], 'o-', label='Факт', markersize=5)
    plt.plot(g_sorted['Год'], y_in_pred, 's--', color='orange', label='In-sample MLP_pred')
    plt.plot(future_years, preds_future, 'd--', color='red', label='Прогноз 20 лет')
    
    # Линия нуля (ПН) и зона отступания
    plt.axhline(y=0, color='black', linestyle='-', linewidth=1, alpha=0.7, label='Пункт наблюдения (ПН)')
    plt.axhspan(-100, 0, alpha=0.1, color='red', label='Зона за ПН')
    
    plt.title(f"{uch} - {prof} (In-sample + прогноз)\nЛитология: {lito_val}")
    plt.xlabel('Год')
    plt.ylabel('Расстояние до ПН (м)\n(отрицательные = отступание за ПН)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{file_uch}{file_prof}in_sample_forecast.png"))
    plt.close()

# ---------------------------
# ГРАФИК: Все участки на одном графике
# ---------------------------
plt.figure(figsize=(14, 8))

# Цвета для разных участков
colors = plt.cm.tab10(np.linspace(0, 1, len(df_all['Участок'].unique())))

for i, (uch, uch_data) in enumerate(df_all.groupby('Участок')):
    color = colors[i]
    
    # Для каждого профиля в участке
    for prof, prof_data in uch_data.groupby('Профиль'):
        prof_sorted = prof_data.sort_values('Год')
        
        # In-sample предсказания
        X_plot = prof_sorted[['Год','Участок','Профиль','Литология_класс']]
        y_in_pred = mlp_model.predict(X_plot)
        
        # Рисуем фактические данные
        plt.plot(prof_sorted['Год'], prof_sorted['Расстояние_до_ПН'], 
                'o-', color=color, markersize=3, linewidth=1, alpha=0.7,
                label=f'{uch} - {prof} (факт)' if i == 0 else "")
        
        # Рисуем предсказания
        plt.plot(prof_sorted['Год'], y_in_pred, 
                's--', color=color, markersize=2, linewidth=1, alpha=0.7,
                label=f'{uch} - {prof} (предсказание)' if i == 0 else "")

# Линия нуля и зона отступания
plt.axhline(y=0, color='black', linestyle='-', linewidth=1, alpha=0.7)
plt.axhspan(-100, 0, alpha=0.1, color='red')

plt.title('Все участки и профили (факт и предсказания)', fontsize=14)
plt.xlabel('Год', fontsize=12)
plt.ylabel('Расстояние до ПН (м)\n(отрицательные = отступание за ПН)', fontsize=12)
plt.grid(True, alpha=0.3)

# Упрощаем легенду - показываем только цвета участков
handles, labels = plt.gca().get_legend_handles_labels()
unique_labels = {}
for handle, label in zip(handles, labels):
    uch_name = label.split(' - ')[0]
    if uch_name not in unique_labels:
        unique_labels[uch_name] = handle

# Создаем упрощенную легенду
plt.legend(unique_labels.values(), unique_labels.keys(), 
          loc='upper right', bbox_to_anchor=(1.15, 1),
          title='Участки')

plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'all_sites_comparison.png'), 
           dpi=150, bbox_inches='tight')
plt.close()
print("График всех участков сохранён.")

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
# 2️⃣ Факт vs предсказание (scatter plot) С ЛЕГЕНДОЙ
# ---------------------------
plt.figure(figsize=(10,8))

# Создаем scatter plot с цветами по литологии
litho_colors = {
    'Твёрдые породы': 'red',
    'Глины': 'blue', 
    'Суглинки': 'green',
    'Пески/супеси': 'orange',
    'Не указано': 'gray',
    'Другое/смешанное': 'purple'
}

# Рисуем точки для каждого типа литологии
for lito_type in df_all['Литология_класс'].unique():
    mask = df_all['Литология_класс'] == lito_type
    color = litho_colors.get(lito_type, 'black')
    plt.scatter(df_all[mask]['Расстояние_до_ПН'], y_pred_in[mask], 
               alpha=0.6, c=color, edgecolor='k', label=lito_type, s=50)

# Линия y=x
min_val = min(df_all['Расстояние_до_ПН'].min(), y_pred_in.min())
max_val = max(df_all['Расстояние_до_ПН'].max(), y_pred_in.max())
plt.plot([min_val, max_val], [min_val, max_val], 
         'r--', lw=2, label='Идеальная линия (y=x)')

plt.xlabel('Факт (м)')
plt.ylabel('MLP предсказание (м)')
plt.title('Факт vs предсказание (in-sample)')
plt.grid(True, alpha=0.3)

# Добавляем легенду
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', 
           title='Литология', frameon=True, fancybox=True)

plt.tight_layout()
plt.savefig(os.path.join(output_dir,'fact_vs_pred_in_sample.png'), 
           dpi=150, bbox_inches='tight')
plt.close()
print("График факт vs предсказание с легендой сохранён.")

# ---------------------------
# ДОПОЛНИТЕЛЬНЫЙ ГРАФИК: Средние значения по участкам
# ---------------------------
plt.figure(figsize=(12, 6))

# Вычисляем средние значения по участкам и годам
site_year_avg = df_all.groupby(['Участок', 'Год'])['Расстояние_до_ПН'].mean().reset_index()

# Цвета для участков
colors = plt.cm.tab10(np.linspace(0, 1, len(site_year_avg['Участок'].unique())))

for i, (uch, uch_data) in enumerate(site_year_avg.groupby('Участок')):
    color = colors[i]
    uch_data_sorted = uch_data.sort_values('Год')
    
    plt.plot(uch_data_sorted['Год'], uch_data_sorted['Расстояние_до_ПН'], 
             'o-', color=color, linewidth=2, markersize=6, 
             label=uch, alpha=0.8)

# Линия нуля и зона отступания
plt.axhline(y=0, color='black', linestyle='-', linewidth=1, alpha=0.7)
plt.axhspan(-100, 0, alpha=0.1, color='red', label='Зона за ПН')

plt.title('Среднее расстояние до ПН по участкам', fontsize=14)
plt.xlabel('Год', fontsize=12)
plt.ylabel('Среднее расстояние до ПН (м)', fontsize=12)
plt.grid(True, alpha=0.3)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title='Участки')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, 'sites_average_trends.png'), 
           dpi=150, bbox_inches='tight')
plt.close()
print("График средних значений по участкам сохранён.")

# ---------------------------
# НОВЫЙ АНАЛИЗ: График ошибок для Бережновки
# ---------------------------
berezhnovka_data = df_all[df_all['Участок'] == 'Бережновка']

if not berezhnovka_data.empty:
    # Вычисляем ошибки для Бережновки
    berezhnovka_X = berezhnovka_data[['Год','Участок','Профиль','Литология_класс']]
    berezhnovka_y_pred = mlp_model.predict(berezhnovka_X)
    berezhnovka_data = berezhnovka_data.copy()
    berezhnovka_data['Ошибка'] = berezhnovka_data['Расстояние_до_ПН'] - berezhnovka_y_pred
    berezhnovka_data['Абсолютная_ошибка'] = np.abs(berezhnovka_data['Ошибка'])
    
    # График ошибок по годам и профилям
    plt.figure(figsize=(12, 6))
    
    for prof in berezhnovka_data['Профиль'].unique():
        prof_data = berezhnovka_data[berezhnovka_data['Профиль'] == prof].sort_values('Год')
        plt.plot(prof_data['Год'], prof_data['Ошибка'], 'o-', label=prof, markersize=4, linewidth=2)
    
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.7, label='Нулевая ошибка')
    plt.title('Ошибки модели для участка Бережновка\n(Факт - Предсказание)', fontsize=14)
    plt.xlabel('Год', fontsize=12)
    plt.ylabel('Ошибка (м)', fontsize=12)
    plt.legend(title='Профиль')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'berezhnovka_errors.png'), dpi=150)
    plt.close()
    
    # Статистика ошибок для Бережновки
    error_stats = {
        'Средняя_ошибка': berezhnovka_data['Ошибка'].mean(),
        'Средняя_абсолютная_ошибка': berezhnovka_data['Абсолютная_ошибка'].mean(),
        'Стандартное_отклонение_ошибки': berezhnovka_data['Ошибка'].std(),
        'Максимальная_ошибка': berezhnovka_data['Ошибка'].max(),
        'Минимальная_ошибка': berezhnovka_data['Ошибка'].min(),
        'Количество_наблюдений': len(berezhnovka_data)
    }
    
    error_stats_df = pd.DataFrame([error_stats])
    error_stats_df.to_excel(os.path.join(output_dir, 'berezhnovka_error_stats.xlsx'), index=False)
    
    print("График ошибок для Бережновки сохранён.")
    print("Статистика ошибок для Бережновки:")
    print(error_stats_df)
else:
    print("Участок Бережновка не найден в данных")

# ---------------------------
# НОВЫЙ АНАЛИЗ: Корреляционная матрица для профилей
# ---------------------------
# Создаем сводную таблицу для профилей
pivot_profiles = df_all.pivot_table(index='Год', columns=['Участок', 'Профиль'], 
                                   values='Расстояние_до_ПН', aggfunc='mean')

# Убираем столбцы с недостаточным количеством данных (меньше 3 наблюдений)
pivot_profiles = pivot_profiles.dropna(axis=1, thresh=3)

if not pivot_profiles.empty and pivot_profiles.shape[1] > 1:
    # Вычисляем корреляционную матрицу
    corr_matrix_profiles = pivot_profiles.corr()
    
    # Создаем график корреляционной матрицы
    plt.figure(figsize=(14, 12))
    mask = np.triu(np.ones_like(corr_matrix_profiles, dtype=bool))  # Маска для верхнего треугольника
    sns.heatmap(corr_matrix_profiles, mask=mask, annot=True, fmt='.2f', cmap='coolwarm', 
                center=0, square=True, linewidths=0.5, cbar_kws={"shrink": .8})
    plt.title('Корреляционная матрица между профилями всех участков', fontsize=14, pad=20)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'profiles_correlation_matrix.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Сохраняем матрицу корреляции в Excel
    corr_matrix_profiles.to_excel(os.path.join(output_dir, 'profiles_correlation_matrix.xlsx'))
    
    print("Корреляционная матрица для профилей сохранена.")
    
    # Анализ сильных корреляций
    strong_correlations = []
    for i in range(len(corr_matrix_profiles.columns)):
        for j in range(i+1, len(corr_matrix_profiles.columns)):
            corr_val = corr_matrix_profiles.iloc[i, j]
            if abs(corr_val) > 0.7:  # Сильная корреляция
                strong_correlations.append({
                    'Профиль_1': corr_matrix_profiles.columns[i],
                    'Профиль_2': corr_matrix_profiles.columns[j],
                    'Корреляция': corr_val
                })
    
    if strong_correlations:
        strong_corr_df = pd.DataFrame(strong_correlations)
        strong_corr_df.to_excel(os.path.join(output_dir, 'strong_profile_correlations.xlsx'), index=False)
        print(f"Найдено {len(strong_correlations)} сильных корреляций между профилями")
else:
    print("Недостаточно данных для построения корреляционной матрицы профилей")

# ---------------------------
# НОВЫЙ АНАЛИЗ: Корреляционная матрица для участков
# ---------------------------
# Создаем сводную таблицу для участков (усредняем по профилям)
pivot_sites = df_all.groupby(['Участок', 'Год'])['Расстояние_до_ПН'].mean().reset_index()
pivot_sites = pivot_sites.pivot_table(index='Год', columns='Участок', values='Расстояние_до_ПН')

# Убираем столбцы с недостаточным количеством данных
pivot_sites = pivot_sites.dropna(axis=1, thresh=3)

if not pivot_sites.empty and pivot_sites.shape[1] > 1:
    # Вычисляем корреляционную матрицу
    corr_matrix_sites = pivot_sites.corr()
    
    # Создаем график корреляционной матрицы
    plt.figure(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr_matrix_sites, dtype=bool))
    sns.heatmap(corr_matrix_sites, mask=mask, annot=True, fmt='.2f', cmap='coolwarm', 
                center=0, square=True, linewidths=0.5, cbar_kws={"shrink": .8})
    plt.title('Корреляционная матрица между участками', fontsize=14, pad=20)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sites_correlation_matrix.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Сохраняем матрицу корреляции в Excel
    corr_matrix_sites.to_excel(os.path.join(output_dir, 'sites_correlation_matrix.xlsx'))
    
    print("Корреляционная матрица для участков сохранена.")
    
    # Анализ сильных корреляций между участками
    strong_site_correlations = []
    for i in range(len(corr_matrix_sites.columns)):
        for j in range(i+1, len(corr_matrix_sites.columns)):
            corr_val = corr_matrix_sites.iloc[i, j]
            if abs(corr_val) > 0.7:  # Сильная корреляция
                strong_site_correlations.append({
                    'Участок_1': corr_matrix_sites.columns[i],
                    'Участок_2': corr_matrix_sites.columns[j],
                    'Корреляция': corr_val
                })
    
    if strong_site_correlations:
        strong_site_corr_df = pd.DataFrame(strong_site_correlations)
        strong_site_corr_df.to_excel(os.path.join(output_dir, 'strong_site_correlations.xlsx'), index=False)
        print(f"Найдено {len(strong_site_correlations)} сильных корреляций между участками")
else:
    print("Недостаточно данных для построения корреляционной матрицы участков")

# ---------------------------
# ОБЩАЯ СТАТИСТИКА ОШИБОК
# ---------------------------
# Вычисляем ошибки для всех данных
df_all_with_errors = df_all.copy()
df_all_with_errors['Ошибка'] = df_all_with_errors['Расстояние_до_ПН'] - y_pred_in
df_all_with_errors['Абсолютная_ошибка'] = np.abs(df_all_with_errors['Ошибка'])

# Статистика ошибок по участкам
error_stats_by_site = df_all_with_errors.groupby('Участок').agg({
    'Ошибка': ['mean', 'std', 'min', 'max'],
    'Абсолютная_ошибка': 'mean',
    'Расстояние_до_ПН': 'count'
}).round(3)

error_stats_by_site.columns = ['Средняя_ошибка', 'Стд_ошибка', 'Мин_ошибка', 'Макс_ошибка', 
                              'Средняя_абс_ошибка', 'Количество_наблюдений']
error_stats_by_site.to_excel(os.path.join(output_dir, 'error_stats_by_site.xlsx'))

print("Общая статистика ошибок по участкам сохранена.")
print("\nСтатистика ошибок по участкам:")
print(error_stats_by_site)

# ---------------------------
# СОХРАНЕНИЕ ОТЧЕТА
# ---------------------------
report = f"""
ОТЧЕТ ПО АНАЛИЗУ МОДЕЛИ
========================
Дата анализа: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

ОБЩИЕ МЕТРИКИ МОДЕЛИ:
---------------------
RMSE: {metrics_df.loc['In-sample', 'RMSE']:.2f}
MAE: {metrics_df.loc['In-sample', 'MAE']:.2f}
R²: {metrics_df.loc['In-sample', 'R2']:.3f}

СТАТИСТИКА ДАННЫХ:
------------------
Количество наблюдений: {len(df_all)}
Количество участков: {df_all['Участок'].nunique()}
Количество профилей: {df_all['Профиль'].nunique()}
Период данных: {df_all['Год'].min()} - {df_all['Год'].max()}

РАСПРЕДЕЛЕНИЕ ПО ЛИТОЛОГИИ:
---------------------------
{df_all['Литология_класс'].value_counts().to_string()}

СТАТИСТИКА ОШИБОК:
------------------
Средняя абсолютная ошибка по всем данным: {df_all_with_errors['Абсолютная_ошибка'].mean():.2f} м
Стандартное отклонение ошибок: {df_all_with_errors['Ошибка'].std():.2f} м
"""

with open(os.path.join(output_dir, "analysis_report.txt"), "w", encoding='utf-8') as f:
    f.write(report)

print("="*60)
print("АНАЛИЗ ЗАВЕРШЕН!")
print(f"Все результаты сохранены в: {output_dir}")
print("="*60)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Полный скрипт:
- читает все листы из Excel с профилями (240716 БРОВКИ Баранова.xls)
- на каждом листе автоматически находит блоки профилей и извлекает даты + расстояния до ПН
- строит временные ряды и scatter для пар профилей, считает корреляцию Пирсона + 95% CI
- вычисляет среднюю скорость отступания (м/год) по листу (по среднему значениям профилей)
- загружает таблицу литологии из /mnt/data/Ориентация берега участки.xlsx
- связывает литологию с рассчитанными скоростями, сохраняет результаты и строит boxplot/barplot
- сохраняет все графики в папку plots/
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from itertools import combinations
from scipy.stats import pearsonr, norm
import os
import re
import warnings
warnings.simplefilter("ignore")

# -----------------------------
# Настройки файлов и папок
# -----------------------------
profiles_file = "БРОВКИ.xls"   # твой файл с профилями
lithology_file = "Ориентация берега участки.xlsx"  # загруженный файл литологии
output_excel = "результаты_корреляции_и_литология.xlsx"
plots_dir = "plots"
os.makedirs(plots_dir, exist_ok=True)
os.makedirs("plots_lithology", exist_ok=True)

# -----------------------------
# Утилиты
# -----------------------------
def corr_confidence_interval(r, n, confidence=0.95):
    """
    95% CI для корреляции Пирсона через Fisher z-преобразование.
    Возвращает (lo, hi)
    """
    if np.isnan(r) or n < 4:
        return (np.nan, np.nan)
    z = np.arctanh(r)
    se = 1/np.sqrt(n - 3)
    z_crit = norm.ppf(1 - (1 - confidence) / 2)
    lo_z, hi_z = z - z_crit * se, z + z_crit * se
    lo, hi = np.tanh([lo_z, hi_z])
    return (lo, hi)

# Копия функций детекции/извлечения из твоего скрипта (с небольшими улучшениями)
def detect_profiles(df):
    """
    Автоматически определяет блоки столбцов для профилей
    на основе поиска слова 'ПРОФИЛЬ' или 'ПРОФИЛЬ №' в заголовках (первая строка/названия колонок).
    Возвращает список (profile_name, start_col, end_col).
    """
    profiles = []
    # Ищем все столбцы, где имя колонки содержит "ПРОФИЛЬ" или в первой строке содержится "ПРОФИЛЬ"
    profile_columns = []
    # Проверяем названия столбцов
    for i, col in enumerate(df.columns):
        col_str = str(col)
        if re.search(r'ПРОФИЛЬ\s*№?\s*\d+', col_str, re.IGNORECASE):
            profile_columns.append(i)
    # Также проверим первую строку (в некоторых файлах заголовки в первой строке)
    if not profile_columns and len(df) > 0:
        first_row = df.iloc[0].astype(str).tolist()
        for i, cell in enumerate(first_row):
            if isinstance(cell, str) and re.search(r'ПРОФИЛЬ\s*№?\s*\d+', cell, re.IGNORECASE):
                profile_columns.append(i)
    if profile_columns:
        profile_columns = sorted(set(profile_columns))
        profile_columns.append(len(df.columns))
        for i in range(len(profile_columns)-1):
            start_col = profile_columns[i]
            end_col = profile_columns[i+1] - 1
            # Попытка извлечь реальный номер профиля из текста заголовка
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

def extract_profile_data(df, start_col, end_col, profile_name, header_rows_to_skip=3):
    """
    Извлекает столбцы блока для профиля, пропуская первые header_rows_to_skip строк заголовка.
    Пытается определить столбец с датой и столбец с расстоянием до бровки/ПН.
    Возвращает DataFrame с колонками ['Дата','Расстояние_до_ПН'] или None.
    """
    # Берём блок данных
    profile_block = df.iloc[header_rows_to_skip:, start_col:end_col+1].copy()
    profile_block = profile_block.reset_index(drop=True)
    if profile_block.empty:
        return None
    # Подберём имена: попробуем стандартную структуру (6 столбцов)
    num_columns = profile_block.shape[1]
    # Переименуем временно колонки в generic
    profile_block.columns = [f"c{i}" for i in range(num_columns)]
    # Ищем столбец с датами (первый, где значения похожи на даты)
    date_col = None
    for col in profile_block.columns[:min(3, num_columns)]:  # чаще дата в первых 3 столбцах
        # если хотя бы 30% значений успешно парсятся в даты — считаем колонку датой
        parsed = pd.to_datetime(profile_block[col], errors='coerce')
        if parsed.notna().sum() >= max(1, int(0.3 * len(parsed))):
            date_col = col
            profile_block[col] = parsed
            break
    if date_col is None:
        # попробуем первый столбец в качестве даты
        try:
            profile_block['c0'] = pd.to_datetime(profile_block['c0'], errors='coerce')
            if profile_block['c0'].notna().sum() > 0:
                date_col = 'c0'
        except Exception:
            date_col = None
    # Ищем столбец с расстоянием (числовой)
    dist_col = None
    # Ищем последний числовой столбец в блоке
    for col in reversed(profile_block.columns):
        # проверим, есть ли числовые значения
        if pd.to_numeric(profile_block[col], errors='coerce').notna().any():
            # но исключаем колонку с датой, если случайно парсится числом
            if col != date_col:
                dist_col = col
                break
    # Если не найден, пробуем второй столбец
    if dist_col is None and num_columns >= 2:
        dist_col = profile_block.columns[1]
    # Преобразуем найденные колонки
    if date_col is None or dist_col is None:
        # если не удалось найти и дату, и расстояние — возвращаем None
        return None
    profile_block['Дата'] = pd.to_datetime(profile_block[date_col], errors='coerce')
    profile_block['Расстояние_до_ПН'] = pd.to_numeric(profile_block[dist_col], errors='coerce')
    result = profile_block[['Дата', 'Расстояние_до_ПН']].dropna(subset=['Дата','Расстояние_до_ПН'])
    if result.empty:
        return None
    # Обрезаем строки без дат
    result = result.reset_index(drop=True)
    return result

# -----------------------------
# Загрузка литологии
# -----------------------------
def load_lithology_table(lithology_path):
    """
    Загружает таблицу литологии и нормализует названия.
    Ожидаются колонки (возможно с похожими именами): 'Название участка', 'Литологический состав берега'
    """
    try:
        litho = pd.read_excel(lithology_path)
    except Exception as e:
        print(f"Ошибка чтения файла литологии: {e}")
        return None
    # Попытка найти нужные колонки
    cols_low = [c.lower() for c in litho.columns]
    # Ищем колонку с названием участка
    name_col = None
    lit_col = None
    for c, cl in zip(litho.columns, cols_low):
        if 'назван' in cl and 'участ' in cl:
            name_col = c
        if 'литолог' in cl or 'литол' in cl or 'состав' in cl:
            lit_col = c
    # Если не нашли очевидные, берём первые две
    if name_col is None:
        name_col = litho.columns[0]
    if lit_col is None and len(litho.columns) > 1:
        lit_col = litho.columns[-1]
    # Нормализация названий
    litho = litho[[name_col, lit_col]].rename(columns={name_col: 'Название_участка', lit_col: 'Литология_описание'})
    litho['Название_участка'] = litho['Название_участка'].astype(str).str.strip()
    litho['Литология_описание'] = litho['Литология_описание'].astype(str).str.strip()
    # Простейшая классификация литологии в несколько классов
    def classify(text):
        t = text.lower()
        if any(x in t for x in ['опок', 'песчаник', 'кремнист']):
            return 'Твёрдые породы'
        if any(x in t for x in ['глин', 'хвалынск']):
            return 'Глины'
        if 'суглин' in t:
            return 'Суглинки'
        if any(x in t for x in ['супес', 'песк', 'песок']):
            return 'Пески/супеси'
        return 'Другое/смешанное'
    litho['Литология_класс'] = litho['Литология_описание'].apply(classify)
    return litho

# -----------------------------
# Основной процесс обработки всех листов
# -----------------------------
results = []           # корреляции по парам профилей (как раньше)
retreat_speed_results = []   # скорость отступания по участку (листу)

# Загружаем литологию (если файл доступен)
litho_df = load_lithology_table(lithology_file)
if litho_df is None:
    print("Внимание: файл литологии не загружен. Анализ литологии пропущен.")
else:
    print(f"Загружено записей литологии: {len(litho_df)}")

# Читаем все листы из файла профилей
try:
    xls = pd.ExcelFile(profiles_file)
    sheet_names = xls.sheet_names
except Exception as e:
    print(f"Ошибка при открытии файла профилей ({profiles_file}): {e}")
    raise SystemExit(1)

processed_sheets = 0
for sheet in sheet_names:
    print(f"\n📄 Обработка листа: {sheet}")
    try:
        # Читаем лист без жесткого header — заголовки могут быть в первых строках
        raw = pd.read_excel(profiles_file, sheet_name=sheet, header=None)
        if raw.empty:
            print(f"   ⚠️ Лист {sheet} пуст, пропускаем")
            continue
        # Попробуем найти строку, где указаны названия колонок (строка с 'Дата' или 'ПРОФИЛЬ')
        header_row_idx = 0
        # ищем строку, где есть слово 'Дата' или 'ПРОФИЛЬ'
        for i in range(min(5, len(raw))):
            row_str = " ".join([str(x) for x in raw.iloc[i].values]).lower()
            if 'дата' in row_str or 'профиль' in row_str:
                header_row_idx = i
                break
        # Читаем с найденным header_row_idx как header (чтобы получить корректные имена колонок)
        df = pd.read_excel(profiles_file, sheet_name=sheet, header=header_row_idx)
        if df.empty:
            print(f"   ⚠️ Лист {sheet} после обработки заголовка пуст, пропускаем")
            continue
        # Детекция блоков профилей
        profiles_info = detect_profiles(df)
        if not profiles_info:
            # Если не найдено "ПРОФИЛЬ" в именах колонок, попробуем разбить равными блоками:
            ncols = df.shape[1]
            # предположим три блока если достаточно колонок
            if ncols >= 6:
                block = ncols // 3
                profiles_info = []
                for i in range(3):
                    start = i*block
                    end = (i+1)*block - 1 if i < 2 else ncols-1
                    profiles_info.append((f"Профиль_{i+1}", start, end))
                print(f"   Авторазбиение на блоки: {profiles_info}")
            else:
                print(f"   ⚠️ Не удаётся определить профили на листе {sheet}, пропускаем")
                continue
        # Извлекаем данные по каждому профилю
        profile_data = {}
        for profile_name, start_col, end_col in profiles_info:
            data = extract_profile_data(df, start_col, end_col, profile_name, header_rows_to_skip=header_row_idx+1)
            if data is not None and len(data) > 0:
                profile_data[profile_name] = data
        if len(profile_data) < 2:
            print(f"   ⚠️ Недостаточно профилей с данными на листе {sheet} (найдено {len(profile_data)})")
            continue
        # Создаём pivot (год -> значения профилей)
        pivot_frames = {}
        for pname, pdata in profile_data.items():
            tmp = pdata.copy()
            tmp['Год'] = tmp['Дата'].dt.year
            yearly = tmp.groupby('Год')['Расстояние_до_ПН'].mean().reset_index()
            pivot_frames[pname] = yearly
        # объединяем по годам
        years_union = sorted({y for fr in pivot_frames.values() for y in fr['Год'].tolist()})
        pivot = pd.DataFrame({'Год': years_union})
        for pname, fr in pivot_frames.items():
            pivot = pivot.merge(fr, on='Год', how='left').rename(columns={'Расстояние_до_ПН': pname})
        pivot = pivot.set_index('Год')
        # фильтруем профили с слишком малым количеством данных
        pivot = pivot.loc[:, pivot.notna().sum() >= 3]
        if pivot.shape[1] < 2:
            print(f"   ⚠️ Недостаточно годовых рядов (после фильтра) на листе {sheet}")
            continue
        # ---- Рассчёт средней кривой по листу и скорость отступания ----
        pivot['Среднее_профили'] = pivot.mean(axis=1)
        valid = pivot['Среднее_профили'].dropna()
        retreat_speed = np.nan
        if len(valid) >= 3:
            years_arr = valid.index.values.astype(float)
            vals = valid.values.astype(float)
            slope, intercept = np.polyfit(years_arr, vals, 1)  # наклон (м/год)
            retreat_speed = float(slope)
            print(f"   -> Скорость отступания по листу {sheet}: {retreat_speed:.4f} (м/год)")
        else:
            print(f"   -> Недостаточно точек для расчёта скорости на листе {sheet}")
        retreat_speed_results.append({'Участок': sheet, 'Скорость_отступания': retreat_speed})
        processed_sheets += 1
        # ---- Временной ряд (по профилям) ----
        plt.figure(figsize=(12,6))
        for col in pivot.columns:
            if col == 'Среднее_профили':
                continue
            ser = pivot[col].dropna()
            if ser.empty:
                continue
            plt.plot(ser.index, ser.values, marker='o', label=col, linewidth=2, markersize=5)
        plt.title(f"Изменение расстояния до ПН во времени ({sheet})", fontsize=14)
        plt.xlabel("Год")
        plt.ylabel("Расстояние до ПН, м")
        plt.grid(True, alpha=0.3)
        plt.legend(loc='best', fontsize=9)
        plt.tight_layout()
        safe_sheet = re.sub(r'[\\/*?:"<>|]', "_", sheet)
        plt.savefig(f"{plots_dir}/{safe_sheet}_time_series.png", dpi=200, bbox_inches='tight')
        plt.close()
        # ---- Корреляции и scatter по парам профилей ----
        found_corr = False
        for p1, p2 in combinations([c for c in pivot.columns if c!='Среднее_профили'], 2):
            sub = pivot[[p1, p2]].dropna()
            if len(sub) >= 4:
                r, pval = pearsonr(sub[p1], sub[p2])
                ci_lo, ci_hi = corr_confidence_interval(r, len(sub))
                results.append({
                    'Участок': sheet,
                    'Профиль 1': p1,
                    'Профиль 2': p2,
                    'r': round(r, 3),
                    'p-value': float(pval),
                    '95% CI low': round(ci_lo, 3) if not np.isnan(ci_lo) else np.nan,
                    '95% CI high': round(ci_hi, 3) if not np.isnan(ci_hi) else np.nan,
                    'N': len(sub)
                })
                found_corr = True
                # scatter
                plt.figure(figsize=(7,6))
                plt.scatter(sub[p1], sub[p2], s=60, alpha=0.7, label='Расстояние от ПН, м')
                if len(sub) > 1:
                    z = np.polyfit(sub[p1], sub[p2], 1)
                    pfun = np.poly1d(z)
                    xs = np.linspace(sub[p1].min(), sub[p1].max(), 50)
                    # Добавляем подпись для линии регрессии
                    regression_line = plt.plot(xs, pfun(xs), 'r--', linewidth=1.2, label='Линия регрессии')
                plt.title(f"{sheet}: {p1} vs {p2}\nr={r:.3f}, p={pval:.4f}")
                plt.xlabel(p1)
                plt.ylabel(p2)
                plt.grid(True, alpha=0.3)
                # Добавляем легенду с уравнением регрессии
                plt.legend(loc='lower right')
                fname = f"{plots_dir}/{safe_sheet}_{re.sub(r'[\\\\/*?:\"<>|]', '_', p1)}_vs_{re.sub(r'[\\\\/*?:\"<>|]', '_', p2)}.png"
                plt.tight_layout()
                plt.savefig(fname, dpi=200, bbox_inches='tight')
                
                plt.close()
        if found_corr:
            print(f"   ✅ Корреляции рассчитаны и графики сохранены для листа {sheet}")
        else:
            print(f"   ⚠️ Не найдено достаточных пар для корреляции на листе {sheet}")
    except Exception as e:
        print(f"   ❌ Ошибка при обработке листа {sheet}: {e}")
        continue

# -----------------------------
# После обработки всех листов: сохраняем результаты корреляций
# -----------------------------
if results:
    results_df = pd.DataFrame(results)
    results_df.to_excel(output_excel, index=False)
    print(f"\n✅ Результаты корреляций сохранены в: {output_excel}")
else:
    print("\nℹ️ Не найдено результатов корреляции для сохранения.")

# -----------------------------
# Анализ литологии vs скорость
# -----------------------------
if retreat_speed_results and litho_df is not None:
    retreat_df = pd.DataFrame(retreat_speed_results)
    retreat_df['Участок'] = retreat_df['Участок'].astype(str).str.strip()
    # Подготовим litho_df (нормализация названий)
    litho_df['Название_участка_norm'] = litho_df['Название_участка'].astype(str).str.strip().str.lower()
    retreat_df['Участок_norm'] = retreat_df['Участок'].astype(str).str.strip().str.lower()
    merged = retreat_df.merge(litho_df, left_on='Участок_norm', right_on='Название_участка_norm', how='left')
    
    # Делаем скорость отступания положительной
    merged['Скорость_отступания_положительная'] = abs(merged['Скорость_отступания'])
    
    # Сохраняем объединённую таблицу
    merged_out = merged[['Участок', 'Скорость_отступания', 'Скорость_отступания_положительная', 'Литология_описание', 'Литология_класс']].rename(
        columns={'Литология_описание': 'Литология_описание', 'Литология_класс': 'Литология_класс'}
    )
    merged_out.to_excel("литология_vs_отступание.xlsx", index=False)
    print("\n✅ Сохранён файл литология_vs_отступание.xlsx")
    
    # Построим boxplot по классам литологии
    valid_for_plot = merged_out.dropna(subset=['Литология_класс', 'Скорость_отступания_положительная'])
    if not valid_for_plot.empty:
        plt.figure(figsize=(10,6))
        # boxplot с использованием pandas
        valid_for_plot.boxplot(column='Скорость_отступания_положительная', by='Литология_класс', grid=True)
        plt.title("Скорость отступания по литологии (класс)")
        plt.suptitle("")
        plt.xlabel("")
        plt.ylabel("Скорость отступания (м/год)")
        plt.xticks(rotation=25)
        plt.tight_layout()
        plt.savefig("plots_lithology/boxplot_lithology_vs_retreat.png", dpi=200, bbox_inches='tight')
        plt.close()
        
        # bar plot средних
        plt.figure(figsize=(10,6))
        mean_vals = valid_for_plot.groupby('Литология_класс')['Скорость_отступания_положительная'].mean().sort_values(ascending=False)
        mean_vals.plot(kind='bar')
        plt.title("Средняя скорость отступания по литологии (класс)")
        plt.ylabel("Скорость отступания (м/год)")
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=25)
        plt.tight_layout()
        plt.savefig("plots_lithology/bar_lithology_vs_retreat.png", dpi=200, bbox_inches='tight')
        plt.close()
        print("✅ Построены графики по литологии (папка plots_lithology)")
    else:
        print("⚠️ Для построения графиков по литологии недостаточно данных (после объединения).")
else:
    print("\nℹ️ Пропущен блок анализа литологии (нет данных или файл литологии не загружен).")

print(f"\nГотово. Обработано листов: {processed_sheets}. Графики в папках: {plots_dir}, plots_lithology.")




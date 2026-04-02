import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr, t, norm
from itertools import combinations

# -------------------------------
# 1️⃣ Чтение данных из Excel
# -------------------------------
file_path = "profiles_ipn.xlsx"
df = pd.read_excel(file_path)

# Убедимся, что данные отсортированы по времени
df = df.sort_values("Data").reset_index(drop=True)

# -------------------------------
# 2️⃣ Корреляционная матрица
# -------------------------------
profiles = ['prof_60', 'prof_61', 'prof_62']
corr_matrix = df[profiles].corr()
print("\n📈 Корреляционная матрица:")
print(corr_matrix)

# -------------------------------
# 3️⃣ Доверительные интервалы для корреляции
# -------------------------------
def correlation_ci(x, y, confidence=0.95):
    # Удаляем пары, где есть хотя бы один NaN
    mask = (~pd.isna(x)) & (~pd.isna(y))
    x = np.array(x[mask])
    y = np.array(y[mask])
    n = len(x)

    if n < 3:
        return np.nan, np.nan, np.nan, np.nan  # слишком мало точек

    # Корреляция Пирсона
    r, p = pearsonr(x, y)

    # Fisher z-преобразование
    z = np.arctanh(r)
    se = 1 / np.sqrt(n - 3)

    # Критическое значение из нормального распределения (классический вариант)
    z_crit = norm.ppf(1 - (1 - confidence) / 2)

    lo_z, hi_z = z - z_crit * se, z + z_crit * se
    lo, hi = np.tanh([lo_z, hi_z])
    return r, lo, hi, p

print("\n📊 Доверительные интервалы для коэффициентов корреляции:")
for p1, p2 in combinations(profiles, 2):
    r, lo, hi, p = correlation_ci(df[p1], df[p2])
    print(f"{p1} vs {p2}: r = {r:.3f}, 95% CI = [{lo:.3f}, {hi:.3f}], p = {p:.3e}")

# -------------------------------
# 4️⃣ Scatter plots (графики рассеяния)
# -------------------------------
sns.set(style="whitegrid", font_scale=1.1)
plt.figure(figsize=(12,4))
for i, (p1, p2) in enumerate(combinations(profiles, 2), 1):
    plt.subplot(1,3,i)
    sns.regplot(x=df[p1], y=df[p2], ci=None, scatter_kws={'s':40})
    plt.xlabel(p1)
    plt.ylabel(p2)
    plt.title(f"{p1} vs {p2}")
plt.suptitle("Scatter-plots между профилями", fontsize=14)
plt.tight_layout()
plt.show()

# -------------------------------
# 5️⃣ Кросс-корреляция (лаговая)
# -------------------------------
def cross_corr_with_lags(x, y, max_lag=5):
    """Возвращает массив корреляций с лагом от -max_lag до +max_lag"""
    lags = range(-max_lag, max_lag+1)
    corr_values = [np.corrcoef(x[max(0, lag):len(x)+min(0, lag)],
                               y[max(0, -lag):len(y)-max(0, lag)])[0,1]
                   for lag in lags]
    return lags, corr_values

plt.figure(figsize=(8,6))
for (p1, p2) in combinations(profiles, 2):
    lags, corr_vals = cross_corr_with_lags(df[p1].values, df[p2].values, max_lag=5)
    plt.plot(lags, corr_vals, marker='o', label=f"{p1} vs {p2}")
plt.axhline(0, color='black', lw=0.8)
plt.axvline(0, color='gray', ls='--')
plt.title("Кросс-корреляция с лагами")
plt.xlabel("Лаг (лет)")
plt.ylabel("Коэффициент корреляции")
plt.legend()
plt.grid(True)
plt.show()

# -------------------------------
# 6️⃣ Тепловая карта корреляции
# -------------------------------
plt.figure(figsize=(5,4))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1)
plt.title("Матрица корреляций между профилями")
plt.tight_layout()
plt.show()




# === 7️⃣ Временной ряд (динамика по годам) ===
plt.figure(figsize=(10, 6))

for profile in profiles:
    plt.plot(df['Data'], df[profile], marker='o', label=profile)

plt.title('Динамика изменения параметра ОБ за период по профилям')
plt.xlabel('Год измерения')
plt.ylabel('ОБ за период, м')
plt.legend(title='Профиль', loc='best')
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()


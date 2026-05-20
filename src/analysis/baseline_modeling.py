"""Базовые регрессионные модели для подготовленного датасета береговых наблюдений."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GroupShuffleSplit, KFold, cross_validate, train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.parsers.common import PROCESSED_DIR, REPORTS_DIR, relative_to_root, setup_logging

RANDOM_STATE = 42
TARGET_COLUMN = "retreat_rate_abs_m_per_year"
TARGET_DECISION_NOTE = (
    "Выбрана целевая переменная `retreat_rate_abs_m_per_year`: это беззнаковая интенсивность "
    "изменения береговой бровки, нормированная на длительность интервала. "
    "Такая целевая переменная безопаснее для базовой регрессии, чем знаковые метрики, потому "
    "что знак смещения требует отдельной доменной интерпретации."
)
TARGET_CANDIDATES = [
    TARGET_COLUMN,
    "retreat_abs_m",
    "retreat_rate_m_per_year",
    "retreat_m",
]
LEAKAGE_COLUMNS = {
    "retreat_m",
    "retreat_rate_m_per_year",
    "retreat_abs_m",
    "retreat_rate_abs_m_per_year",
    "days_between",
    "years_between",
}
ID_OR_DISPLAY_COLUMNS = {
    "interval_id",
    "site_id",
    "site_name",
    "profile_id",
    "profile_num",
    "profile_name",
    "date_start",
    "date_end",
}
TEXT_SERVICE_COLUMNS = {
    "qc_flag",
    "qc_note",
    "qc_flag_analysis",
    "qc_note_analysis",
    "qc_flag_analysis_safe",
    "qc_note_analysis_safe",
    "notes",
    "scope_note",
    "review_reason",
    "duplicate_conflict_note",
    "duplicate_conflict_obs_dates",
}
PREFERRED_CATEGORICAL_FEATURES = {
    "shore_type",
    "shore_orientation_text",
    "exposure_sectors_text",
    "lithology_text",
    "lithology_class",
    "water_context_scope",
    "water_time_resolution",
    "history_start_group",
}
PREFERRED_NUMERIC_FEATURES = {
    "shore_orientation_deg",
    "n_water_obs",
    "coverage_water",
    "mean_water_level_mean_annual_m_abs",
    "max_water_level_mean_annual_m_abs",
    "min_water_level_mean_annual_m_abs",
    "range_water_level_mean_annual_m_abs",
    "mean_water_level_max_annual_m_abs",
    "max_water_level_max_annual_m_abs",
    "min_water_level_max_annual_m_abs",
    "range_water_level_max_annual_m_abs",
    "history_start_year",
    "has_conflicting_shoreline_duplicates",
    "conflicting_duplicate_group_count",
    "interval_mid_year",
}
METADATA_COLUMNS_FOR_ERRORS = [
    "interval_id",
    "site_id",
    "profile_id",
    "date_start",
    "date_end",
]
GROUP_VALIDATION_CANDIDATES = ["profile_id", "site_id"]


def _one_hot_encoder() -> OneHotEncoder:
    """Build a version-compatible one-hot encoder."""

    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def choose_target(df: pd.DataFrame) -> str:
    """Choose the regression target from documented shoreline metrics."""

    for column in TARGET_CANDIDATES:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if len(values) >= 10 and values.nunique() > 1:
            return column
    raise ValueError(
        "Не найдена подходящая целевая переменная для регрессии. Ожидалась одна из колонок: "
        + ", ".join(TARGET_CANDIDATES)
    )


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add conservative date-derived features without using interval length."""

    result = df.copy()
    if {"date_start", "date_end"}.issubset(result.columns):
        start = pd.to_datetime(result["date_start"], errors="coerce")
        end = pd.to_datetime(result["date_end"], errors="coerce")
        midpoint = start + (end - start) / 2
        result["interval_mid_year"] = midpoint.dt.year + (midpoint.dt.dayofyear - 1) / 365.25
    return result


def build_feature_frame(
    df: pd.DataFrame,
    target: str,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, list[str], list[str], list[str]]:
    """Return X/y, row metadata, and selected numeric/categorical features."""

    model_df = add_derived_features(df)
    y = pd.to_numeric(model_df[target], errors="coerce")
    valid_target = y.notna()
    model_df = model_df.loc[valid_target].reset_index(drop=True)
    y = y.loc[valid_target].reset_index(drop=True)

    excluded = set(LEAKAGE_COLUMNS)
    excluded.update(ID_OR_DISPLAY_COLUMNS)
    excluded.update(TEXT_SERVICE_COLUMNS)
    excluded.add(target)

    candidate_features = [column for column in model_df.columns if column not in excluded]
    numeric_features: list[str] = []
    categorical_features: list[str] = []

    for column in candidate_features:
        if column in PREFERRED_NUMERIC_FEATURES:
            numeric_features.append(column)
        elif column in PREFERRED_CATEGORICAL_FEATURES:
            categorical_features.append(column)

    X = model_df[numeric_features + categorical_features].copy()
    for column in numeric_features:
        X[column] = pd.to_numeric(X[column], errors="coerce")
    for column in categorical_features:
        X[column] = X[column].astype(object).where(X[column].notna(), np.nan)

    return X, y, model_df, numeric_features, categorical_features, sorted(excluded)


def build_preprocessor(numeric_features: list[str], categorical_features: list[str]) -> ColumnTransformer:
    """Build preprocessing that is fitted only inside sklearn pipelines."""

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", _one_hot_encoder()),
        ]
    )
    transformers = []
    if numeric_features:
        transformers.append(("numeric", numeric_pipeline, numeric_features))
    if categorical_features:
        transformers.append(("categorical", categorical_pipeline, categorical_features))
    if not transformers:
        raise ValueError("После исключения утечек и служебных колонок не осталось пригодных признаков.")
    return ColumnTransformer(transformers=transformers, remainder="drop", verbose_feature_names_out=False)


def build_pipeline(
    model: object,
    numeric_features: list[str],
    categorical_features: list[str],
) -> Pipeline:
    """Build a fresh preprocessing/model pipeline for one estimator."""

    return Pipeline(
        steps=[
            ("preprocess", build_preprocessor(numeric_features, categorical_features)),
            ("model", model),
        ]
    )


def build_models(numeric_features: list[str], categorical_features: list[str]) -> dict[str, Pipeline]:
    """Build the baseline model set."""

    return {
        "DummyRegressor_median": build_pipeline(
            DummyRegressor(strategy="median"),
            numeric_features,
            categorical_features,
        ),
        "Ridge": build_pipeline(
            Ridge(alpha=1.0),
            numeric_features,
            categorical_features,
        ),
        "RandomForestRegressor": build_pipeline(
            RandomForestRegressor(
                n_estimators=300,
                min_samples_leaf=5,
                random_state=RANDOM_STATE,
                n_jobs=-1,
            ),
            numeric_features,
            categorical_features,
        ),
        "HistGradientBoostingRegressor": build_pipeline(
            HistGradientBoostingRegressor(
                max_iter=300,
                learning_rate=0.05,
                l2_regularization=0.01,
                random_state=RANDOM_STATE,
            ),
            numeric_features,
            categorical_features,
        ),
        "MLPRegressor": build_pipeline(
            MLPRegressor(
                hidden_layer_sizes=(32, 16),
                activation="relu",
                solver="adam",
                alpha=0.001,
                learning_rate_init=0.001,
                max_iter=700,
                early_stopping=True,
                n_iter_no_change=20,
                random_state=RANDOM_STATE,
            ),
            numeric_features,
            categorical_features,
        ),
    }


def _rmse(y_true: pd.Series | np.ndarray, y_pred: np.ndarray) -> float:
    return math.sqrt(mean_squared_error(y_true, y_pred))


def _cv_folds(n_rows: int) -> int | None:
    if n_rows < 10:
        return None
    if n_rows < 50:
        return max(2, min(5, n_rows // 5))
    return 5


def evaluate_models(
    X: pd.DataFrame,
    y: pd.Series,
    metadata: pd.DataFrame,
    models: dict[str, Pipeline],
    target: str,
) -> tuple[pd.DataFrame, Pipeline, str, pd.DataFrame, pd.Series, pd.DataFrame, np.ndarray]:
    """Fit/evaluate each model and return metrics plus the best fitted pipeline."""

    if len(X) < 10:
        raise ValueError(f"Недостаточно строк для разбиения на обучение и тест: {len(X)}")

    X_train, X_test, y_train, y_test, metadata_train, metadata_test = train_test_split(
        X,
        y,
        metadata,
        test_size=0.2,
        random_state=RANDOM_STATE,
    )
    folds = _cv_folds(len(X_train))
    rows = []
    fitted_models: dict[str, Pipeline] = {}

    scoring = {
        "mae": "neg_mean_absolute_error",
        "mse": "neg_mean_squared_error",
        "r2": "r2",
    }

    for model_name, pipeline in models.items():
        cv_mae_mean = np.nan
        cv_mae_std = np.nan
        cv_rmse_mean = np.nan
        cv_rmse_std = np.nan
        cv_r2_mean = np.nan
        cv_r2_std = np.nan
        if folds is not None:
            cv = KFold(n_splits=folds, shuffle=True, random_state=RANDOM_STATE)
            cv_scores = cross_validate(pipeline, X_train, y_train, cv=cv, scoring=scoring, n_jobs=None)
            cv_mae = -cv_scores["test_mae"]
            cv_rmse = np.sqrt(-cv_scores["test_mse"])
            cv_r2 = cv_scores["test_r2"]
            cv_mae_mean = float(np.mean(cv_mae))
            cv_mae_std = float(np.std(cv_mae))
            cv_rmse_mean = float(np.mean(cv_rmse))
            cv_rmse_std = float(np.std(cv_rmse))
            cv_r2_mean = float(np.mean(cv_r2))
            cv_r2_std = float(np.std(cv_r2))

        fitted = pipeline.fit(X_train, y_train)
        y_pred = fitted.predict(X_test)
        fitted_models[model_name] = fitted
        rows.append(
            {
                "target": target,
                "validation_scheme": "случайное разбиение на обучение и тест",
                "model": model_name,
                "n_rows": len(X),
                "n_train": len(X_train),
                "n_test": len(X_test),
                "n_features_raw": X.shape[1],
                "cv_folds": folds if folds is not None else 0,
                "cv_mae_mean": cv_mae_mean,
                "cv_mae_std": cv_mae_std,
                "cv_rmse_mean": cv_rmse_mean,
                "cv_rmse_std": cv_rmse_std,
                "cv_r2_mean": cv_r2_mean,
                "cv_r2_std": cv_r2_std,
                "test_mae": float(mean_absolute_error(y_test, y_pred)),
                "test_rmse": float(_rmse(y_test, y_pred)),
                "test_r2": float(r2_score(y_test, y_pred)),
                "target_decision_note": TARGET_DECISION_NOTE,
            }
        )

    metrics = pd.DataFrame(rows).sort_values(["test_mae", "test_rmse"], ascending=[True, True]).reset_index(drop=True)
    best_name = str(metrics.loc[0, "model"])
    metrics["is_best_by_test_mae"] = metrics["model"].eq(best_name)
    best_predictions = fitted_models[best_name].predict(X_test)
    return metrics, fitted_models[best_name], best_name, X_test, y_test, metadata_test, best_predictions


def choose_group_column(metadata: pd.DataFrame) -> str | None:
    """Choose the strongest available group column for grouped validation."""

    for column in GROUP_VALIDATION_CANDIDATES:
        if column in metadata.columns and metadata[column].notna().any():
            return column
    return None


def evaluate_group_validation(
    X: pd.DataFrame,
    y: pd.Series,
    metadata: pd.DataFrame,
    models: dict[str, Pipeline],
) -> tuple[pd.DataFrame, str | None]:
    """Evaluate generalization to unseen profiles/sites."""

    columns = ["model", "group_column", "n_groups", "n_train", "n_test", "MAE", "RMSE", "R2"]
    group_column = choose_group_column(metadata)
    if group_column is None:
        return pd.DataFrame(columns=columns), "Групповая проверка не выполнена: нет колонки `profile_id` или `site_id`."

    groups = metadata[group_column].astype(object).where(metadata[group_column].notna(), "__missing_group__").astype(str)
    n_groups = int(groups.nunique())
    if n_groups < 2:
        return pd.DataFrame(columns=columns), f"Групповая проверка не выполнена: в `{group_column}` меньше двух групп."

    splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=RANDOM_STATE)
    train_index, test_index = next(splitter.split(X, y, groups=groups))
    X_train = X.iloc[train_index]
    X_test = X.iloc[test_index]
    y_train = y.iloc[train_index]
    y_test = y.iloc[test_index]

    rows = []
    for model_name, pipeline in models.items():
        fitted = pipeline.fit(X_train, y_train)
        y_pred = fitted.predict(X_test)
        rows.append(
            {
                "model": model_name,
                "group_column": group_column,
                "n_groups": n_groups,
                "n_train": len(X_train),
                "n_test": len(X_test),
                "MAE": float(mean_absolute_error(y_test, y_pred)),
                "RMSE": float(_rmse(y_test, y_pred)),
                "R2": float(r2_score(y_test, y_pred)),
            }
        )
    group_metrics = pd.DataFrame(rows).sort_values(["MAE", "RMSE"], ascending=[True, True]).reset_index(drop=True)
    return group_metrics, None


def compute_permutation_importance(
    model: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    best_model_name: str,
    target: str,
) -> pd.DataFrame:
    """Compute raw-column permutation importance for the selected model."""

    if X_test.empty or X_test.shape[1] == 0:
        return pd.DataFrame(
            columns=["rank", "target", "model", "feature_name", "importance_mean", "importance_std"]
        )

    result = permutation_importance(
        model,
        X_test,
        y_test,
        scoring="neg_mean_absolute_error",
        n_repeats=20,
        random_state=RANDOM_STATE,
        n_jobs=None,
    )
    importance = pd.DataFrame(
        {
            "target": target,
            "model": best_model_name,
            "feature_name": X_test.columns,
            "importance_mean": result.importances_mean,
            "importance_std": result.importances_std,
        }
    ).sort_values("importance_mean", ascending=False, key=lambda value: value.fillna(-np.inf))
    importance.insert(0, "rank", range(1, len(importance) + 1))
    return importance.reset_index(drop=True)


def build_prediction_errors(
    metadata_test: pd.DataFrame,
    y_test: pd.Series,
    y_pred: np.ndarray,
) -> pd.DataFrame:
    """Build a row-level error table."""

    available_metadata = [column for column in METADATA_COLUMNS_FOR_ERRORS if column in metadata_test.columns]
    rows = metadata_test[available_metadata].reset_index(drop=True).copy()
    rows["y_true"] = np.asarray(y_test)
    rows["y_pred"] = y_pred
    rows["signed_error"] = rows["y_pred"] - rows["y_true"]
    rows["abs_error"] = rows["signed_error"].abs()
    return rows


def build_worst_predictions(prediction_errors: pd.DataFrame, top_n: int = 25) -> pd.DataFrame:
    """Build a compact table with the largest prediction errors."""

    rows = prediction_errors.sort_values("abs_error", ascending=False).head(top_n).reset_index(drop=True)
    rows.insert(0, "rank", range(1, len(rows) + 1))
    return rows


def write_metrics_markdown(metrics: pd.DataFrame, output_path: Path) -> None:
    """Write a compact markdown version of model metrics."""

    display_columns = [
        "model",
        "cv_mae_mean",
        "cv_rmse_mean",
        "cv_r2_mean",
        "test_mae",
        "test_rmse",
        "test_r2",
        "is_best_by_test_mae",
    ]
    table = metrics[display_columns].copy()
    for column in ["cv_mae_mean", "cv_rmse_mean", "cv_r2_mean", "test_mae", "test_rmse", "test_r2"]:
        table[column] = table[column].map(lambda value: "" if pd.isna(value) else f"{value:.4f}")
    markdown_headers = [
        "model",
        "MAE при кросс-валидации",
        "RMSE при кросс-валидации",
        "R2 при кросс-валидации",
        "MAE на тестовой выборке",
        "RMSE на тестовой выборке",
        "R2 на тестовой выборке",
        "лучшая по MAE",
    ]
    markdown_table = [
        "| " + " | ".join(markdown_headers) + " |",
        "| " + " | ".join("---" for _ in markdown_headers) + " |",
    ]
    for _, row in table.iterrows():
        markdown_table.append("| " + " | ".join(str(row[column]) for column in display_columns) + " |")
    lines = [
        "# Базовое моделирование: метрики",
        "",
        TARGET_DECISION_NOTE,
        "",
        "Метрики рассчитаны на случайном разбиении на обучающую и тестовую выборки. Кросс-валидация считается только на обучающей выборке. Меньшие MAE/RMSE лучше, R2 выше лучше.",
        "",
        "`MLPRegressor` добавлен как первый простой нейросетевой ориентир. Это не финально настроенная нейросетевая модель, а часть первичного сравнения с более простыми и ансамблевыми подходами.",
        "",
        *markdown_table,
        "",
        "Ограничение: базовые модели проверяют наличие предсказательного сигнала в подготовленных признаках, но не доказывают причинность. Кросс-валидация и групповая проверка могут давать другую картину, поэтому результат базового эксперимента следует считать первичной оценкой, а не финальным выводом.",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def write_group_validation_markdown(group_metrics: pd.DataFrame, warning: str | None, output_path: Path) -> None:
    """Write grouped validation metrics in a small markdown report."""

    lines = [
        "# Базовое моделирование: групповая проверка",
        "",
        "Эта проверка оценивает обобщение на новые профили или участки. Она дополняет случайное разбиение на обучающую и тестовую выборки и не заменяет основную таблицу метрик.",
        "",
    ]
    if warning:
        lines.extend(["Предупреждение:", "", warning, ""])
    elif group_metrics.empty:
        lines.extend(["Групповая проверка не была рассчитана.", ""])
    else:
        display_columns = ["model", "group_column", "n_groups", "n_train", "n_test", "MAE", "RMSE", "R2"]
        table = group_metrics[display_columns].copy()
        for column in ["MAE", "RMSE", "R2"]:
            table[column] = table[column].map(lambda value: "" if pd.isna(value) else f"{value:.4f}")
        markdown_headers = [
            "model",
            "групповая колонка",
            "число групп",
            "строк в обучении",
            "строк в тестовой выборке",
            "MAE",
            "RMSE",
            "R2",
        ]
        lines.append("| " + " | ".join(markdown_headers) + " |")
        lines.append("| " + " | ".join("---" for _ in markdown_headers) + " |")
        for _, row in table.iterrows():
            lines.append("| " + " | ".join(str(row[column]) for column in display_columns) + " |")
        lines.append("")
        lines.append("Если групповая проверка заметно хуже случайного разбиения, это признак более слабого обобщения на новые профили или участки.")
        lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def plot_metrics(metrics: pd.DataFrame, output_path: Path) -> None:
    """Plot test metrics for baseline models."""

    plot_df = metrics.sort_values("test_mae", ascending=True).copy()
    x = np.arange(len(plot_df))
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    axes[0].bar(x - 0.18, plot_df["test_mae"], width=0.36, label="MAE", color="#4F6F8F")
    axes[0].bar(x + 0.18, plot_df["test_rmse"], width=0.36, label="RMSE", color="#C96F53")
    axes[0].set_title("Ошибки на тестовой выборке")
    axes[0].set_ylabel("м/год")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(plot_df["model"], rotation=25, ha="right")
    axes[0].legend()
    axes[0].grid(axis="y", alpha=0.25)

    axes[1].bar(x, plot_df["test_r2"], color="#6C8B5E")
    axes[1].axhline(0, color="#444444", linewidth=0.8)
    axes[1].set_title("R2 на тестовой выборке")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(plot_df["model"], rotation=25, ha="right")
    axes[1].grid(axis="y", alpha=0.25)

    fig.suptitle("Сравнение базовых регрессоров на случайном разбиении")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_feature_importance(importance: pd.DataFrame, output_path: Path, top_n: int = 15) -> None:
    """Plot permutation importance for the selected model."""

    fig, ax = plt.subplots(figsize=(9, 5.8), constrained_layout=True)
    if importance.empty:
        ax.text(0.5, 0.5, "Перестановочная важность признаков недоступна", ha="center", va="center")
        ax.axis("off")
    else:
        plot_df = importance.head(top_n).sort_values("importance_mean", ascending=True)
        ax.barh(plot_df["feature_name"], plot_df["importance_mean"], xerr=plot_df["importance_std"], color="#4F6F8F", alpha=0.9)
        ax.axvline(0, color="#444444", linewidth=0.8)
        ax.set_title("Перестановочная важность признаков выбранной модели")
        ax.set_xlabel("Рост MAE при перестановке признака")
        ax.grid(axis="x", alpha=0.25)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_residuals(prediction_errors: pd.DataFrame, output_path: Path) -> None:
    """Plot prediction error diagnostics for the selected model."""

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    errors = prediction_errors["signed_error"]
    axes[0].hist(errors, bins=16, color="#4F6F8F", alpha=0.85)
    axes[0].axvline(0, color="#444444", linewidth=0.8)
    axes[0].set_title("Распределение ошибок прогноза")
    axes[0].set_xlabel("Прогноз минус факт, м/год")
    axes[0].set_ylabel("Число строк тестовой выборки")
    axes[0].grid(axis="y", alpha=0.25)

    axes[1].scatter(prediction_errors["y_true"], prediction_errors["y_pred"], color="#C96F53", alpha=0.82)
    max_value = max(prediction_errors["y_true"].max(), prediction_errors["y_pred"].max())
    min_value = min(prediction_errors["y_true"].min(), prediction_errors["y_pred"].min())
    axes[1].plot([min_value, max_value], [min_value, max_value], color="#444444", linewidth=0.8)
    axes[1].set_title("Фактическое значение и прогноз")
    axes[1].set_xlabel("Фактическое значение, м/год")
    axes[1].set_ylabel("Прогноз, м/год")
    axes[1].grid(alpha=0.25)

    fig.suptitle("Диагностика ошибок на тестовой выборке")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_model_comparison_mae(metrics: pd.DataFrame, group_metrics: pd.DataFrame, output_path: Path) -> None:
    """Plot MAE comparison for the main split and grouped validation."""

    random_df = metrics[["model", "test_mae"]].rename(columns={"test_mae": "MAE"}).copy()
    random_df["проверка"] = "обучение и тест"
    frames = [random_df]
    if not group_metrics.empty:
        group_df = group_metrics[["model", "MAE"]].copy()
        group_df["проверка"] = "групповая проверка"
        frames.append(group_df)
    plot_df = pd.concat(frames, ignore_index=True)

    ordered_models = metrics.sort_values("test_mae")["model"].tolist()
    checks = plot_df["проверка"].drop_duplicates().tolist()
    x = np.arange(len(ordered_models))
    width = 0.34 if len(checks) > 1 else 0.55
    colors = ["#4F6F8F", "#C96F53"]

    fig, ax = plt.subplots(figsize=(11.5, 5.2), constrained_layout=True)
    for index, check in enumerate(checks):
        values = (
            plot_df.loc[plot_df["проверка"].eq(check)]
            .set_index("model")
            .reindex(ordered_models)["MAE"]
        )
        offset = (index - (len(checks) - 1) / 2) * width
        ax.bar(x + offset, values, width=width, label=check, color=colors[index % len(colors)])

    ax.set_title("Сравнение моделей по MAE")
    ax.set_ylabel("MAE, м/год")
    ax.set_xticks(x)
    ax.set_xticklabels(ordered_models, rotation=25, ha="right")
    ax.grid(axis="y", alpha=0.25)
    if len(checks) > 1:
        ax.legend(title="Схема проверки")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def run_baseline_modeling(input_path: Path | None = None, verbose: bool = True) -> dict[str, Path | str | int | float]:
    """Run baseline modeling and write all report artifacts."""

    logger = setup_logging("baseline_modeling")
    input_path = input_path or PROCESSED_DIR / "final_dataset_for_modeling.csv"
    tables_dir = REPORTS_DIR / "tables"
    figures_dir = REPORTS_DIR / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    target = choose_target(df)
    X, y, metadata, numeric_features, categorical_features, excluded_columns = build_feature_frame(df, target)
    models = build_models(numeric_features, categorical_features)
    metrics, best_model, best_model_name, X_test, y_test, metadata_test, best_predictions = evaluate_models(
        X, y, metadata, models, target
    )
    group_metrics, group_warning = evaluate_group_validation(
        X,
        y,
        metadata,
        build_models(numeric_features, categorical_features),
    )
    importance = compute_permutation_importance(best_model, X_test, y_test, best_model_name, target)
    prediction_errors = build_prediction_errors(metadata_test, y_test, best_predictions)
    worst_predictions = build_worst_predictions(prediction_errors)

    metrics_path = tables_dir / "baseline_modeling_metrics.csv"
    metrics_md_path = tables_dir / "baseline_modeling_metrics.md"
    importance_path = tables_dir / "baseline_feature_importance.csv"
    group_metrics_path = tables_dir / "baseline_group_validation_metrics.csv"
    group_metrics_md_path = tables_dir / "baseline_group_validation_metrics.md"
    worst_predictions_path = tables_dir / "baseline_worst_predictions.csv"
    metrics_figure_path = figures_dir / "baseline_modeling_metrics.png"
    importance_figure_path = figures_dir / "baseline_feature_importance.png"
    residuals_figure_path = figures_dir / "baseline_residuals.png"
    model_comparison_mae_path = figures_dir / "03_model_comparison_mae.png"

    metrics.to_csv(metrics_path, index=False)
    write_metrics_markdown(metrics, metrics_md_path)
    importance.to_csv(importance_path, index=False)
    group_metrics.to_csv(group_metrics_path, index=False)
    write_group_validation_markdown(group_metrics, group_warning, group_metrics_md_path)
    worst_predictions.to_csv(worst_predictions_path, index=False)
    plot_metrics(metrics, metrics_figure_path)
    plot_feature_importance(importance, importance_figure_path)
    plot_residuals(prediction_errors, residuals_figure_path)
    plot_model_comparison_mae(metrics, group_metrics, model_comparison_mae_path)

    best_row = metrics.loc[metrics["model"].eq(best_model_name)].iloc[0]
    outputs: dict[str, Path | str | int | float] = {
        "target": target,
        "n_rows": int(len(X)),
        "n_features": int(X.shape[1]),
        "numeric_features": ", ".join(numeric_features),
        "categorical_features": ", ".join(categorical_features),
        "excluded_columns": ", ".join(excluded_columns),
        "models": ", ".join(models.keys()),
        "best_model": best_model_name,
        "best_test_mae": float(best_row["test_mae"]),
        "best_test_rmse": float(best_row["test_rmse"]),
        "best_test_r2": float(best_row["test_r2"]),
        "metrics_csv": metrics_path,
        "metrics_md": metrics_md_path,
        "feature_importance_csv": importance_path,
        "group_validation_metrics_csv": group_metrics_path,
        "group_validation_metrics_md": group_metrics_md_path,
        "worst_predictions_csv": worst_predictions_path,
        "metrics_figure": metrics_figure_path,
        "feature_importance_figure": importance_figure_path,
        "residuals_figure": residuals_figure_path,
        "model_comparison_mae_figure": model_comparison_mae_path,
    }

    if verbose:
        logger.info("Целевая переменная: %s", target)
        logger.info("Обоснование выбора целевой переменной: %s", TARGET_DECISION_NOTE)
        logger.info("Строк: %s; признаков до кодирования категорий: %s", len(X), X.shape[1])
        logger.info("Модели: %s", ", ".join(models))
        logger.info(
            "Лучшая по MAE на тестовой выборке при данном случайном разбиении: %s | MAE=%.4f RMSE=%.4f R2=%.4f",
            best_model_name,
            best_row["test_mae"],
            best_row["test_rmse"],
            best_row["test_r2"],
        )
        if group_warning:
            logger.warning("%s", group_warning)
        elif not group_metrics.empty:
            group_best = group_metrics.iloc[0]
            logger.info(
                "Лучшая по MAE при групповой проверке: %s | группа=%s MAE=%.4f RMSE=%.4f R2=%.4f",
                group_best["model"],
                group_best["group_column"],
                group_best["MAE"],
                group_best["RMSE"],
                group_best["R2"],
            )
        artifact_labels = {
            "metrics_csv": "таблица метрик",
            "metrics_md": "markdown-версия метрик",
            "feature_importance_csv": "таблица важности признаков",
            "group_validation_metrics_csv": "таблица групповой проверки",
            "group_validation_metrics_md": "markdown-версия групповой проверки",
            "worst_predictions_csv": "наблюдения с наибольшими ошибками прогноза",
            "metrics_figure": "график метрик",
            "feature_importance_figure": "график важности признаков",
            "residuals_figure": "график ошибок прогноза",
            "model_comparison_mae_figure": "график сравнения моделей по MAE",
        }
        for key, label in artifact_labels.items():
            logger.info("%s -> %s", label, relative_to_root(Path(outputs[key])))

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=PROCESSED_DIR / "final_dataset_for_modeling.csv",
        help="Путь к подготовленному датасету для моделирования.",
    )
    args = parser.parse_args()
    run_baseline_modeling(input_path=args.input, verbose=True)


if __name__ == "__main__":
    main()

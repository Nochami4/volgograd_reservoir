"""Демонстрационный автоэнкодер для поиска подозрительных наблюдений."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor

from src.analysis.baseline_modeling import (
    RANDOM_STATE,
    build_feature_frame,
    build_preprocessor,
    choose_target,
)
from src.parsers.common import PROCESSED_DIR, REPORTS_DIR, relative_to_root, setup_logging

ANOMALY_QUANTILE = 0.95
TOP_N = 25
METADATA_COLUMNS = ["interval_id", "site_id", "profile_id", "date_start", "date_end"]
ANOMALY_NOTE = (
    "Строка имеет высокую ошибку восстановления автоэнкодера и требует предметной проверки; "
    "это не является автоматическим доказательством ошибки в данных."
)
NORMAL_NOTE = "Ошибка восстановления ниже выбранного порога."


def build_autoencoder(input_dim: int) -> MLPRegressor:
    """Build a small MLP autoencoder with a narrow hidden layer."""

    bottleneck = max(4, min(8, input_dim // 3))
    hidden_width = max(16, min(32, input_dim // 2))
    return MLPRegressor(
        hidden_layer_sizes=(hidden_width, bottleneck, hidden_width),
        activation="relu",
        solver="adam",
        alpha=0.001,
        learning_rate_init=0.001,
        max_iter=700,
        early_stopping=True,
        n_iter_no_change=20,
        random_state=RANDOM_STATE,
    )


def prepare_feature_matrix(
    df: pd.DataFrame,
) -> tuple[np.ndarray, pd.DataFrame, list[str], list[str], int, str]:
    """Prepare the same conservative feature set used by baseline modeling."""

    target = choose_target(df)
    X, _, metadata, numeric_features, categorical_features, _ = build_feature_frame(df, target)
    if X.empty or X.shape[1] == 0:
        raise ValueError("Не осталось признаков для обучения автоэнкодера.")

    train_index, _ = train_test_split(
        np.arange(len(X)),
        test_size=0.2,
        random_state=RANDOM_STATE,
    )
    preprocessor = build_preprocessor(numeric_features, categorical_features)
    preprocessor.fit(X.iloc[train_index])
    matrix = preprocessor.transform(X)
    matrix = np.asarray(matrix, dtype=float)
    return matrix, metadata.reset_index(drop=True), numeric_features, categorical_features, len(train_index), target


def fit_autoencoder(matrix: np.ndarray) -> tuple[MLPRegressor, np.ndarray, np.ndarray, float]:
    """Fit the autoencoder and return reconstruction errors for all rows."""

    train_index, _ = train_test_split(
        np.arange(matrix.shape[0]),
        test_size=0.2,
        random_state=RANDOM_STATE,
    )
    model = build_autoencoder(matrix.shape[1])
    model.fit(matrix[train_index], matrix[train_index])

    reconstructed = model.predict(matrix)
    errors = np.mean((matrix - reconstructed) ** 2, axis=1)
    train_errors = errors[train_index]
    threshold = float(np.quantile(train_errors, ANOMALY_QUANTILE))
    return model, errors, train_errors, threshold


def build_scores_table(metadata: pd.DataFrame, errors: np.ndarray, threshold: float) -> pd.DataFrame:
    """Build a sorted table with row-level reconstruction errors."""

    available_metadata = [column for column in METADATA_COLUMNS if column in metadata.columns]
    scores = metadata[available_metadata].copy()
    scores["reconstruction_error"] = errors
    scores["anomaly_threshold"] = threshold
    scores["is_autoencoder_anomaly"] = scores["reconstruction_error"].ge(threshold)
    scores["anomaly_note_ru"] = np.where(scores["is_autoencoder_anomaly"], ANOMALY_NOTE, NORMAL_NOTE)
    scores = scores.sort_values("reconstruction_error", ascending=False).reset_index(drop=True)
    scores.insert(0, "rank", range(1, len(scores) + 1))
    return scores


def _row_label(row: pd.Series) -> str:
    site = str(row.get("site_id", "site")).strip()
    profile = str(row.get("profile_id", "profile")).strip()
    start = str(row.get("date_start", "?")).strip()
    end = str(row.get("date_end", "?")).strip()
    return f"{site} / {profile} / {start}-{end}"


def plot_reconstruction_error(scores: pd.DataFrame, output_path: Path) -> None:
    """Plot reconstruction error distribution."""

    threshold = float(scores["anomaly_threshold"].iloc[0])
    fig, ax = plt.subplots(figsize=(9, 5.2), constrained_layout=True)
    ax.hist(scores["reconstruction_error"], bins=32, color="#4F6F8F", alpha=0.86)
    ax.axvline(threshold, color="#C96F53", linewidth=2, label="порог 95-го процентиля")
    ax.set_title("Распределение ошибок восстановления автоэнкодера")
    ax.set_xlabel("Ошибка восстановления")
    ax.set_ylabel("Число наблюдений")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def plot_top_anomalies(top_scores: pd.DataFrame, output_path: Path) -> None:
    """Plot top rows by reconstruction error."""

    plot_df = top_scores.sort_values("reconstruction_error", ascending=True).copy()
    labels = plot_df.apply(_row_label, axis=1)
    fig, ax = plt.subplots(figsize=(10, 8), constrained_layout=True)
    ax.barh(labels, plot_df["reconstruction_error"], color="#4F6F8F", alpha=0.9)
    ax.axvline(float(plot_df["anomaly_threshold"].iloc[0]), color="#C96F53", linewidth=1.8, label="порог")
    ax.set_title("Наблюдения с наибольшей ошибкой восстановления")
    ax.set_xlabel("Ошибка восстановления")
    ax.set_ylabel("Строка")
    ax.grid(axis="x", alpha=0.25)
    ax.legend()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=240, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def write_summary_markdown(
    output_path: Path,
    *,
    input_path: Path,
    n_rows: int,
    n_features_raw: int,
    n_features_encoded: int,
    n_train: int,
    threshold: float,
    n_anomalies: int,
    numeric_features: list[str],
    categorical_features: list[str],
) -> None:
    """Write a compact Russian markdown report."""

    percent = 100 * n_anomalies / n_rows if n_rows else 0
    lines = [
        "# Автоэнкодер: поиск подозрительных наблюдений",
        "",
        "Автоэнкодер — это нейросетевая модель, которая учится восстанавливать входные данные. Если строка плохо восстанавливается, она отличается от типичной структуры признаков и попадает в список для проверки.",
        "",
        f"Использован файл `{relative_to_root(input_path)}`. В модель передано {n_rows} строк, {n_features_raw} исходных признаков до кодирования категорий и {n_features_encoded} числовых признаков после предобработки.",
        "",
        "Предобработка: числовые признаки заполняются медианой и масштабируются, категориальные признаки заполняются наиболее частым значением и кодируются. Целевая переменная и прямые метрики изменения береговой бровки не используются как признаки.",
        "",
        f"Порог подозрительности выбран как 95-й процентиль ошибки восстановления на обучающей части ({n_train} строк): `{threshold:.6f}`.",
        "",
        f"Помечено подозрительных строк: {n_anomalies} из {n_rows} ({percent:.1f}%).",
        "",
        "Если при обучении появляется предупреждение о сходимости, оно не скрывается: модель используется как демонстрационный ориентир, а не как финально настроенная нейросетевая архитектура.",
        "",
        "Эти строки не удаляются автоматически и не считаются доказанными ошибками исходных данных. Их нужно проверить предметно: посмотреть участок, профиль, даты интервала, QC-пояснения и возможные конфликтующие дубли.",
        "",
        "Использованные признаки:",
        "",
        f"- числовые: {', '.join(numeric_features) if numeric_features else 'нет'};",
        f"- категориальные: {', '.join(categorical_features) if categorical_features else 'нет'}.",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def run_autoencoder_anomaly_detection(
    input_path: Path | None = None,
    verbose: bool = True,
) -> dict[str, Path | int | float]:
    """Run autoencoder anomaly detection and save report artifacts."""

    logger = setup_logging("autoencoder_anomaly_detection")
    input_path = input_path or PROCESSED_DIR / "final_dataset_for_modeling.csv"
    tables_dir = REPORTS_DIR / "tables"
    figures_dir = REPORTS_DIR / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    matrix, metadata, numeric_features, categorical_features, n_train, target = prepare_feature_matrix(df)
    _, errors, _, threshold = fit_autoencoder(matrix)
    scores = build_scores_table(metadata, errors, threshold)
    top_scores = scores.head(TOP_N).copy()

    scores_path = tables_dir / "autoencoder_anomaly_scores.csv"
    top_scores_path = tables_dir / "autoencoder_top_anomalies.csv"
    summary_path = tables_dir / "autoencoder_anomaly_summary.md"
    error_figure_path = figures_dir / "04_autoencoder_reconstruction_error.png"
    top_figure_path = figures_dir / "04_autoencoder_top_anomalies.png"

    scores.to_csv(scores_path, index=False)
    top_scores.to_csv(top_scores_path, index=False)
    write_summary_markdown(
        summary_path,
        input_path=input_path,
        n_rows=len(scores),
        n_features_raw=len(numeric_features) + len(categorical_features),
        n_features_encoded=matrix.shape[1],
        n_train=n_train,
        threshold=threshold,
        n_anomalies=int(scores["is_autoencoder_anomaly"].sum()),
        numeric_features=numeric_features,
        categorical_features=categorical_features,
    )
    plot_reconstruction_error(scores, error_figure_path)
    plot_top_anomalies(top_scores, top_figure_path)

    outputs: dict[str, Path | int | float] = {
        "n_rows": int(len(scores)),
        "n_features_raw": int(len(numeric_features) + len(categorical_features)),
        "n_features_encoded": int(matrix.shape[1]),
        "n_train": int(n_train),
        "threshold": float(threshold),
        "n_anomalies": int(scores["is_autoencoder_anomaly"].sum()),
        "target_excluded": target,
        "scores_csv": scores_path,
        "top_anomalies_csv": top_scores_path,
        "summary_md": summary_path,
        "reconstruction_error_figure": error_figure_path,
        "top_anomalies_figure": top_figure_path,
    }

    if verbose:
        logger.info("Обработано строк: %s", outputs["n_rows"])
        logger.info("Признаков до кодирования категорий: %s", outputs["n_features_raw"])
        logger.info("Признаков после предобработки: %s", outputs["n_features_encoded"])
        logger.info("Исключённая целевая переменная: %s", outputs["target_excluded"])
        logger.info("Порог ошибки восстановления: %.6f", outputs["threshold"])
        logger.info("Подозрительных строк: %s", outputs["n_anomalies"])
        artifact_labels = {
            "scores_csv": "таблица ошибок восстановления",
            "top_anomalies_csv": "наблюдения с наибольшей ошибкой восстановления",
            "summary_md": "markdown-отчёт автоэнкодера",
            "reconstruction_error_figure": "график распределения ошибок восстановления",
            "top_anomalies_figure": "график наблюдений с наибольшей ошибкой восстановления",
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
        help="Путь к подготовленному датасету для поиска подозрительных наблюдений.",
    )
    args = parser.parse_args()
    run_autoencoder_anomaly_detection(input_path=args.input, verbose=True)


if __name__ == "__main__":
    main()

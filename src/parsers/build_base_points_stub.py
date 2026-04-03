"""Build a transparent base-points layer with a manual-first workflow."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

import pandas as pd

from .common import (
    INTERIM_DIR,
    PROCESSED_DIR,
    RAW_DIR,
    clean_text,
    parse_number,
    relative_to_root,
    resolve_site_id_from_text,
    safe_parse_date,
    setup_logging,
)

BASE_POINT_COLUMNS = [
    "base_point_id",
    "site_id",
    "base_point_name",
    "obs_date",
    "x_m",
    "y_m",
    "accuracy_m",
    "point_status",
    "status_note",
    "is_calculated",
    "is_reinstalled",
    "is_new",
    "source_file",
    "source_row_ref",
    "qc_flag",
    "qc_note",
]

DOC_PATHS = [
    RAW_DIR / "docs" / "GPS-координаты пунктов базиса с дополнениями Барановой 2024 г..doc",
    RAW_DIR / "docs" / "Абрисы-2006.doc",
]


def normalize_point_name_token(text: object) -> str:
    """Normalize a point token for matching across sources."""

    return clean_text(text).upper().replace(" ", "")


def extract_unicode_strings(path: Path) -> list[tuple[int, str]]:
    """Extract UTF-16LE strings from a legacy Word file using `strings`."""

    try:
        result = subprocess.run(
            ["strings", "-el", "-n", "6", str(path)],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []
    return [(line_no, line.rstrip()) for line_no, line in enumerate(result.stdout.splitlines(), start=1)]


def is_coordinate_token(text: str) -> bool:
    """Return True for coordinate-like integer tokens."""

    return bool(re.fullmatch(r"0?\d{6,7}", text.strip()))


def is_point_name_candidate(text: str) -> bool:
    """Return True for point-name-like tokens extracted from the doc."""

    token = clean_text(text)
    if not token:
        return False
    if safe_parse_date(token) is not None:
        return False
    if is_coordinate_token(token):
        return False
    if any(fragment in token.lower() for fragment in ["mergeformat", "ole_link", "times new roman", "summaryinformation"]):
        return False
    return bool(re.fullmatch(r"[0-9A-Za-zА-Яа-я\"* ._-]+\([^)]+\)", token))


def find_nearby_date_and_coords(lines: list[tuple[int, str]], index: int) -> tuple[str | None, float | None, float | None, str]:
    """Find a nearby date and coordinate pair around a point-name token."""

    qc_notes: list[str] = []
    date_text: str | None = None
    x_value: float | None = None
    y_value: float | None = None

    for offset in [1, 2, 3, -1, -2, -3]:
        probe_index = index + offset
        if not 0 <= probe_index < len(lines):
            continue
        _, probe_text = lines[probe_index]
        if re.fullmatch(r"\d{1,2}\.\d{2}\.\d{2,4}", clean_text(probe_text)) or re.fullmatch(r"\.\d{2}\.\d{4}", clean_text(probe_text)):
            date_text = clean_text(probe_text)
            for coord_start in [probe_index + 1, probe_index - 2]:
                coord_window = lines[coord_start : coord_start + 2]
                if len(coord_window) < 2:
                    continue
                coord_tokens = [clean_text(item[1]) for item in coord_window]
                if all(is_coordinate_token(token) for token in coord_tokens):
                    x_value = float(int(coord_tokens[0]))
                    y_value = float(int(coord_tokens[1]))
                    return date_text, x_value, y_value, ""
            qc_notes.append("Nearby date found but coordinate pair was not recovered.")
            return date_text, x_value, y_value, " ".join(qc_notes)

    qc_notes.append("No nearby explicit date token found.")
    return date_text, x_value, y_value, " ".join(qc_notes)


def parse_base_point_date_token(text: object) -> str | None:
    """Parse only explicit full dates for base-point records."""

    token = clean_text(text)
    if not token:
        return None
    if re.fullmatch(r"\d{1,2}\.\d{2}\.\d{2,4}", token):
        parsed = safe_parse_date(token)
        return parsed.isoformat() if parsed else None
    return None


def build_site_lookup() -> dict[str, str]:
    """Map normalized point names to site ids when the mapping is unique."""

    shoreline_path = PROCESSED_DIR / "shoreline_observations.csv"
    if not shoreline_path.exists():
        return {}

    obs = pd.read_csv(shoreline_path)
    mappings: dict[str, set[str]] = {}
    for column in ["measured_point_name", "pn_name"]:
        if column not in obs.columns:
            continue
        for _, row in obs[["site_id", column]].dropna().iterrows():
            token = normalize_point_name_token(row[column])
            if not token:
                continue
            mappings.setdefault(token, set()).add(str(row["site_id"]))

    return {token: next(iter(site_ids)) for token, site_ids in mappings.items() if len(site_ids) == 1}


def normalize_point_status(status_value: object, status_note: object) -> str:
    """Normalize point status into a controlled vocabulary."""

    status_text = clean_text(status_value).lower()
    if status_text in {"new", "reinstalled", "calculated", "refined", "original", "unknown"}:
        return status_text

    raw_text = " ".join(part for part in [clean_text(status_value), clean_text(status_note)] if part).lower()
    if "расч" in raw_text:
        return "calculated"
    if "переустанов" in raw_text:
        return "reinstalled"
    if "нов" in raw_text:
        return "new"
    if "уточнен" in raw_text or "уточнение" in raw_text:
        return "refined"
    if raw_text:
        return "original"
    return "unknown"


def extract_doc_candidates(site_lookup: dict[str, str]) -> pd.DataFrame:
    """Extract only the most reliable point fragments from legacy `.doc` files."""

    rows: list[dict[str, object]] = []
    for path in DOC_PATHS:
        lines = extract_unicode_strings(path)
        for index, (line_no, text) in enumerate(lines):
            point_name = clean_text(text)
            if not is_point_name_candidate(point_name):
                continue

            date_text, x_value, y_value, extraction_note = find_nearby_date_and_coords(lines, index)
            obs_date = parse_base_point_date_token(date_text)
            qc_flags = ["MANUAL_REVIEW_REQUIRED"]
            qc_notes = ["Semi-automatic candidate from UTF-16 string extraction; verify against the original .doc before analysis."]
            if extraction_note:
                qc_notes.append(extraction_note)
            if date_text and obs_date is None:
                qc_flags.append("PARTIAL_DATE")
                qc_notes.append(f"Date token {date_text!r} could not be normalized safely.")
            if x_value is None or y_value is None:
                qc_flags.append("MISSING_COORDINATE")

            normalized_name = normalize_point_name_token(point_name)
            site_id = site_lookup.get(normalized_name)
            if site_id is None:
                qc_flags.append("SITE_ID_UNRESOLVED")
                qc_notes.append("site_id was not inferred because the point name was absent or ambiguous in shoreline observations.")
            else:
                qc_notes.append("site_id inferred from a unique point-name occurrence in shoreline observations.")

            rows.append(
                {
                    "base_point_id": None,
                    "site_id": site_id,
                    "base_point_name": point_name,
                    "obs_date": obs_date,
                    "x_m": x_value,
                    "y_m": y_value,
                    "accuracy_m": None,
                    "point_status": "unknown",
                    "status_note": None,
                    "is_calculated": False,
                    "is_reinstalled": False,
                    "is_new": False,
                    "source_file": relative_to_root(path),
                    "source_row_ref": f"strings_el:{line_no}",
                    "qc_flag": ";".join(dict.fromkeys(qc_flags)),
                    "qc_note": " ".join(dict.fromkeys(qc_notes)),
                }
            )

    if not rows:
        return pd.DataFrame(columns=BASE_POINT_COLUMNS)

    candidate_df = pd.DataFrame(rows, columns=BASE_POINT_COLUMNS)
    return candidate_df.drop_duplicates(subset=["source_file", "source_row_ref", "base_point_name"], keep="first")


def load_existing_template(template_path: Path) -> pd.DataFrame:
    """Load an existing manual template without destroying user edits."""

    if not template_path.exists():
        return pd.DataFrame(columns=BASE_POINT_COLUMNS)
    existing = pd.read_csv(template_path, dtype=object)
    for column in BASE_POINT_COLUMNS:
        if column not in existing.columns:
            existing[column] = None
    return existing[BASE_POINT_COLUMNS]


def merge_template(existing: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    """Merge existing manual entries with auto-generated candidates."""

    if existing.empty:
        combined = candidates.copy()
    else:
        auto_mask = (
            existing["source_row_ref"].fillna("").astype(str).str.startswith("strings_el:")
            & existing["qc_flag"].fillna("").astype(str).str.contains("MANUAL_REVIEW_REQUIRED", regex=False)
            & existing["status_note"].fillna("").astype(str).eq("")
            & existing["accuracy_m"].fillna("").astype(str).eq("")
        )
        preserved_existing = existing.loc[~auto_mask].copy()
        frames = [frame for frame in [preserved_existing, candidates] if not frame.empty]
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=BASE_POINT_COLUMNS)
    if combined.empty:
        return pd.DataFrame(columns=BASE_POINT_COLUMNS)
    combined = combined.drop_duplicates(subset=["source_file", "source_row_ref", "base_point_name"], keep="first")
    return combined[BASE_POINT_COLUMNS]


def finalize_base_points(template_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the manual template into the processed base-points table."""

    if template_df.empty:
        return pd.DataFrame(columns=BASE_POINT_COLUMNS)

    result = template_df.copy()
    result["site_id"] = result["site_id"].map(resolve_site_id_from_text)
    result["obs_date"] = result["obs_date"].map(lambda value: safe_parse_date(value).isoformat() if safe_parse_date(value) else None)
    for column in ["x_m", "y_m", "accuracy_m"]:
        result[column] = result[column].map(parse_number)

    result["point_status"] = result.apply(
        lambda row: normalize_point_status(row.get("point_status"), row.get("status_note")),
        axis=1,
    )
    result["is_calculated"] = result["point_status"].eq("calculated")
    result["is_reinstalled"] = result["point_status"].eq("reinstalled")
    result["is_new"] = result["point_status"].eq("new")

    qc_flag_series = result["qc_flag"].fillna("").astype(str)
    qc_note_series = result["qc_note"].fillna("").astype(str)
    unresolved_mask = result["site_id"].isna()
    qc_flag_series.loc[unresolved_mask] = qc_flag_series.loc[unresolved_mask].map(
        lambda value: ";".join(dict.fromkeys([part for part in [value, "SITE_ID_UNRESOLVED"] if part]))
    )
    qc_note_series.loc[unresolved_mask] = qc_note_series.loc[unresolved_mask].map(
        lambda value: " ".join(
            dict.fromkeys(
                [
                    part
                    for part in [
                        value,
                        "site_id is still unresolved and requires manual mapping before geospatial interpretation.",
                    ]
                    if part
                ]
            )
        )
    )
    result["qc_flag"] = qc_flag_series.replace("", pd.NA)
    result["qc_note"] = qc_note_series.replace("", pd.NA)

    def make_base_point_id(row: pd.Series) -> str:
        site = clean_text(row.get("site_id")) or "unknown_site"
        point = re.sub(r"[^a-z0-9]+", "_", normalize_point_name_token(row.get("base_point_name")).lower()).strip("_") or "unknown_point"
        obs_date = clean_text(row.get("obs_date")) or "unknown_date"
        return f"{site}_{point}_{obs_date}"

    result["base_point_id"] = result.apply(make_base_point_id, axis=1)
    return result[BASE_POINT_COLUMNS]


def write_template_readme(path: Path) -> None:
    """Write instructions for the manual base-point workflow."""

    path.write_text(
        "\n".join(
            [
                "# Base Points Manual Template",
                "",
                "Status: partially manual workflow.",
                "",
                "What this file contains:",
                "- Existing manual edits are preserved across pipeline runs.",
                "- Semi-automatic candidate rows may be appended from UTF-16 string extraction of the legacy `.doc` files.",
                "- Every candidate row must be manually checked against the source document before scientific use.",
                "",
                "How to fill the columns:",
                "- `base_point_name`: preserve the source spelling of the point identifier.",
                "- `obs_date`: use ISO date only when the source gives an explicit full date.",
                "- `x_m`, `y_m`: copy coordinates exactly as given; do not transform units or CRS silently.",
                "- `accuracy_m`: fill only when the source explicitly states measurement accuracy.",
                "- `point_status`: allowed normalized values are `new`, `reinstalled`, `calculated`, `refined`, `original`, `unknown`.",
                "- `status_note`: keep the original wording that justifies the status.",
                "- `site_id`: fill manually when not resolved automatically.",
                "- `qc_flag`: keep parser flags and append your own transparent tags such as `MANUAL_VERIFIED` when review is complete.",
                "- `qc_note`: explain why a `site_id`, status, or date remains uncertain.",
                "- `source_row_ref`: keep a stable reference such as `strings_el:115` or a manual note like `page 2, paragraph 3`.",
                "",
                "Status normalization rules used by the parser:",
                "- `Новый` -> `new`",
                "- `Переустановлен` -> `reinstalled`",
                "- `Расчетные координаты` / `Расчётные координаты` -> `calculated`",
                "- `Уточнены` / `Уточнение координат` -> `refined`",
                "- otherwise -> `original` or `unknown`",
                "",
                "Important:",
                "- Do not invent coordinates or dates.",
                "- Leave uncertain values blank and explain uncertainty in `qc_note` or `status_note`.",
                "- Rows with `SITE_ID_UNRESOLVED` are expected until the point-to-site mapping is confirmed manually.",
            ]
        ),
        encoding="utf-8",
    )


def build_base_points_stub(output_path: Path | None = None) -> Path:
    """Build `base_points.csv` from a preserved manual template plus safe auto-candidates."""

    output_path = output_path or PROCESSED_DIR / "base_points.csv"
    template_path = INTERIM_DIR / "base_points_manual_template.csv"
    readme_path = INTERIM_DIR / "base_points_manual_template_README.md"
    logger = setup_logging("build_base_points_stub")

    site_lookup = build_site_lookup()
    existing_template = load_existing_template(template_path)
    candidate_template = extract_doc_candidates(site_lookup)
    merged_template = merge_template(existing_template, candidate_template)

    template_path.parent.mkdir(parents=True, exist_ok=True)
    merged_template.to_csv(template_path, index=False)
    write_template_readme(readme_path)

    processed = finalize_base_points(merged_template)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    processed.to_csv(output_path, index=False)

    logger.info("Updated manual template at %s with %s rows", template_path, len(merged_template))
    logger.info("Built %s with %s rows", output_path, len(processed))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "base_points.csv")
    args = parser.parse_args()
    build_base_points_stub(output_path=args.output)


if __name__ == "__main__":
    main()

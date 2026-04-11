"""Build normalized base-point history/current layers from the legacy GPS `.doc`."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

import pandas as pd

from .common import (
    PROCESSED_DIR,
    RAW_DIR,
    REPORTS_DIR,
    clean_text,
    merge_with_checks,
    parse_number,
    relative_to_root,
    resolve_site_id_from_text,
    safe_parse_date,
    setup_logging,
)

DOC_PATH = RAW_DIR / "docs" / "GPS-координаты пунктов базиса с дополнениями Барановой 2024 г..doc"
SOURCE_FILE = relative_to_root(DOC_PATH)

HISTORY_COLUMNS = [
    "site_name_raw",
    "site_id",
    "base_point_name_raw",
    "base_point_name_norm",
    "obs_date_raw",
    "obs_date",
    "y_m",
    "x_m",
    "accuracy_m",
    "note_raw",
    "point_status",
    "is_calculated",
    "is_reinstalled",
    "is_new",
    "is_refined",
    "has_uncertain_date",
    "needs_manual_review",
    "review_reason",
    "source_file",
    "source_row_ref",
]

MANUAL_SITE_MAP = {
    "118(0609)": "novonikolskoe",
    "124D": "nizhniy_balykley",
    "124E": "nizhniy_balykley",
    "124F": "nizhniy_balykley",
    "126D": "nizhniy_balykley",
    "126E(1107)": "nizhniy_balykley",
    "126F(1107)": "nizhniy_balykley",
    "128D(1107)": "nizhniy_balykley",
    "32(1407)": "urakov_bugor",
    "33(1407)": "urakov_bugor",
    "3": "urakov_bugor",
    "571(1507)": "molchanovka",
    "581(1507)": "molchanovka",
    "138C": "berezhnovka",
    "140D": "berezhnovka",
    "140F": "berezhnovka",
}

SPECIAL_CASES = [
    {
        "site_name_raw": "Ураков Бугор",
        "site_id": "urakov_bugor",
        "base_point_name_raw": "3",
        "base_point_name_norm": "3",
        "obs_date_raw": "14.08.2019",
        "y_m": "5565795",
        "x_m": "0543709",
        "accuracy_m": None,
        "note_raw": "Координаты извлечены из блока без явной подписи точки; в проектном контексте соответствуют точке 3 участка Ураков Бугор.",
        "review_reason": "Coordinate assignment inferred from the document block and conflicts with the known Nizhniy Urakov point-3 coordinate cluster.",
        "source_row_ref": "strings_el:229",
    }
]


def normalize_point_name_token(text: object) -> str:
    """Normalize a point token for matching across sources."""

    token = clean_text(text).upper()
    token = token.replace(" ", "")
    token = token.replace("(", "(").replace(")", ")")
    if token.endswith("("):
        token = token[:-1]
    return token


def canonical_site_name(site_id: object) -> str | None:
    """Return a readable site name from the normalized id."""

    value = resolve_site_id_from_text(site_id)
    if value is None:
        return None
    sites = pd.read_csv(PROCESSED_DIR / "sites.csv")
    match = sites.loc[sites["site_id"].eq(value), "site_name"]
    if match.empty:
        return None
    return str(match.iloc[0])


def extract_unicode_strings(path: Path) -> list[tuple[int, str]]:
    """Extract UTF-16LE strings from a legacy Word file using `strings`."""

    try:
        result = subprocess.run(
            ["strings", "-el", "-n", "4", str(path)],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []
    return [(line_no, line.rstrip()) for line_no, line in enumerate(result.stdout.splitlines(), start=1)]


def is_coordinate_token(text: object) -> bool:
    """Return True for coordinate-like integer tokens."""

    return bool(re.fullmatch(r"0?\d{6,7}", clean_text(text)))


def is_full_date_token(text: object) -> bool:
    """Return True for full date tokens."""

    return bool(re.fullmatch(r"\d{1,2}\.\d{2}\.\d{2,4}", clean_text(text)))


def is_partial_date_token(text: object) -> bool:
    """Return True for partial date tokens that cannot be normalized safely."""

    return bool(re.fullmatch(r"\.?\d{0,2}\.\d{2}\.\d{4}", clean_text(text))) and not is_full_date_token(text)


def is_point_name_candidate(text: object) -> bool:
    """Return True for tokens that look like point names in the extracted string stream."""

    token = clean_text(text)
    if not token or is_coordinate_token(token) or is_full_date_token(token) or is_partial_date_token(token):
        return False
    upper_token = token.upper()
    if token.startswith("-") or "MERGEFORMAT" in upper_token or upper_token in {"UNKNOWN", "ARIAL", "CALIBRI", "CAMBRIA MATH", "TIMES NEW ROMAN", "SYMBOL"}:
        return False
    if upper_token in {"GPS-", "-2024", "OLE_LINK1", "1TABLE"}:
        return False
    if upper_token.startswith("GPS"):
        return False
    if not any(char.isdigit() for char in token):
        return False
    if token.startswith("(") or token.endswith("("):
        return False
    if re.fullmatch(r"\d+", token):
        return False
    return bool(re.fullmatch(r'[0-9A-Za-zА-Яа-я"* ._()-]+', token))


def normalize_point_status(note_raw: object) -> str:
    """Normalize point status from note text."""

    text = clean_text(note_raw).lower()
    if "расч" in text:
        return "calculated"
    if "переустанов" in text:
        return "reinstalled"
    if "нов" in text:
        return "new"
    if "уточ" in text or "gps" in text:
        return "refined"
    return "original"


def extract_accuracy(note_raw: object) -> float | None:
    """Extract explicit accuracy when present."""

    text = clean_text(note_raw).lower()
    match = re.search(r"d\s*=\s*([0-9]+(?:[,.][0-9]+)?)", text)
    if not match:
        return None
    return parse_number(match.group(1))


def build_site_lookup() -> pd.DataFrame:
    """Build a one-row-per-point lookup from shoreline observations."""

    obs = pd.read_csv(PROCESSED_DIR / "shoreline_observations.csv")
    rows: list[dict[str, object]] = []
    for column in ["measured_point_name", "pn_name"]:
        for _, row in obs[["site_id", column]].dropna().iterrows():
            point_name = clean_text(row[column])
            if not point_name:
                continue
            rows.append(
                {
                    "base_point_name_norm": normalize_point_name_token(point_name),
                    "site_id": row["site_id"],
                    "match_source": column,
                }
            )
    lookup = pd.DataFrame(rows).drop_duplicates()
    if lookup.empty:
        return pd.DataFrame(columns=["base_point_name_norm", "site_id"])

    counts = lookup.groupby("base_point_name_norm")["site_id"].nunique().reset_index(name="n_site_ids")
    unique_lookup = lookup.merge(counts, on="base_point_name_norm", how="left", validate="many_to_one")
    unique_lookup = unique_lookup.loc[unique_lookup["n_site_ids"].eq(1), ["base_point_name_norm", "site_id"]].drop_duplicates()
    return unique_lookup


def parse_record_context(lines: list[tuple[int, str]], index: int) -> tuple[str | None, float | None, float | None, str | None]:
    """Parse the best available date, coordinates, and nearby note for one point token."""

    date_raw = None
    y_value = None
    x_value = None
    note_bits: list[str] = []

    point_name = clean_text(lines[index][1])
    point_name_norm = normalize_point_name_token(point_name)

    if point_name_norm in {"138C"} and index + 2 < len(lines):
        y_candidate = clean_text(lines[index + 1][1])
        x_candidate = clean_text(lines[index + 2][1])
        if is_coordinate_token(y_candidate) and is_coordinate_token(x_candidate):
            return None, parse_number(y_candidate), parse_number(x_candidate), None

    for offset in range(1, 5):
        if index + offset >= len(lines):
            break
        candidate = clean_text(lines[index + offset][1])
        if is_full_date_token(candidate) or is_partial_date_token(candidate):
            date_raw = candidate
            if index + offset + 2 < len(lines):
                y_candidate = clean_text(lines[index + offset + 1][1])
                x_candidate = clean_text(lines[index + offset + 2][1])
                if is_coordinate_token(y_candidate) and is_coordinate_token(x_candidate):
                    y_value = parse_number(y_candidate)
                    x_value = parse_number(x_candidate)
                    for note_offset in range(index + offset + 3, min(index + offset + 5, len(lines))):
                        note_candidate = clean_text(lines[note_offset][1])
                        if (
                            note_candidate
                            and not note_candidate.startswith("-")
                            and not note_candidate.endswith("(")
                            and not is_point_name_candidate(note_candidate)
                            and not is_coordinate_token(note_candidate)
                            and not is_full_date_token(note_candidate)
                        ):
                            note_bits.append(note_candidate)
                    break

    if y_value is None or x_value is None:
        if index + 2 < len(lines):
            y_candidate = clean_text(lines[index + 1][1])
            x_candidate = clean_text(lines[index + 2][1])
            if is_coordinate_token(y_candidate) and is_coordinate_token(x_candidate):
                y_value = parse_number(y_candidate)
                x_value = parse_number(x_candidate)

    note_raw = " | ".join(dict.fromkeys(bit for bit in note_bits if bit)) or None
    return date_raw, y_value, x_value, note_raw


def build_raw_history() -> pd.DataFrame:
    """Extract recoverable history rows from the legacy `.doc` string stream."""

    lines = extract_unicode_strings(DOC_PATH)
    records: list[dict[str, object]] = []
    for index, (line_no, text) in enumerate(lines):
        point_name_raw = clean_text(text)
        if not is_point_name_candidate(point_name_raw):
            continue
        date_raw, y_value, x_value, note_raw = parse_record_context(lines, index)
        point_name_norm = normalize_point_name_token(point_name_raw)
        records.append(
            {
                "site_name_raw": None,
                "site_id": MANUAL_SITE_MAP.get(point_name_norm),
                "base_point_name_raw": point_name_raw,
                "base_point_name_norm": point_name_norm,
                "obs_date_raw": date_raw,
                "obs_date": safe_parse_date(date_raw).isoformat() if is_full_date_token(date_raw) and safe_parse_date(date_raw) else None,
                "y_m": y_value,
                "x_m": x_value,
                "accuracy_m": extract_accuracy(note_raw),
                "note_raw": note_raw,
                "point_status": normalize_point_status(note_raw),
                "is_calculated": False,
                "is_reinstalled": False,
                "is_new": False,
                "is_refined": False,
                "has_uncertain_date": False,
                "needs_manual_review": False,
                "review_reason": None,
                "source_file": SOURCE_FILE,
                "source_row_ref": f"strings_el:{line_no}",
            }
        )

    for extra in SPECIAL_CASES:
        records.append(
            {
                **extra,
                "obs_date": safe_parse_date(extra["obs_date_raw"]).isoformat() if safe_parse_date(extra["obs_date_raw"]) else None,
                "point_status": normalize_point_status(extra["note_raw"]),
                "is_calculated": False,
                "is_reinstalled": False,
                "is_new": False,
                "is_refined": False,
                "has_uncertain_date": False,
                "needs_manual_review": True,
                "source_file": SOURCE_FILE,
            }
        )

    history = pd.DataFrame(records, columns=HISTORY_COLUMNS)
    history = history.drop_duplicates(subset=["source_file", "source_row_ref", "base_point_name_norm", "obs_date_raw", "y_m", "x_m"], keep="first")
    return history


def enrich_history(history: pd.DataFrame) -> pd.DataFrame:
    """Resolve site ids, normalize flags, and mark review cases."""

    if history.empty:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    site_lookup = build_site_lookup()
    history = history.copy()
    history["base_point_name_norm"] = history["base_point_name_norm"].map(normalize_point_name_token)

    manual_lookup = pd.DataFrame(
        [{"base_point_name_norm": key, "site_id_manual": value} for key, value in MANUAL_SITE_MAP.items()]
    ).drop_duplicates()
    history = merge_with_checks(
        history,
        manual_lookup,
        on="base_point_name_norm",
        how="left",
        validate="many_to_one",
        relationship_name="base_points history -> manual site map",
        require_all_left=False,
    )
    history["site_id"] = history["site_id"].combine_first(history["site_id_manual"])
    history = history.drop(columns=["site_id_manual"])

    history = merge_with_checks(
        history,
        site_lookup.rename(columns={"site_id": "site_id_from_shoreline"}),
        on="base_point_name_norm",
        how="left",
        validate="many_to_one",
        relationship_name="base_points history -> shoreline point lookup",
        require_all_left=False,
    )
    history["site_id"] = history["site_id"].combine_first(history["site_id_from_shoreline"])
    history = history.drop(columns=["site_id_from_shoreline"])

    history["site_id"] = history["site_id"].map(resolve_site_id_from_text)
    history["site_name_raw"] = history.apply(
        lambda row: row["site_name_raw"] if clean_text(row["site_name_raw"]) else canonical_site_name(row["site_id"]),
        axis=1,
    )
    history["point_status"] = history["note_raw"].map(normalize_point_status)
    history["is_calculated"] = history["point_status"].eq("calculated")
    history["is_reinstalled"] = history["point_status"].eq("reinstalled")
    history["is_new"] = history["point_status"].eq("new")
    history["is_refined"] = history["point_status"].eq("refined")
    history["has_uncertain_date"] = history.apply(
        lambda row: bool(clean_text(row["obs_date_raw"]) and clean_text(row["obs_date"]) == "") or not clean_text(row["obs_date_raw"]),
        axis=1,
    )
    history["accuracy_m"] = history["accuracy_m"].map(parse_number)
    history["y_m"] = history["y_m"].map(parse_number)
    history["x_m"] = history["x_m"].map(parse_number)

    review_reasons: list[str | None] = []
    for _, row in history.iterrows():
        reasons: list[str] = []
        if not clean_text(row["site_id"]):
            reasons.append("site_id unresolved")
        if row["has_uncertain_date"]:
            reasons.append("date missing or partial in source")
        if clean_text(row["base_point_name_raw"]).endswith("("):
            reasons.append("base point label truncated in extracted text")
        if pd.isna(row["y_m"]) or pd.isna(row["x_m"]):
            reasons.append("coordinate pair incomplete")
        if clean_text(row["source_row_ref"]) == "strings_el:229":
            reasons.append("Urakov Bugor point 3 on 2019-08-14 matches the suspicious Nizhniy Urakov point-3 coordinate cluster")
        review_reasons.append("; ".join(dict.fromkeys(reasons)) if reasons else None)
    history["review_reason"] = history["review_reason"].combine_first(pd.Series(review_reasons, index=history.index))
    history["needs_manual_review"] = history["review_reason"].notna()

    return history[HISTORY_COLUMNS].sort_values(["site_id", "base_point_name_norm", "obs_date", "source_row_ref"], na_position="last").reset_index(drop=True)


def build_current(history: pd.DataFrame) -> pd.DataFrame:
    """Choose one current coordinate set per site/base-point combination."""

    if history.empty:
        return pd.DataFrame(columns=HISTORY_COLUMNS)

    current = history.copy()
    current["obs_date_dt"] = pd.to_datetime(current["obs_date"], errors="coerce")
    current["selection_rank"] = list(
        zip(
            current["obs_date_dt"].notna().astype(int),
            current["obs_date_dt"].fillna(pd.Timestamp("1900-01-01")),
            (~current["has_uncertain_date"]).astype(int),
            (~current["is_calculated"]).astype(int),
            (~current["needs_manual_review"]).astype(int),
        )
    )
    current = current.sort_values(["site_id", "base_point_name_norm", "selection_rank", "source_row_ref"])
    current = current.groupby(["site_id", "base_point_name_norm"], dropna=True, as_index=False).tail(1)
    return current[HISTORY_COLUMNS].sort_values(["site_id", "base_point_name_norm"]).reset_index(drop=True)


def compatibility_base_points(current: pd.DataFrame) -> pd.DataFrame:
    """Build a backward-compatible `base_points.csv` alias from current rows."""

    compat = current.copy()
    compat.insert(
        0,
        "base_point_id",
        compat.apply(
            lambda row: f"{row['site_id']}_{re.sub(r'[^a-z0-9]+', '_', clean_text(row['base_point_name_norm']).lower()).strip('_')}_{clean_text(row['obs_date']) or 'undated'}",
            axis=1,
        ),
    )
    return compat


def write_outputs(history: pd.DataFrame, current: pd.DataFrame) -> None:
    """Write processed and review outputs."""

    history_path = PROCESSED_DIR / "base_points_history.csv"
    current_path = PROCESSED_DIR / "base_points_current.csv"
    compat_path = PROCESSED_DIR / "base_points.csv"
    review_path = REPORTS_DIR / "tables" / "base_points_manual_review.csv"

    history_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.parent.mkdir(parents=True, exist_ok=True)

    history.to_csv(history_path, index=False)
    current.to_csv(current_path, index=False)
    compatibility_base_points(current).to_csv(compat_path, index=False)
    history.loc[history["needs_manual_review"]].to_csv(review_path, index=False)


def build_base_points_stub(output_path: Path | None = None) -> Path:
    """Build base-point history/current layers and a compatibility current alias."""

    logger = setup_logging("build_base_points")
    history = enrich_history(build_raw_history())
    current = build_current(history)
    write_outputs(history, current)

    output_path = output_path or PROCESSED_DIR / "base_points.csv"
    logger.info("Built %s with %s history rows", relative_to_root(PROCESSED_DIR / "base_points_history.csv"), len(history))
    logger.info("Built %s with %s current rows", relative_to_root(PROCESSED_DIR / "base_points_current.csv"), len(current))
    logger.info("Built %s with %s review rows", relative_to_root(REPORTS_DIR / "tables" / "base_points_manual_review.csv"), int(history["needs_manual_review"].sum()))
    logger.info("Updated compatibility alias at %s", relative_to_root(output_path))
    return output_path


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PROCESSED_DIR / "base_points.csv")
    args = parser.parse_args()
    build_base_points_stub(output_path=args.output)


if __name__ == "__main__":
    main()

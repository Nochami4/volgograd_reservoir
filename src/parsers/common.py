"""Common helpers for reproducible ETL parsers."""

from __future__ import annotations

import difflib
import logging
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from dateutil import parser as date_parser

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"

CANONICAL_SITE_NAMES = {
    "бережновка": "Бережновка",
    "бурты": "Бурты",
    "молчановка": "Молчановка",
    "нижний балыклей": "Нижний Балыклей",
    "нижний ураков": "Нижний Ураков",
    "новоникольское": "Новоникольское",
    "пичуга-южный": "Пичуга-Южный",
    "пролейский": "Пролейский",
    "суводская": "Суводская",
    "ураков бугов": "Ураков Бугор",
    "ураков бугор": "Ураков Бугор",
}

SITE_ID_MAP = {
    "Бережновка": "berezhnovka",
    "Бурты": "burty",
    "Молчановка": "molchanovka",
    "Нижний Балыклей": "nizhniy_balykley",
    "Нижний Ураков": "nizhniy_urakov",
    "Новоникольское": "novonikolskoe",
    "Пичуга-Южный": "pichuga_yuzhny",
    "Пролейский": "proleyskiy",
    "Суводская": "suvodskaya",
    "Ураков Бугор": "urakov_bugor",
}

SITE_NAME_ALIASES = {
    "пичуга южный": "pichuga_yuzhny",
    "пичуга-южный": "pichuga_yuzhny",
    "нижний ураков": "nizhniy_urakov",
    "нижний балыклей": "nizhniy_balykley",
    "новоникольское": "novonikolskoe",
    "пролейский": "proleyskiy",
    "суводская": "suvodskaya",
    "молчановка": "molchanovka",
    "бережновка": "berezhnovka",
    "бурты": "burty",
    "ураков бугор": "urakov_bugor",
    "ураков бугов": "urakov_bugor",
}

ORIENTATION_TO_DEGREES = {
    "север": 0.0,
    "с": 0.0,
    "северо-восток": 45.0,
    "севоро-восток": 45.0,
    "св": 45.0,
    "восток": 90.0,
    "в": 90.0,
    "юго-восток": 135.0,
    "юв": 135.0,
    "юг": 180.0,
    "ю": 180.0,
    "юго-запад": 225.0,
    "юз": 225.0,
    "запад": 270.0,
    "з": 270.0,
    "северо-запад": 315.0,
    "сз": 315.0,
}

WIND_DIRECTION_TO_DEGREES = {
    "с": 0.0,
    "ссв": 22.5,
    "св": 45.0,
    "всв": 67.5,
    "в": 90.0,
    "вюв": 112.5,
    "юв": 135.0,
    "ююв": 157.5,
    "ю": 180.0,
    "ююз": 202.5,
    "юз": 225.0,
    "зюз": 247.5,
    "з": 270.0,
    "зсз": 292.5,
    "сз": 315.0,
    "ссз": 337.5,
    "штиль": 0.0,
}

MISSING_TOKENS = {"", "-", "—", "*", "nan", "none", "null"}
MIN_REASONABLE_WIND_YEAR = 1950
MAX_REASONABLE_WIND_YEAR = 2025


def ensure_directory(path: Path) -> Path:
    """Create a directory if needed and return it."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def relative_to_root(path: Path) -> str:
    """Return a project-root relative POSIX path."""

    return path.resolve().relative_to(PROJECT_ROOT).as_posix()


def find_input_file(directory: Path, pattern: str) -> Path:
    """Find a required input file or raise a helpful error."""

    matches = sorted(directory.glob(pattern))
    if matches:
        return matches[0]

    candidates = sorted(p.name for p in directory.iterdir() if p.is_file())
    closest = difflib.get_close_matches(pattern.replace("*", ""), candidates, n=1)
    suggestion = f" Closest match: {closest[0]!r}." if closest else ""
    raise FileNotFoundError(f"Expected input matching {pattern!r} in {directory}.{suggestion}")


def setup_logging(name: str, log_path: Path | None = None) -> logging.Logger:
    """Create a logger writing to stderr and optionally to a file."""

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_path is not None:
        ensure_directory(log_path.parent)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def clean_text(value: object) -> str:
    """Normalize cell values into compact text."""

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def normalize_site_name(name: object) -> str:
    """Return a canonical site name when the alias is known."""

    text = clean_text(name)
    lowered = text.lower()
    return CANONICAL_SITE_NAMES.get(lowered, text)


def normalize_site_id(name: object) -> str:
    """Normalize a site name to a stable site_id."""

    canonical = normalize_site_name(name)
    if canonical in SITE_ID_MAP:
        return SITE_ID_MAP[canonical]

    fallback = transliterate(canonical.lower())
    fallback = re.sub(r"[^a-z0-9]+", "_", fallback).strip("_")
    if not fallback:
        raise ValueError(f"Could not normalize site id for {name!r}")
    return fallback


def resolve_site_id_from_text(name: object) -> str | None:
    """Resolve a site id from either a site id, canonical name, or known alias."""

    text = clean_text(name)
    if not text:
        return None

    if text in SITE_ID_MAP.values():
        return text

    normalized_text = re.sub(r"[\s_-]+", " ", text.lower()).strip()
    if normalized_text in SITE_NAME_ALIASES:
        return SITE_NAME_ALIASES[normalized_text]

    try:
        return normalize_site_id(text)
    except ValueError:
        return None


def transliterate(text: str) -> str:
    """Transliterate Cyrillic text to ASCII for ids."""

    mapping = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "y",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "kh",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
    return "".join(mapping.get(char, char) for char in text)


def parse_number(value: object) -> float | None:
    """Parse numeric values with Russian decimal commas without guessing units."""

    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)

    text = clean_text(value)
    if text.lower() in MISSING_TOKENS:
        return None

    candidate = text.replace(" ", "")
    candidate = candidate.replace(",", ".")
    if candidate.endswith("."):
        candidate = candidate[:-1]

    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[Ee][+-]?\d+)?", candidate):
        return None

    try:
        return float(candidate)
    except ValueError:
        return None


def safe_parse_date(value: object) -> date | None:
    """Parse Excel serials, Python dates, and common text date formats."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        numeric = float(value)
        if 10_000 <= numeric <= 90_000:
            return (datetime(1899, 12, 30) + timedelta(days=numeric)).date()
        return None

    text = clean_text(value)
    if text.lower() in MISSING_TOKENS:
        return None

    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    try:
        return date_parser.parse(text, dayfirst=True, fuzzy=False).date()
    except (ValueError, OverflowError):
        return None


def combine_date_and_hour(obs_date: date | None, hour_value: object) -> datetime | None:
    """Build a datetime when date and hour are both available."""

    if obs_date is None:
        return None
    hour = parse_number(hour_value)
    if hour is None:
        return None
    hour_int = int(hour)
    if not 0 <= hour_int <= 23:
        return None
    return datetime.combine(obs_date, time(hour=hour_int))


def normalize_wind_direction(value: object) -> tuple[str, float | None]:
    """Normalize textual wind direction and return an approximate azimuth."""

    text = clean_text(value)
    if not text:
        return "", None
    normalized = (
        text.lower()
        .replace("сев.", "с")
        .replace("сев", "с")
        .replace("юж.", "ю")
        .replace("юж", "ю")
        .replace("вост.", "в")
        .replace("вост", "в")
        .replace("зап.", "з")
        .replace("зап", "з")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
    )
    return text, WIND_DIRECTION_TO_DEGREES.get(normalized)


def is_reasonable_wind_year(year: object) -> bool:
    """Check whether a year is within the accepted wind-observation range."""

    numeric = parse_number(year)
    if numeric is None:
        return False
    year_int = int(numeric)
    return MIN_REASONABLE_WIND_YEAR <= year_int <= MAX_REASONABLE_WIND_YEAR


def orientation_to_deg(value: object) -> float | None:
    """Convert a textual shoreline orientation into degrees when explicit."""

    text = clean_text(value).lower().replace(" ", "")
    return ORIENTATION_TO_DEGREES.get(text)


def make_profile_id(site_id: str, profile_num: str | int | None, profile_name: str) -> str:
    """Create a stable profile identifier."""

    if profile_num is not None and clean_text(profile_num):
        suffix = re.sub(r"[^0-9a-z]+", "_", clean_text(profile_num).lower()).strip("_")
    else:
        suffix = re.sub(r"[^0-9a-z]+", "_", transliterate(profile_name.lower())).strip("_")
    return f"{site_id}_profile_{suffix}"


def profile_number_from_header(header_text: str) -> str | None:
    """Extract a profile number or symbolic label from a header."""

    match = re.search(r"ПРОФИЛ[ЬЯ]?\s*№?\s*([0-9]+)", header_text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    if "ПРОФИЛЬ МЕЖДУ" in header_text.upper():
        return None
    return None


def join_nonempty(values: Iterable[object], sep: str = " | ") -> str:
    """Join non-empty stringified values."""

    cleaned = [clean_text(value) for value in values if clean_text(value)]
    return sep.join(cleaned)


def first_existing(paths: Iterable[Path]) -> Path | None:
    """Return the first path that exists."""

    for path in paths:
        if path.exists():
            return path
    return None


def merge_with_checks(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str | list[str],
    how: str,
    validate: str,
    relationship_name: str,
    require_all_left: bool = True,
    suffixes: tuple[str, str] = ("", "_right"),
) -> pd.DataFrame:
    """Merge two dataframes and fail loudly when the join contract is broken."""

    merged = left.merge(
        right,
        on=on,
        how=how,
        validate=validate,
        suffixes=suffixes,
        indicator=True,
    )
    if require_all_left:
        unmatched = merged["_merge"].ne("both")
        if unmatched.any():
            sample = merged.loc[unmatched, on].head(5)
            if isinstance(sample, pd.Series):
                sample_payload = sample.astype(str).tolist()
            else:
                sample_payload = sample.to_dict(orient="records")
            raise ValueError(
                f"Merge check failed for {relationship_name}: {int(unmatched.sum())} left row(s) did not match. "
                f"Sample keys: {sample_payload}"
            )
    return merged.drop(columns=["_merge"])

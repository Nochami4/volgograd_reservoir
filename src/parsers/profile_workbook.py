"""Helpers for extracting shoreline profile blocks from the legacy XLS workbook."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import xlrd

from .common import (
    clean_text,
    join_nonempty,
    make_profile_id,
    normalize_site_id,
    normalize_site_name,
    parse_number,
    profile_number_from_header,
    relative_to_root,
    safe_parse_date,
)


@dataclass(frozen=True)
class ProfileBlock:
    """Metadata describing one profile block inside the workbook."""

    site_name: str
    site_id: str
    profile_name: str
    profile_num: str | None
    profile_id: str
    sheet_name: str
    source_file: str
    start_col: int
    end_col: int
    note_col_start: int
    data_start_row: int
    data_end_row: int


@dataclass(frozen=True)
class BlockRow:
    """One raw row extracted from a profile block."""

    block: ProfileBlock
    source_row: int
    obs_date_text: str
    obs_date: object
    measured_point_name: object
    raw_measured_distance: object
    pn_name: object
    gp_to_pn_offset: object
    brow_position_pn: object
    note_text: str
    raw_values_text: str


def open_profile_workbook(path: Path) -> xlrd.book.Book:
    """Open the legacy XLS workbook."""

    return xlrd.open_workbook(path)


def iter_profile_blocks(path: Path) -> Iterator[ProfileBlock]:
    """Yield profile blocks inferred from the first row of each site sheet."""

    workbook = open_profile_workbook(path)
    source_file = relative_to_root(path)
    for sheet_name in workbook.sheet_names():
        if sheet_name in {"Лист1", "Лист2"}:
            continue
        sheet = workbook.sheet_by_name(sheet_name)
        if sheet.nrows == 0:
            continue

        raw_site_name = normalize_site_name(sheet_name)
        site_id = normalize_site_id(raw_site_name)
        first_row = sheet.row_values(0)
        block_starts = [
            index
            for index, value in enumerate(first_row)
            if clean_text(value).upper().startswith("ПРОФИЛЬ")
        ]
        if not block_starts:
            continue

        note_col_start = next(
            (index for index, value in enumerate(first_row) if "ПРИМЕЧАН" in clean_text(value).upper()),
            sheet.ncols,
        )
        for position, start_col in enumerate(block_starts):
            header = clean_text(first_row[start_col])
            end_col = block_starts[position + 1] if position + 1 < len(block_starts) else note_col_start
            end_col = min(end_col, start_col + 6)
            profile_num = profile_number_from_header(header)
            profile_id = make_profile_id(site_id, profile_num, header)
            yield ProfileBlock(
                site_name=raw_site_name,
                site_id=site_id,
                profile_name=header,
                profile_num=profile_num,
                profile_id=profile_id,
                sheet_name=sheet_name,
                source_file=source_file,
                start_col=start_col,
                end_col=end_col,
                note_col_start=note_col_start,
                data_start_row=4,
                data_end_row=sheet.nrows,
            )


def iter_block_rows(path: Path) -> Iterator[BlockRow]:
    """Yield raw rows for each profile block in the workbook."""

    workbook = open_profile_workbook(path)
    sheets = {sheet_name: workbook.sheet_by_name(sheet_name) for sheet_name in workbook.sheet_names()}

    for block in iter_profile_blocks(path):
        sheet = sheets[block.sheet_name]
        for row_idx in range(block.data_start_row, block.data_end_row):
            row_values = sheet.row_values(row_idx)
            block_values = row_values[block.start_col : block.start_col + 6]
            note_values = row_values[block.note_col_start :] if block.note_col_start < sheet.ncols else []
            if not any(clean_text(value) for value in block_values + note_values):
                continue

            raw_values_text = join_nonempty(block_values)
            yield BlockRow(
                block=block,
                source_row=row_idx + 1,
                obs_date_text=clean_text(block_values[0]) if len(block_values) > 0 else "",
                obs_date=block_values[0] if len(block_values) > 0 else None,
                measured_point_name=block_values[1] if len(block_values) > 1 else None,
                raw_measured_distance=block_values[2] if len(block_values) > 2 else None,
                pn_name=block_values[3] if len(block_values) > 3 else None,
                gp_to_pn_offset=block_values[4] if len(block_values) > 4 else None,
                brow_position_pn=block_values[5] if len(block_values) > 5 else None,
                note_text=join_nonempty(note_values, sep=" "),
                raw_values_text=raw_values_text,
            )


def summarize_profile_block(path: Path, block: ProfileBlock) -> dict[str, object]:
    """Build profile-level metadata from a block."""

    workbook = open_profile_workbook(path)
    sheet = workbook.sheet_by_name(block.sheet_name)
    dates = []
    row_count = 0

    for row_idx in range(block.data_start_row, block.data_end_row):
        row_values = sheet.row_values(row_idx)
        block_values = row_values[block.start_col : block.start_col + 6]
        if not any(clean_text(value) for value in block_values):
            continue
        row_count += 1
        parsed_date = safe_parse_date(block_values[0] if block_values else None)
        if parsed_date is not None:
            dates.append(parsed_date)

    return {
        "site_id": block.site_id,
        "profile_id": block.profile_id,
        "profile_num": block.profile_num,
        "profile_name": block.profile_name,
        "sheet_name_raw": block.sheet_name,
        "start_date": min(dates).isoformat() if dates else None,
        "end_date": max(dates).isoformat() if dates else None,
        "n_observations": row_count,
        "source_file": block.source_file,
    }


def block_row_to_numeric_fields(row: BlockRow) -> dict[str, float | None]:
    """Parse numeric columns from a raw block row."""

    raw_distance = parse_number(row.raw_measured_distance)
    brow_position_pn = parse_number(row.brow_position_pn)
    return {
        "raw_measured_distance_m": raw_distance,
        "gp_to_pn_offset_m": parse_number(row.gp_to_pn_offset),
        "brow_position_pn_m": brow_position_pn,
        "brow_position_raw_m": raw_distance,
    }

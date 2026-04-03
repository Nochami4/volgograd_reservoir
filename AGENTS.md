# Project Rules For Future Agents

- Do not modify files under `data/raw/`.
- Build every dataset with scripts and CLI entrypoints, never by hand-editing outputs.
- Do not impute missing raw values or invent replacements.
- Keep every conclusion reproducible from committed code and raw inputs.
- Write intermediate outputs only to `data/interim/`.
- Write final derived outputs only to `data/processed/`.
- Preserve explicit provenance fields such as `source_file`, `source_sheet`, and `source_row` whenever applicable.
- When file structure is ambiguous, keep `qc_note` or `TODO` markers instead of guessing semantic meaning.

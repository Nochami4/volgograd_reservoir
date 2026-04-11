# Base Points Manual Template

Status: partially manual workflow.

What this file contains:
- Existing manual edits are preserved across pipeline runs.
- Semi-automatic candidate rows may be appended from UTF-16 string extraction of the legacy `.doc` files.
- Every candidate row must be manually checked against the source document before scientific use.

How to fill the columns:
- `base_point_name`: preserve the source spelling of the point identifier.
- `obs_date`: use ISO date only when the source gives an explicit full date.
- `x_m`, `y_m`: copy coordinates exactly as given; do not transform units or CRS silently.
- `accuracy_m`: fill only when the source explicitly states measurement accuracy.
- `point_status`: allowed normalized values are `new`, `reinstalled`, `calculated`, `refined`, `original`, `unknown`.
- `status_note`: keep the original wording that justifies the status.
- `site_id`: fill manually when not resolved automatically.
- `qc_flag`: keep parser flags and append your own transparent tags such as `MANUAL_VERIFIED` when review is complete.
- `qc_note`: explain why a `site_id`, status, or date remains uncertain.
- `source_row_ref`: keep a stable reference such as `strings_el:115` or a manual note like `page 2, paragraph 3`.

Status normalization rules used by the parser:
- `Новый` -> `new`
- `Переустановлен` -> `reinstalled`
- `Расчетные координаты` / `Расчётные координаты` -> `calculated`
- `Уточнены` / `Уточнение координат` -> `refined`
- otherwise -> `original` or `unknown`

Important:
- Do not invent coordinates or dates.
- Leave uncertain values blank and explain uncertainty in `qc_note` or `status_note`.
- Rows with `SITE_ID_UNRESOLVED` are expected until the point-to-site mapping is confirmed manually.
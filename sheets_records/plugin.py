from dataclasses import dataclass
from typing import (Dict, List, Optional)

from webapp.job_runner_container.shared import pmap
from webapp.job_runner_container.work_unit_context import map_work_unit


@dataclass
class GoogleSheetColumnSummary:
    column_name: str
    values_preview: List[str]


@dataclass
class GoogleSheetSummary:
    sheet_name: str
    columns: List[GoogleSheetColumnSummary]


def _make_columns_summary(
    rows: List[List[GoogleSheetCell]],
) -> List[GoogleSheetColumnSummary]:
    if len(rows) == 0:
        return []
    header_row = rows[0]
    return [
        GoogleSheetColumnSummary(
            column_name=header_cell.as_str(),
            values_preview=[
                rows[i][column_index].as_str() for i in range(1, min(6, len(rows)))
            ],
        )
        for column_index, header_cell in enumerate(header_row)
    ]


async def google_sheets_get_summary(
    google_sheet_id: GoogleSheetID,
) -> List[GoogleSheetSummary]:
    """
    Returns a summary of the structure of `google_sheet_id`.

    The first row in each sheet is treated as a header row that specifies column names.
    `values_preview` contains the column's values in the next 5 rows.
    """
    spreadsheet = await actions_v0.google_sheets_read_spreadsheet(google_sheet_id)
    return [
        GoogleSheetSummary(
            sheet_name=sheet_name,
            columns=_make_columns_summary(spreadsheet.sheet_data[sheet_name]),
        )
        for sheet_name in spreadsheet.metadata.sheet_names
    ]


def _get_sheet_name_and_data(
    spreadsheet: GoogleSpreadsheet, sheet_name: Optional[str]
) -> Tuple[str, List[List[GoogleSheetCell]]]:
    if sheet_name is None:
        if len(spreadsheet.metadata.sheet_names) == 0:
            raise ValueError("Google Sheet has no sheets")
        sheet_name = spreadsheet.metadata.sheet_names[0]
    return sheet_name, spreadsheet.sheet_data[sheet_name]


def _get_column_indexes(rows: List[List[GoogleSheetCell]]) -> Dict[str, int]:
    """
    Return a mapping from column name to index.

    If there are duplicate column names, use the first column.
    """
    if len(rows) == 0:
        return {}
    column_indexes = {}
    for i, cell in enumerate(rows[0]):
        column_name = cell.as_str()
        if column_name not in column_indexes:
            column_indexes[column_name] = i
    return column_indexes


async def google_sheets_add_empty_columns(
    google_sheet_id: GoogleSheetID,
    column_names: List[str],
    sheet_name: Optional[str] = None,
) -> None:
    """
    Add empty named columns to a sheet in `google_sheet_id`.

    The first row is treated as a header row that specifies column names,
    so this action adds each of `column_names` to the header row.

    If `sheet_name` is specified, operates on that sheet (tab) of the spreadsheet. If not, operates on the first sheet.
    """
    if len(column_names) == 0:
        return

    spreadsheet = await actions_v0.google_sheets_read_spreadsheet(google_sheet_id)
    sheet_name, sheet_data = _get_sheet_name_and_data(spreadsheet, sheet_name)

    if len(sheet_data) == 0:
        start = await actions_v0.google_sheet_cell_compute_offset(None, 0, 0)
    else:
        start = await actions_v0.google_sheet_cell_compute_offset(
            sheet_data[0][-1], 0, 1
        )

    await actions_v0.google_sheets_update_values(
        google_sheet_id=google_sheet_id,
        sheet_name=sheet_name,
        cell_range=actions_v0.GoogleSheetCellRange(start=start),
        data=[column_names],
    )


async def google_sheets_parallel_update_rows(
    google_sheet_id: GoogleSheetID,
    f: Callable[[Dict[str, str]], None],
    limit: int,
    sheet_name: Optional[str] = None,
) -> None:
    """
    Update rows in a sheet in `google_sheet_id`.

    The first row is treated as a header row that specifies column names.

    For each row after the header, up to `limit` rows, `f` is called in parallel. `f` receives a
    mapping from column name to value, and `f` may mutate the values in its argument to change the
    values in the spreadsheet row. `f` must not add or remove any keys. `f` must return `None`.

    If `sheet_name` is specified, operates on that sheet (tab) of the spreadsheet. If not, operates on the first sheet.
    """
    spreadsheet = await actions_v0.google_sheets_read_spreadsheet(google_sheet_id)
    sheet_name, sheet_data = _get_sheet_name_and_data(spreadsheet, sheet_name)
    column_indexes = _get_column_indexes(sheet_data)

    async def update_row(row_index: int) -> None:
        row = sheet_data[row_index]
        row_dict = {
            column_name: row[i].as_str() if i < len(row) else ""
            for column_name, i in column_indexes.items()
        }
        old_row_dict = dict(row_dict)

        # TODO: `f` seems to be a coroutine, so I await it. Is it guaranteed to be a coroutine?
        result = await f(row_dict)

        # Despite the types and docstring, the LLM sometimes decides to return an update instead of
        # mutating. Attempt to catch this at runtime.
        if result is not None:
            raise ValueError("`f` must return `None`.")

        added_keys = row_dict.keys() - old_row_dict.keys()
        removed_keys = old_row_dict.keys() - row_dict.keys()
        if len(added_keys) > 0 or len(removed_keys) > 0:
            raise ValueError(
                f"`f` changed its argument's keys. Added: {added_keys}. Removed: {removed_keys}."
            )

        for column_name, old_value in old_row_dict.items():
            # TODO: We could collapse contiguous updates. Or even do a single non-contiguous update if the sheets API supports that.
            new_value = row_dict[column_name]
            if new_value == old_value:
                continue
            column_index = column_indexes[column_name]
            await actions_v0.google_sheets_update_values(
                google_sheet_id=google_sheet_id,
                sheet_name=sheet_name,
                cell_range=actions_v0.GoogleSheetCellRange(
                    start=await actions_v0.google_sheet_cell_compute_offset(
                        None, row_index, column_index
                    )
                ),
                data=[[new_value]],
            )

    async def update_row_with_context(row_index: int) -> None:
        with map_work_unit(str(row_index)):
            await update_row(row_index)

    await pmap.DEFAULT.pmap(update_row_with_context, range(1, min(1 + limit, len(sheet_data))))

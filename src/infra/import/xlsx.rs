use std::path::Path;

use anyhow::{Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use rusqlite::params;

use crate::infra::sqlite::queries::insert_header_names;
use crate::infra::sqlite::schema::{init_db, open_connection};
use crate::{HoldingsTransform, ImportResult};

#[allow(dead_code)]
pub fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::String(v) => v.to_string(),
        Data::Float(v) => v.to_string(),
        Data::Int(v) => v.to_string(),
        Data::Bool(v) => v.to_string(),
        Data::DateTime(v) => v.to_string(),
        Data::DateTimeIso(v) => v.to_string(),
        Data::DurationIso(v) => v.to_string(),
        Data::Error(v) => format!("{v:?}"),
        Data::Empty => String::new(),
    }
}

#[allow(dead_code)]
pub fn import_xlsx_selected_sheets_to_sqlite(
    db_path: &Path,
    xlsx_path: &Path,
) -> Result<Vec<ImportResult>> {
    init_db(db_path)?;

    let mut workbook = open_workbook_auto(xlsx_path)
        .with_context(|| format!("failed to open xlsx: {}", xlsx_path.display()))?;
    let source_path = xlsx_path.to_string_lossy().into_owned();

    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start xlsx import transaction")?;

    let assets_range = workbook
        .worksheet_range("資產總表")
        .context("failed to read sheet: 資產總表")?;
    let holdings_range = workbook
        .worksheet_range("持股明細")
        .context("failed to read sheet: 持股明細")?;
    let dividends_range = workbook
        .worksheet_range("股息收入明細表")
        .context("failed to read sheet: 股息收入明細表")?;

    let assets_rows: Vec<Vec<String>> = assets_range
        .rows()
        .skip(3)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();
    let holdings_rows: Vec<Vec<String>> = holdings_range
        .rows()
        .skip(2)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();
    let dividends_rows: Vec<Vec<String>> = dividends_range
        .rows()
        .skip(1)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();

    let holdings = crate::transform_holdings_sheet(&holdings_rows);
    let (assets_headers, assets_data) =
        crate::transform_assets_sheet(&assets_rows, holdings.total_cost, holdings.total_net);
    let (_dividend_headers, dividend_data) =
        crate::transform_dividend_sheet(&dividends_rows, &holdings.by_code);
    let (merged_headers, merged_data) =
        crate::merge_holdings_and_dividends(holdings.headers, holdings.rows, &dividend_data);

    let transformed = vec![
        ("資產總表", assets_headers, assets_data),
        ("持股股息總表", merged_headers, merged_data),
    ];

    let mut imported = Vec::new();
    for (sheet_name, headers, rows) in transformed {
        tx.execute(
            "INSERT INTO dataset(name, source_path, row_count) VALUES (?1, ?2, 0)",
            params![sheet_name, format!("{source_path}#{sheet_name}")],
        )
        .with_context(|| format!("failed to insert dataset for sheet: {sheet_name}"))?;
        let dataset_id = tx.last_insert_rowid();

        insert_header_names(&tx, dataset_id, &headers)?;

        let mut insert_cell = tx
            .prepare(
                "INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)",
            )
            .context("failed to prepare xlsx cell insert")?;

        for (row_idx, row) in rows.iter().enumerate() {
            for (col_idx, value) in row.iter().enumerate() {
                insert_cell
                    .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                    .context("failed to insert transformed xlsx cell")?;
            }
        }
        drop(insert_cell);

        let row_count = rows.len() as i64;
        tx.execute(
            "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
            params![row_count, dataset_id],
        )
        .context("failed to update xlsx dataset row_count")?;

        imported.push(ImportResult {
            dataset_id,
            row_count,
        });
    }

    tx.commit()
        .context("failed to commit xlsx import transaction")?;

    Ok(imported)
}

#[allow(dead_code)]
pub fn holdings_transform_placeholder(_t: &HoldingsTransform) {}

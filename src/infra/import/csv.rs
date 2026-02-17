use std::path::Path;

use anyhow::{Context, Result};
use csv::StringRecord;
use rusqlite::params;

use crate::infra::sqlite::queries::insert_headers;
use crate::infra::sqlite::schema::{init_db, open_connection};
use crate::ImportResult;

#[allow(dead_code)]
pub fn import_csv_to_sqlite(db_path: &Path, csv_path: &Path) -> Result<ImportResult> {
    init_db(db_path)?;

    let mut reader = csv::Reader::from_path(csv_path)
        .with_context(|| format!("failed to open csv: {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from csv: {}", csv_path.display()))?
        .clone();

    if headers.is_empty() {
        anyhow::bail!("csv header is required")
    }

    let source_path = csv_path.to_string_lossy().into_owned();
    let dataset_name = csv_path
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("dataset")
        .to_string();

    let mut conn = open_connection(db_path)?;
    let tx = conn.transaction().context("failed to start transaction")?;

    tx.execute(
        "INSERT INTO dataset(name, source_path, row_count) VALUES (?1, ?2, 0)",
        params![dataset_name, source_path],
    )
    .context("failed to insert dataset")?;
    let dataset_id = tx.last_insert_rowid();

    insert_headers(&tx, dataset_id, &headers)?;

    let mut insert_cell = tx
        .prepare("INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)")
        .context("failed to prepare cell insert")?;

    let mut row_count = 0_i64;
    let header_len = headers.len();
    for (row_idx, record) in reader.records().enumerate() {
        let record = record.context("failed to parse csv record")?;
        for col_idx in 0..header_len {
            let value = record.get(col_idx).unwrap_or("");
            insert_cell
                .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                .context("failed to insert cell")?;
        }
        row_count += 1;
    }
    drop(insert_cell);

    tx.execute(
        "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
        params![row_count, dataset_id],
    )
    .context("failed to update dataset row_count")?;

    tx.commit().context("failed to commit import transaction")?;

    Ok(ImportResult {
        dataset_id,
        row_count,
    })
}

#[allow(dead_code)]
pub fn csv_headers_placeholder(_headers: &StringRecord) {}

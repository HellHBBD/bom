use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use csv::StringRecord;
use rusqlite::{params, types::Value};

use crate::domain::entities::edit::CellKey;
use crate::infra::sqlite::schema::{init_db, open_connection};
use crate::usecase::ports::repo::DatasetMeta;
use crate::QueryOptions;

type ReloadPageResult = (Vec<String>, Vec<Vec<String>>, i64, i64);

#[allow(dead_code)]
pub fn insert_headers(
    tx: &rusqlite::Transaction<'_>,
    dataset_id: i64,
    headers: &StringRecord,
) -> Result<()> {
    let mut insert_header = tx
        .prepare("INSERT INTO column_name(dataset_id, col_idx, name) VALUES (?1, ?2, ?3)")
        .context("failed to prepare header insert")?;

    for (col_idx, name) in headers.iter().enumerate() {
        insert_header
            .execute(params![dataset_id, col_idx as i64, name])
            .context("failed to insert header")?;
    }

    Ok(())
}

#[allow(dead_code)]
pub fn insert_header_names(
    tx: &rusqlite::Transaction<'_>,
    dataset_id: i64,
    headers: &[String],
) -> Result<()> {
    let mut insert_header = tx
        .prepare("INSERT INTO column_name(dataset_id, col_idx, name) VALUES (?1, ?2, ?3)")
        .context("failed to prepare header insert")?;

    for (col_idx, name) in headers.iter().enumerate() {
        insert_header
            .execute(params![dataset_id, col_idx as i64, name])
            .context("failed to insert header")?;
    }

    Ok(())
}

#[allow(dead_code)]
pub fn upsert_column_visibility(
    db_path: &Path,
    dataset_id: i64,
    visibility: &BTreeMap<i64, bool>,
) -> Result<()> {
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start column visibility transaction")?;

    tx.execute(
        "DELETE FROM column_visibility WHERE dataset_id = ?1",
        [dataset_id],
    )
    .context("failed to clear existing column visibility")?;

    let mut insert_stmt = tx
        .prepare(
            "INSERT INTO column_visibility(dataset_id, col_idx, visible)
             VALUES (?1, ?2, ?3)",
        )
        .context("failed to prepare column visibility insert")?;

    for (col_idx, visible) in visibility {
        let value = if *visible { 1 } else { 0 };
        insert_stmt
            .execute(params![dataset_id, *col_idx, value])
            .context("failed to insert column visibility")?;
    }

    drop(insert_stmt);
    tx.commit()
        .context("failed to commit column visibility updates")?;
    Ok(())
}

#[allow(dead_code)]
pub fn load_column_visibility(db_path: &Path, dataset_id: i64) -> Result<BTreeMap<i64, bool>> {
    let conn = open_connection(db_path)?;
    let mut stmt = conn
        .prepare(
            "SELECT col_idx, visible
             FROM column_visibility
             WHERE dataset_id = ?1
             ORDER BY col_idx ASC",
        )
        .context("failed to prepare column visibility query")?;

    let visibility_iter = stmt
        .query_map([dataset_id], |row| {
            let col_idx: i64 = row.get(0)?;
            let visible: i64 = row.get(1)?;
            Ok((col_idx, visible != 0))
        })
        .context("failed to query column visibility")?;

    let mut visibility = BTreeMap::new();
    for item in visibility_iter {
        let (col_idx, visible) = item.context("failed to read column visibility row")?;
        visibility.insert(col_idx, visible);
    }

    Ok(visibility)
}

#[allow(dead_code)]
pub fn upsert_holdings_flag(db_path: &Path, dataset_id: i64, is_holdings: bool) -> Result<()> {
    let conn = open_connection(db_path)?;
    let value = if is_holdings { 1 } else { 0 };
    conn.execute(
        "INSERT INTO dataset_flag(dataset_id, is_holdings)
         VALUES (?1, ?2)
         ON CONFLICT(dataset_id) DO UPDATE SET is_holdings = excluded.is_holdings",
        params![dataset_id, value],
    )
    .context("failed to upsert holdings flag")?;
    Ok(())
}

#[allow(dead_code)]
pub fn load_holdings_flags(db_path: &Path) -> Result<BTreeMap<i64, bool>> {
    let conn = open_connection(db_path)?;
    let mut stmt = conn
        .prepare(
            "SELECT dataset_id, is_holdings
             FROM dataset_flag",
        )
        .context("failed to prepare holdings flag query")?;

    let mut flags = BTreeMap::new();
    let rows = stmt
        .query_map([], |row| {
            let dataset_id: i64 = row.get(0)?;
            let is_holdings: i64 = row.get(1)?;
            Ok((dataset_id, is_holdings != 0))
        })
        .context("failed to query holdings flags")?;

    for row in rows {
        let (dataset_id, is_holdings) = row.context("failed to read holdings flag row")?;
        flags.insert(dataset_id, is_holdings);
    }

    Ok(flags)
}

#[allow(dead_code)]
pub fn rename_dataset(db_path: &Path, dataset_id: i64, name: &str) -> Result<()> {
    let conn = open_connection(db_path)?;
    conn.execute(
        "UPDATE dataset SET name = ?1 WHERE id = ?2",
        params![name, dataset_id],
    )
    .context("failed to rename dataset")?;
    Ok(())
}

#[allow(dead_code)]
pub fn query_page(
    db_path: &Path,
    dataset_id: i64,
    target_page: i64,
    page_size: i64,
    options: &QueryOptions,
) -> Result<(Vec<String>, Vec<Vec<String>>, i64)> {
    if page_size <= 0 {
        anyhow::bail!("page_size must be greater than zero")
    }

    let conn = open_connection(db_path)?;

    let mut columns_stmt = conn
        .prepare(
            "SELECT name
             FROM column_name
             WHERE dataset_id = ?1
             ORDER BY col_idx ASC",
        )
        .context("failed to prepare columns query")?;
    let columns = columns_stmt
        .query_map([dataset_id], |row| row.get::<_, String>(0))
        .context("failed to query columns")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect columns")?;
    drop(columns_stmt);

    if columns.is_empty() {
        return Ok((columns, Vec::new(), 0));
    }

    if let Some(column_search_col) = options.column_search_col {
        if column_search_col < 0 || column_search_col as usize >= columns.len() {
            anyhow::bail!(
                "column_search_col out of range: {column_search_col} (columns: {})",
                columns.len()
            );
        }
    }

    if let Some(sort_col) = options.sort_col {
        if sort_col < 0 || sort_col as usize >= columns.len() {
            anyhow::bail!(
                "sort_col out of range: {sort_col} (columns: {})",
                columns.len()
            );
        }
    }

    let mut filter_clauses = vec!["base.dataset_id = ?".to_string()];
    let mut filter_params = vec![Value::Integer(dataset_id)];

    let global_search = options.global_search.trim();
    if !global_search.is_empty() {
        filter_clauses.push(
            "EXISTS (
                SELECT 1 FROM cell gs
                WHERE gs.dataset_id = ?
                  AND gs.row_idx = base.row_idx
                  AND gs.value LIKE ?
            )"
            .to_string(),
        );
        filter_params.push(Value::Integer(dataset_id));
        filter_params.push(Value::Text(format!("%{global_search}%")));
    }

    let column_search_text = options.column_search_text.trim();
    if !column_search_text.is_empty() {
        if let Some(column_search_col) = options.column_search_col {
            filter_clauses.push(
                "EXISTS (
                    SELECT 1 FROM cell cs
                    WHERE cs.dataset_id = ?
                      AND cs.row_idx = base.row_idx
                      AND cs.col_idx = ?
                      AND cs.value LIKE ?
                )"
                .to_string(),
            );
            filter_params.push(Value::Integer(dataset_id));
            filter_params.push(Value::Integer(column_search_col));
            filter_params.push(Value::Text(format!("%{column_search_text}%")));
        }
    }

    let where_sql = filter_clauses.join(" AND ");

    let count_sql = format!(
        "SELECT COUNT(*)
         FROM (
             SELECT base.row_idx
             FROM cell base
             WHERE {where_sql}
             GROUP BY base.row_idx
         ) filtered"
    );
    let total_rows: i64 = conn
        .query_row(
            &count_sql,
            rusqlite::params_from_iter(filter_params.iter().cloned()),
            |row| row.get(0),
        )
        .context("failed to query filtered row count")?;

    let offset = target_page.max(0) * page_size;
    let sort_direction = if options.sort_desc { "DESC" } else { "ASC" };

    let mut row_params = Vec::<Value>::new();
    let mut row_sql = String::from("SELECT base.row_idx FROM cell base ");
    if let Some(sort_col) = options.sort_col {
        row_sql.push_str(
            "LEFT JOIN cell sort_cell
             ON sort_cell.dataset_id = base.dataset_id
            AND sort_cell.row_idx = base.row_idx
            AND sort_cell.col_idx = ? ",
        );
        row_params.push(Value::Integer(sort_col));
    }

    row_sql.push_str(&format!(
        "WHERE {where_sql} GROUP BY base.row_idx ORDER BY "
    ));
    if options.sort_col.is_some() {
        row_sql.push_str(&format!("COALESCE(sort_cell.value, '') {sort_direction}, "));
    }
    row_sql.push_str("base.row_idx ASC LIMIT ? OFFSET ?");

    row_params.extend(filter_params.iter().cloned());
    row_params.push(Value::Integer(page_size));
    row_params.push(Value::Integer(offset));

    let mut row_stmt = conn
        .prepare(&row_sql)
        .context("failed to prepare page row_idx query")?;
    let row_indices = row_stmt
        .query_map(rusqlite::params_from_iter(row_params), |row| {
            row.get::<_, i64>(0)
        })
        .context("failed to query page row_idx")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect page row_idx")?;
    drop(row_stmt);

    if row_indices.is_empty() {
        return Ok((columns, Vec::new(), total_rows));
    }

    let placeholders = std::iter::repeat_n("?", row_indices.len())
        .collect::<Vec<_>>()
        .join(",");
    let hydrate_sql = format!(
        "SELECT row_idx, col_idx, value
         FROM cell
         WHERE dataset_id = ? AND row_idx IN ({placeholders})
         ORDER BY row_idx ASC, col_idx ASC"
    );
    let mut hydrate_params = vec![Value::Integer(dataset_id)];
    hydrate_params.extend(row_indices.iter().copied().map(Value::Integer));

    let mut rows = vec![vec![String::new(); columns.len()]; row_indices.len()];
    let row_pos: HashMap<i64, usize> = row_indices
        .iter()
        .copied()
        .enumerate()
        .map(|(idx, row_idx)| (row_idx, idx))
        .collect();

    let mut hydrate_stmt = conn
        .prepare(&hydrate_sql)
        .context("failed to prepare row hydration query")?;

    let mut hydrate_rows = hydrate_stmt
        .query(rusqlite::params_from_iter(hydrate_params))
        .context("failed to run row hydration query")?;

    while let Some(row) = hydrate_rows.next().context("failed to read hydrated row")? {
        let row_idx: i64 = row.get(0).context("failed to read row_idx")?;
        let col_idx: i64 = row.get(1).context("failed to read col_idx")?;
        let value: String = row.get(2).context("failed to read value")?;

        if let Some(&dest_row_idx) = row_pos.get(&row_idx) {
            if let Some(dest_cell) = rows
                .get_mut(dest_row_idx)
                .and_then(|dest_row| dest_row.get_mut(col_idx as usize))
            {
                *dest_cell = value;
            }
        }
    }

    Ok((columns, rows, total_rows))
}

#[allow(dead_code)]
pub fn reload_page_data(
    db_path: &Path,
    dataset_id: Option<i64>,
    target_page: i64,
    options: &QueryOptions,
) -> Result<ReloadPageResult> {
    let page = target_page.max(0);
    if let Some(dataset_id) = dataset_id {
        let (columns, rows, total_rows) =
            query_page(db_path, dataset_id, page, crate::PAGE_SIZE, options)?;
        Ok((columns, rows, total_rows, page))
    } else {
        Ok((Vec::new(), Vec::new(), 0, 0))
    }
}

#[allow(dead_code)]
pub fn list_datasets(db_path: &Path, include_deleted: bool) -> Result<Vec<DatasetMeta>> {
    init_db(db_path)?;
    let conn = open_connection(db_path)?;
    let filter = if include_deleted {
        ""
    } else {
        "WHERE deleted_at IS NULL"
    };
    let mut stmt = conn
        .prepare(&format!(
            "SELECT id, name, row_count, source_path, deleted_at
             FROM dataset
             {filter}
             ORDER BY id DESC"
        ))
        .context("failed to prepare datasets query")?;

    let datasets = stmt
        .query_map([], |row| {
            Ok(DatasetMeta {
                id: row.get::<_, i64>(0)?.into(),
                name: row.get(1)?,
                row_count: row.get(2)?,
                source_path: row.get(3)?,
                deleted_at: row.get(4)?,
            })
        })
        .context("failed to query datasets")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect datasets")?;

    Ok(datasets)
}

#[allow(dead_code)]
pub fn soft_delete_dataset(db_path: &Path, dataset_id: i64) -> Result<()> {
    init_db(db_path)?;
    let conn = open_connection(db_path)?;
    conn.execute(
        "UPDATE dataset SET deleted_at = datetime('now') WHERE id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to soft-delete dataset #{dataset_id}"))?;
    Ok(())
}

#[allow(dead_code)]
pub fn purge_dataset(db_path: &Path, dataset_id: i64) -> Result<()> {
    init_db(db_path)?;
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start purge transaction")?;
    tx.execute(
        "DELETE FROM cell WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to delete cells for dataset #{dataset_id}"))?;
    tx.execute(
        "DELETE FROM column_name WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to delete columns for dataset #{dataset_id}"))?;
    tx.execute("DELETE FROM dataset WHERE id = ?1", params![dataset_id])
        .with_context(|| format!("failed to delete dataset #{dataset_id}"))?;
    tx.commit().context("failed to commit purge transaction")?;
    Ok(())
}

pub fn build_updated_rows(
    columns: &[String],
    rows: &[Vec<String>],
    staged_cells: &HashMap<CellKey, String>,
    deleted_rows: &BTreeSet<usize>,
    added_rows: &[Vec<String>],
) -> Vec<Vec<String>> {
    let mut updated = Vec::new();
    for (row_idx, row) in rows.iter().enumerate() {
        if deleted_rows.contains(&row_idx) {
            continue;
        }
        let mut next_row = row.clone();
        for (col_idx, header) in columns.iter().enumerate() {
            if let Some(value) = staged_cells.get(&CellKey {
                row_idx,
                col_idx,
                column: header.clone(),
            }) {
                if col_idx < next_row.len() {
                    next_row[col_idx] = value.clone();
                }
            }
        }
        updated.push(next_row);
    }
    for row in added_rows {
        updated.push(row.clone());
    }
    updated
}

#[allow(dead_code)]
pub fn apply_changes_to_dataset(
    db_path: &Path,
    dataset_id: i64,
    columns: &[String],
    rows: &[Vec<String>],
    staged_cells: &HashMap<CellKey, String>,
    deleted_rows: &BTreeSet<usize>,
    added_rows: &[Vec<String>],
) -> Result<()> {
    let updated_rows = build_updated_rows(columns, rows, staged_cells, deleted_rows, added_rows);
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start update transaction")?;

    tx.execute(
        "DELETE FROM cell WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .context("failed to clear existing cells")?;

    let mut insert_cell = tx
        .prepare("INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)")
        .context("failed to prepare cell insert")?;
    for (row_idx, row) in updated_rows.iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            insert_cell
                .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                .context("failed to insert updated cell")?;
        }
    }
    drop(insert_cell);

    tx.execute(
        "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
        params![updated_rows.len() as i64, dataset_id],
    )
    .context("failed to update dataset row_count")?;

    tx.commit().context("failed to commit dataset update")?;
    Ok(())
}

#[allow(dead_code)]
pub fn create_dataset_from_rows(
    db_path: &Path,
    name: &str,
    source_path: &str,
    columns: &[String],
    rows: &[Vec<String>],
) -> Result<i64> {
    init_db(db_path)?;
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start dataset create transaction")?;

    tx.execute(
        "INSERT INTO dataset(name, source_path, row_count) VALUES (?1, ?2, 0)",
        params![name, source_path],
    )
    .context("failed to insert dataset")?;
    let dataset_id = tx.last_insert_rowid();

    insert_header_names(&tx, dataset_id, columns)?;

    let mut insert_cell = tx
        .prepare("INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)")
        .context("failed to prepare cell insert")?;
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            insert_cell
                .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                .context("failed to insert dataset cell")?;
        }
    }
    drop(insert_cell);

    tx.execute(
        "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
        params![rows.len() as i64, dataset_id],
    )
    .context("failed to update dataset row_count")?;

    tx.commit().context("failed to commit dataset create")?;
    Ok(dataset_id)
}

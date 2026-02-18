use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;

#[allow(dead_code)]
pub fn open_connection(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open db: {}", db_path.display()))?;
    conn.execute("PRAGMA foreign_keys = ON", [])
        .context("failed to enable foreign key enforcement")?;
    Ok(conn)
}

#[allow(dead_code)]
pub fn init_db(db_path: &Path) -> Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir: {}", parent.display()))?;
    }

    let conn = open_connection(db_path)?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS dataset (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            source_path TEXT NOT NULL,
            row_count   INTEGER NOT NULL,
            deleted_at  TEXT,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS column_name (
            dataset_id  INTEGER NOT NULL,
            col_idx     INTEGER NOT NULL,
            name        TEXT NOT NULL,
            PRIMARY KEY (dataset_id, col_idx),
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE TABLE IF NOT EXISTS cell (
            dataset_id  INTEGER NOT NULL,
            row_idx     INTEGER NOT NULL,
            col_idx     INTEGER NOT NULL,
            value       TEXT NOT NULL,
            PRIMARY KEY (dataset_id, row_idx, col_idx),
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE TABLE IF NOT EXISTS column_visibility (
            dataset_id  INTEGER NOT NULL,
            col_idx     INTEGER NOT NULL,
            visible     INTEGER NOT NULL,
            PRIMARY KEY (dataset_id, col_idx),
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE TABLE IF NOT EXISTS dataset_flag (
            dataset_id   INTEGER PRIMARY KEY,
            is_holdings  INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE INDEX IF NOT EXISTS idx_cell_dataset_row
            ON cell(dataset_id, row_idx);

        CREATE INDEX IF NOT EXISTS idx_cell_dataset_col_value
            ON cell(dataset_id, col_idx, value);
        ",
    )
    .context("failed to initialize schema")?;

    conn.execute("ALTER TABLE dataset ADD COLUMN deleted_at TEXT", [])
        .ok();

    Ok(())
}

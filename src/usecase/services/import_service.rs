use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::infra::import::csv::import_csv_to_sqlite;
use crate::infra::import::xlsx::import_xlsx_selected_sheets_to_sqlite;
use crate::ImportResult;

#[allow(dead_code)]
pub struct ImportService {
    db_path: PathBuf,
}

impl ImportService {
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
    }

    pub fn import_csv(&self, path: &Path) -> Result<ImportResult> {
        import_csv_to_sqlite(&self.db_path, path)
    }

    pub fn import_xlsx(&self, path: &Path) -> Result<Vec<ImportResult>> {
        import_xlsx_selected_sheets_to_sqlite(&self.db_path, path)
    }
}

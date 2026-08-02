use std::fs;
use std::path::{Path, PathBuf};

use chrono::Local;
use rusqlite::Connection;

use crate::error::AppResult;

pub fn backup_for_today(database_path: &Path) -> AppResult<PathBuf> {
    let today = Local::now().format("%Y-%m-%d");
    create_backup(database_path, &format!("data-{today}"))
}

fn create_backup(database_path: &Path, stem: &str) -> AppResult<PathBuf> {
    let backup_dir = database_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    fs::create_dir_all(&backup_dir)?;

    let backup_path = backup_dir.join(format!("{stem}.sqlite"));
    if backup_path.exists() {
        return Ok(backup_path);
    }

    let connection = Connection::open(database_path)?;
    connection.execute(
        "VACUUM main INTO ?1",
        [backup_path.to_string_lossy().as_ref()],
    )?;

    Ok(backup_path)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::backup_for_today;

    #[test]
    fn reuses_same_daily_backup_path() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        let connection = Connection::open(&database_path).expect("open db");
        connection
            .execute_batch("CREATE TABLE marker (id INTEGER PRIMARY KEY);")
            .expect("create table");
        drop(connection);

        let first_backup = backup_for_today(&database_path).expect("first daily backup");
        let second_backup = backup_for_today(&database_path).expect("second daily backup");

        assert_eq!(first_backup, second_backup);
        assert!(first_backup.exists());
    }
}

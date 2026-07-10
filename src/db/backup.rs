use std::fs;
use std::path::{Path, PathBuf};

use chrono::{Local, Timelike};
use rusqlite::Connection;

use crate::error::AppResult;

pub fn backup_before_migration(database_path: &Path, current_version: i64) -> AppResult<PathBuf> {
    let now = Local::now();
    create_backup(
        database_path,
        &format!(
            "pre_migration_v{current_version}_{}_{:09}",
            now.format("%Y-%m-%dT%H-%M-%S"),
            now.nanosecond()
        ),
        false,
    )
}

#[allow(dead_code)]
pub fn backup_for_today(database_path: &Path) -> AppResult<PathBuf> {
    let today = Local::now().format("%Y-%m-%d");
    create_backup(database_path, &format!("data-{today}"), true)
}

fn create_backup(database_path: &Path, stem: &str, reuse_existing: bool) -> AppResult<PathBuf> {
    let backup_dir = database_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("backups");
    fs::create_dir_all(&backup_dir)?;

    let backup_path = available_backup_path(&backup_dir, stem, reuse_existing);
    if reuse_existing && backup_path.exists() {
        return Ok(backup_path);
    }

    let connection = Connection::open(database_path)?;
    connection.execute(
        "VACUUM main INTO ?1",
        [backup_path.to_string_lossy().as_ref()],
    )?;

    Ok(backup_path)
}

fn available_backup_path(backup_dir: &Path, stem: &str, reuse_existing: bool) -> PathBuf {
    let first_path = backup_dir.join(format!("{stem}.sqlite"));
    if reuse_existing || !first_path.exists() {
        return first_path;
    }

    for index in 1.. {
        let candidate = backup_dir.join(format!("{stem}-{index}.sqlite"));
        if !candidate.exists() {
            return candidate;
        }
    }

    unreachable!("unbounded backup filename search should always return")
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{backup_before_migration, backup_for_today};

    #[test]
    fn creates_pre_migration_backup_without_reusing_stale_file() {
        let temp_dir = tempdir().expect("temp dir");
        let database_path = temp_dir.path().join("data.sqlite");
        let connection = Connection::open(&database_path).expect("open db");
        connection
            .execute_batch("CREATE TABLE marker (id INTEGER PRIMARY KEY);")
            .expect("create table");
        drop(connection);

        let first_backup = backup_before_migration(&database_path, 0).expect("backup succeeds");
        let second_backup =
            backup_before_migration(&database_path, 0).expect("second backup succeeds");

        assert_ne!(first_backup, second_backup);
        assert!(first_backup.exists());
        assert!(second_backup.exists());
    }

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

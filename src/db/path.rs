use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;

use directories::ProjectDirs;
use rusqlite::{Connection, OpenFlags};

use crate::error::{AppError, AppResult};

const APP_QUALIFIER: &str = "local";
const APP_ORGANIZATION: &str = "HellHBBD";
const APP_NAME: &str = "asset-manager";
const DATABASE_FILE_NAME: &str = "data.sqlite";
const SEED_DATABASE_PATH: &str = "assets/data.sqlite";

pub fn runtime_database_path() -> AppResult<PathBuf> {
    let project_dirs = ProjectDirs::from(APP_QUALIFIER, APP_ORGANIZATION, APP_NAME)
        .ok_or_else(|| AppError::Validation("無法取得應用程式資料目錄".to_string()))?;
    Ok(project_dirs.data_dir().join(DATABASE_FILE_NAME))
}

pub fn seed_database_path() -> PathBuf {
    seed_database_candidates()
        .into_iter()
        .find(|path| path.exists())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SEED_DATABASE_PATH))
}

pub fn ensure_runtime_database() -> AppResult<PathBuf> {
    let runtime_path = runtime_database_path()?;
    if runtime_path.exists() {
        return Ok(runtime_path);
    }

    if let Some(parent) = runtime_path.parent() {
        fs::create_dir_all(parent)?;
    }

    copy_seed_database_atomically(&runtime_path)?;
    Ok(runtime_path)
}

fn copy_seed_database_atomically(runtime_path: &PathBuf) -> AppResult<()> {
    let temp_path = runtime_path.with_extension(format!("sqlite.tmp.{}", std::process::id()));
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    fs::copy(seed_database_path(), &temp_path)?;
    let seed_connection =
        Connection::open_with_flags(&temp_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    seed_connection.query_row("PRAGMA integrity_check", [], |row| {
        let result: String = row.get(0)?;
        if result == "ok" {
            Ok(())
        } else {
            Err(rusqlite::Error::InvalidQuery)
        }
    })?;
    drop(seed_connection);

    match fs::hard_link(&temp_path, runtime_path) {
        Ok(()) => {
            fs::remove_file(temp_path)?;
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            fs::remove_file(temp_path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn seed_database_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            candidates.push(exe_dir.join(SEED_DATABASE_PATH));
            candidates.push(exe_dir.join(DATABASE_FILE_NAME));
        }
    }

    candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SEED_DATABASE_PATH));
    candidates
}

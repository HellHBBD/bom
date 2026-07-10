#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum AppError {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("資料庫錯誤：{0}")]
    Database(#[from] rusqlite::Error),

    #[error("數字格式錯誤：{field}")]
    InvalidDecimal { field: &'static str },

    #[error("找不到資料：{0}")]
    NotFound(String),

    #[error("資料重複：{0}")]
    Duplicate(String),

    #[error("輸入資料無效：{0}")]
    Validation(String),

    #[error("檔案操作失敗：{0}")]
    Io(#[from] std::io::Error),
}

#[allow(dead_code)]
pub type AppResult<T> = Result<T, AppError>;

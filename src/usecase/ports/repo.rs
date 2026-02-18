use std::collections::BTreeMap;

use crate::domain::entities::dataset::{DatasetId, PageQuery, PageResult};
use crate::domain::entities::edit::StagedEdits;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoError {
    Message(String),
}

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoError::Message(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for RepoError {}

pub trait DatasetRepository: Send + Sync {
    fn init(&self) -> Result<(), RepoError>;

    fn list_datasets(&self, include_deleted: bool) -> Result<Vec<DatasetMeta>, RepoError>;
    fn query_page(&self, query: PageQuery) -> Result<PageResult, RepoError>;

    fn create_dataset(
        &self,
        meta: NewDatasetMeta,
        data: TabularData,
    ) -> Result<DatasetId, RepoError>;
    fn apply_edits(&self, id: DatasetId, edits: StagedEdits) -> Result<(), RepoError>;
    fn soft_delete_dataset(&self, id: DatasetId) -> Result<(), RepoError>;
    fn purge_dataset(&self, id: DatasetId) -> Result<(), RepoError>;
    fn load_column_visibility(&self, id: DatasetId) -> Result<BTreeMap<i64, bool>, RepoError>;
    fn upsert_column_visibility(
        &self,
        id: DatasetId,
        visibility: BTreeMap<i64, bool>,
    ) -> Result<(), RepoError>;
    fn load_holdings_flags(&self) -> Result<BTreeMap<i64, bool>, RepoError>;
    fn upsert_holdings_flag(&self, id: DatasetId, is_holdings: bool) -> Result<(), RepoError>;
    fn rename_dataset(&self, id: DatasetId, name: String) -> Result<(), RepoError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetMeta {
    pub id: DatasetId,
    pub name: String,
    pub row_count: i64,
    pub source_path: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDatasetMeta {
    pub name: String,
    pub source_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabularData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

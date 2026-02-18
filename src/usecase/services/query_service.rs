use std::sync::Arc;

use std::collections::BTreeMap;

use crate::domain::entities::dataset::{DatasetId, PageQuery, PageResult};
use crate::usecase::ports::repo::{DatasetMeta, DatasetRepository, RepoError};

#[allow(dead_code)]
pub struct QueryService {
    repo: Arc<dyn DatasetRepository>,
}

impl QueryService {
    pub fn new(repo: Arc<dyn DatasetRepository>) -> Self {
        Self { repo }
    }

    pub fn list_datasets(&self, include_deleted: bool) -> Result<Vec<DatasetMeta>, RepoError> {
        self.repo.list_datasets(include_deleted)
    }

    pub fn query_page(&self, query: PageQuery) -> Result<PageResult, RepoError> {
        self.repo.query_page(query)
    }

    pub fn load_column_visibility(
        &self,
        dataset_id: DatasetId,
    ) -> Result<BTreeMap<i64, bool>, RepoError> {
        self.repo.load_column_visibility(dataset_id)
    }

    pub fn upsert_column_visibility(
        &self,
        dataset_id: DatasetId,
        visibility: BTreeMap<i64, bool>,
    ) -> Result<(), RepoError> {
        self.repo.upsert_column_visibility(dataset_id, visibility)
    }

    pub fn load_holdings_flags(&self) -> Result<BTreeMap<i64, bool>, RepoError> {
        self.repo.load_holdings_flags()
    }

    pub fn upsert_holdings_flag(
        &self,
        dataset_id: DatasetId,
        is_holdings: bool,
    ) -> Result<(), RepoError> {
        self.repo.upsert_holdings_flag(dataset_id, is_holdings)
    }

    pub fn rename_dataset(&self, dataset_id: DatasetId, name: String) -> Result<(), RepoError> {
        self.repo.rename_dataset(dataset_id, name)
    }
}

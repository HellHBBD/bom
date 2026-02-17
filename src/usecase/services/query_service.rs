use std::sync::Arc;

use crate::domain::entities::dataset::{PageQuery, PageResult};
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
}

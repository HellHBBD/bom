use std::sync::Arc;

use crate::domain::entities::dataset::DatasetId;
use crate::domain::entities::edit::StagedEdits;
use crate::usecase::ports::repo::{DatasetRepository, RepoError};
use crate::usecase::ports::repo::{NewDatasetMeta, TabularData};

#[allow(dead_code)]
pub struct EditService {
    repo: Arc<dyn DatasetRepository>,
}

impl EditService {
    pub fn new(repo: Arc<dyn DatasetRepository>) -> Self {
        Self { repo }
    }

    pub fn apply_edits(&self, dataset_id: DatasetId, edits: StagedEdits) -> Result<(), RepoError> {
        self.repo.apply_edits(dataset_id, edits)
    }

    pub fn create_dataset(
        &self,
        meta: NewDatasetMeta,
        data: TabularData,
    ) -> Result<DatasetId, RepoError> {
        self.repo.create_dataset(meta, data)
    }

    pub fn soft_delete_dataset(&self, dataset_id: DatasetId) -> Result<(), RepoError> {
        self.repo.soft_delete_dataset(dataset_id)
    }

    pub fn purge_dataset(&self, dataset_id: DatasetId) -> Result<(), RepoError> {
        self.repo.purge_dataset(dataset_id)
    }
}

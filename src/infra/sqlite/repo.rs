use std::path::PathBuf;

use crate::domain::entities::dataset::{DatasetId, PageQuery, PageResult, SortDirection};
use crate::domain::entities::edit::StagedEdits;
use crate::infra::sqlite::queries::{
    apply_changes_to_dataset, create_dataset_from_rows, list_datasets, load_column_visibility,
    load_holdings_flags, purge_dataset, query_page, rename_dataset, soft_delete_dataset,
    upsert_column_visibility, upsert_holdings_flag,
};
use crate::infra::sqlite::schema::init_db;
use crate::usecase::ports::repo::{
    DatasetMeta, DatasetRepository, NewDatasetMeta, RepoError, TabularData,
};
use crate::QueryOptions;
use std::collections::BTreeMap;

#[allow(dead_code)]
pub struct SqliteRepo {
    pub db_path: PathBuf,
}

impl DatasetRepository for SqliteRepo {
    fn init(&self) -> Result<(), RepoError> {
        init_db(&self.db_path).map_err(|err| RepoError::Message(err.to_string()))
    }

    fn list_datasets(&self, include_deleted: bool) -> Result<Vec<DatasetMeta>, RepoError> {
        list_datasets(&self.db_path, include_deleted)
            .map_err(|err| RepoError::Message(err.to_string()))
    }

    fn query_page(&self, query: PageQuery) -> Result<PageResult, RepoError> {
        let (column_search_col, column_search_text) = match query.column_filter {
            Some(filter) => (Some(filter.column_idx), filter.term),
            None => (None, String::new()),
        };
        let (sort_col, sort_desc) = match query.sort {
            Some(sort) => (
                Some(sort.column_idx),
                matches!(sort.direction, SortDirection::Desc),
            ),
            None => (None, false),
        };
        let options = QueryOptions {
            global_search: query.global_search,
            column_search_col,
            column_search_text,
            sort_col,
            sort_desc,
        };

        let (columns, rows, total_rows) = query_page(
            &self.db_path,
            query.dataset_id.0,
            query.page,
            query.page_size,
            &options,
        )
        .map_err(|err| RepoError::Message(err.to_string()))?;

        Ok(PageResult {
            columns,
            rows,
            total_rows,
        })
    }

    fn create_dataset(
        &self,
        meta: NewDatasetMeta,
        data: TabularData,
    ) -> Result<DatasetId, RepoError> {
        let dataset_id = create_dataset_from_rows(
            &self.db_path,
            &meta.name,
            &meta.source_path,
            &data.columns,
            &data.rows,
        )
        .map_err(|err| RepoError::Message(err.to_string()))?;

        Ok(DatasetId(dataset_id))
    }

    fn apply_edits(&self, id: DatasetId, edits: StagedEdits) -> Result<(), RepoError> {
        let (columns, rows, _total) =
            query_page(&self.db_path, id.0, 0, i64::MAX, &QueryOptions::default())
                .map_err(|err| RepoError::Message(err.to_string()))?;

        apply_changes_to_dataset(
            &self.db_path,
            id.0,
            &columns,
            &rows,
            &edits.staged_cells,
            &edits.deleted_rows,
            &edits.added_rows,
        )
        .map_err(|err| RepoError::Message(err.to_string()))
    }

    fn soft_delete_dataset(&self, id: DatasetId) -> Result<(), RepoError> {
        soft_delete_dataset(&self.db_path, id.0).map_err(|err| RepoError::Message(err.to_string()))
    }

    fn purge_dataset(&self, id: DatasetId) -> Result<(), RepoError> {
        purge_dataset(&self.db_path, id.0).map_err(|err| RepoError::Message(err.to_string()))
    }

    fn load_column_visibility(&self, id: DatasetId) -> Result<BTreeMap<i64, bool>, RepoError> {
        load_column_visibility(&self.db_path, id.0)
            .map_err(|err| RepoError::Message(err.to_string()))
    }

    fn upsert_column_visibility(
        &self,
        id: DatasetId,
        visibility: BTreeMap<i64, bool>,
    ) -> Result<(), RepoError> {
        upsert_column_visibility(&self.db_path, id.0, &visibility)
            .map_err(|err| RepoError::Message(err.to_string()))
    }

    fn load_holdings_flags(&self) -> Result<BTreeMap<i64, bool>, RepoError> {
        load_holdings_flags(&self.db_path).map_err(|err| RepoError::Message(err.to_string()))
    }

    fn upsert_holdings_flag(&self, id: DatasetId, is_holdings: bool) -> Result<(), RepoError> {
        upsert_holdings_flag(&self.db_path, id.0, is_holdings)
            .map_err(|err| RepoError::Message(err.to_string()))
    }

    fn rename_dataset(&self, id: DatasetId, name: String) -> Result<(), RepoError> {
        rename_dataset(&self.db_path, id.0, &name)
            .map_err(|err| RepoError::Message(err.to_string()))
    }
}

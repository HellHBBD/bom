#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatasetId(pub i64);

impl From<i64> for DatasetId {
    fn from(value: i64) -> Self {
        DatasetId(value)
    }
}

impl From<DatasetId> for i64 {
    fn from(value: DatasetId) -> Self {
        value.0
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    pub column_idx: i64,
    pub direction: SortDirection,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFilter {
    pub column_idx: i64,
    pub term: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageQuery {
    pub dataset_id: DatasetId,
    pub page: i64,
    pub page_size: i64,
    pub global_search: String,
    pub column_filter: Option<ColumnFilter>,
    pub sort: Option<SortSpec>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total_rows: i64,
}

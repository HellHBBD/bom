use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CellKey {
    pub row_idx: usize,
    pub col_idx: usize,
    pub column: String,
}

#[derive(Debug, Clone, Default)]
pub struct StagedEdits {
    pub staged_cells: HashMap<CellKey, String>,
    pub deleted_rows: BTreeSet<usize>,
    pub added_rows: Vec<Vec<String>>,
}

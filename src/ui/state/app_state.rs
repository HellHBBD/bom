use std::collections::{BTreeMap, BTreeSet, HashMap};

use dioxus::prelude::{use_signal, Signal};

use crate::domain::entities::edit::CellKey;
use crate::usecase::ports::repo::DatasetMeta;
use crate::{default_dataset_name_mmdd, PendingAction};

pub struct AppState {
    pub datasets: Signal<Vec<DatasetMeta>>,
    pub selected_group_key: Signal<Option<String>>,
    pub selected_dataset_id: Signal<Option<i64>>,
    pub columns: Signal<Vec<String>>,
    pub column_visibility: Signal<BTreeMap<i64, bool>>,
    pub rows: Signal<Vec<Vec<String>>>,
    pub holdings_flags: Signal<BTreeMap<i64, bool>>,
    pub page: Signal<i64>,
    pub total_rows: Signal<i64>,
    pub global_search: Signal<String>,
    pub column_search_col: Signal<Option<i64>>,
    pub column_search_text: Signal<String>,
    pub sort_col: Signal<Option<i64>>,
    pub sort_desc: Signal<bool>,
    pub show_deleted: Signal<bool>,
    pub busy: Signal<bool>,
    pub status: Signal<String>,
    pub staged_cells: Signal<HashMap<CellKey, String>>,
    pub deleted_rows: Signal<BTreeSet<usize>>,
    pub selected_rows: Signal<BTreeSet<usize>>,
    pub editing_cell: Signal<Option<CellKey>>,
    pub editing_value: Signal<String>,
    pub added_rows: Signal<Vec<Vec<String>>>,
    pub show_add_row: Signal<bool>,
    pub new_row_inputs: Signal<HashMap<String, String>>,
    pub context_menu: Signal<Option<(f64, f64)>>,
    pub context_row: Signal<Option<usize>>,
    pub pending_action: Signal<Option<PendingAction>>,
    pub show_save_prompt: Signal<bool>,
    pub show_save_as_prompt: Signal<bool>,
    pub save_as_name: Signal<String>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            datasets: use_signal(Vec::<DatasetMeta>::new),
            selected_group_key: use_signal(|| None::<String>),
            selected_dataset_id: use_signal(|| None::<i64>),
            columns: use_signal(Vec::<String>::new),
            column_visibility: use_signal(BTreeMap::<i64, bool>::new),
            rows: use_signal(Vec::<Vec<String>>::new),
            holdings_flags: use_signal(BTreeMap::<i64, bool>::new),
            page: use_signal(|| 0_i64),
            total_rows: use_signal(|| 0_i64),
            global_search: use_signal(String::new),
            column_search_col: use_signal(|| None::<i64>),
            column_search_text: use_signal(String::new),
            sort_col: use_signal(|| None::<i64>),
            sort_desc: use_signal(|| false),
            show_deleted: use_signal(|| false),
            busy: use_signal(|| false),
            status: use_signal(|| "就緒".to_string()),
            staged_cells: use_signal(HashMap::<CellKey, String>::new),
            deleted_rows: use_signal(BTreeSet::<usize>::new),
            selected_rows: use_signal(BTreeSet::<usize>::new),
            editing_cell: use_signal(|| None::<CellKey>),
            editing_value: use_signal(String::new),
            added_rows: use_signal(Vec::<Vec<String>>::new),
            show_add_row: use_signal(|| false),
            new_row_inputs: use_signal(HashMap::<String, String>::new),
            context_menu: use_signal(|| None::<(f64, f64)>),
            context_row: use_signal(|| None::<usize>),
            pending_action: use_signal(|| None::<PendingAction>),
            show_save_prompt: use_signal(|| false),
            show_save_as_prompt: use_signal(|| false),
            save_as_name: use_signal(default_dataset_name_mmdd),
        }
    }
}

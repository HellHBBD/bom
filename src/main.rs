mod app;
mod domain;
mod infra;
mod platform;
mod ui;
mod usecase;

use dioxus::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use directories::ProjectDirs;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::domain::entities::dataset::{
    ColumnFilter, DatasetId, PageQuery, SortDirection, SortSpec,
};
use crate::domain::entities::edit::{CellKey, StagedEdits};
use crate::infra::sqlite::repo::SqliteRepo;
use crate::usecase::ports::repo::{DatasetMeta, DatasetRepository, NewDatasetMeta, TabularData};
use crate::usecase::services::edit_service::EditService;
use crate::usecase::services::import_service::ImportService;
use crate::usecase::services::query_service::QueryService;

pub const PAGE_SIZE: i64 = i64::MAX;
const NONE_OPTION_VALUE: &str = "__none__";

type ReloadPageResult = (Vec<String>, Vec<Vec<String>>, i64, i64);

fn build_page_query(dataset_id: i64, page: i64, options: &QueryOptions) -> PageQuery {
    let column_filter = options.column_search_col.map(|col| ColumnFilter {
        column_idx: col,
        term: options.column_search_text.clone(),
    });
    let sort = options.sort_col.map(|col| SortSpec {
        column_idx: col,
        direction: if options.sort_desc {
            SortDirection::Desc
        } else {
            SortDirection::Asc
        },
    });
    PageQuery {
        dataset_id: dataset_id.into(),
        page,
        page_size: PAGE_SIZE,
        global_search: options.global_search.clone(),
        column_filter,
        sort,
    }
}

fn reload_page_data_usecase(
    service: &QueryService,
    dataset_id: Option<i64>,
    target_page: i64,
    options: &QueryOptions,
) -> Result<ReloadPageResult> {
    let page = target_page.max(0);
    if let Some(dataset_id) = dataset_id {
        let query = build_page_query(dataset_id, page, options);
        let result = service
            .query_page(query)
            .map_err(|err| anyhow!(err.to_string()))?;
        Ok((result.columns, result.rows, result.total_rows, page))
    } else {
        Ok((Vec::new(), Vec::new(), 0, 0))
    }
}

fn main() {
    let webview_data_dir =
        default_webview_data_dir().expect("should resolve and create WebView2 data directory");

    dioxus::LaunchBuilder::desktop()
        .with_cfg(
            dioxus::desktop::Config::new()
                .with_window(dioxus::desktop::WindowBuilder::new().with_title("BOM"))
                .with_data_directory(webview_data_dir),
        )
        .launch(app::App);
}

#[allow(dead_code)]
#[component]
fn App() -> Element {
    let db_path = match default_db_path() {
        Ok(path) => path,
        Err(err) => {
            return rsx! {
                div {
                    p { "無法取得資料庫路徑：{err}" }
                }
            };
        }
    };

    let mut datasets = use_signal(Vec::<DatasetMeta>::new);
    let mut selected_group_key = use_signal(|| None::<String>);
    let mut selected_dataset_id = use_signal(|| None::<i64>);
    let mut columns = use_signal(Vec::<String>::new);
    let mut rows = use_signal(Vec::<Vec<String>>::new);
    let mut page = use_signal(|| 0_i64);
    let mut total_rows = use_signal(|| 0_i64);
    let mut global_search = use_signal(String::new);
    let mut column_search_col = use_signal(|| None::<i64>);
    let mut column_search_text = use_signal(String::new);
    let mut sort_col = use_signal(|| None::<i64>);
    let mut sort_desc = use_signal(|| false);
    let mut show_deleted = use_signal(|| false);
    let mut busy = use_signal(|| false);
    let mut status = use_signal(|| "就緒".to_string());
    let mut staged_cells = use_signal(HashMap::<CellKey, String>::new);
    let mut deleted_rows = use_signal(BTreeSet::<usize>::new);
    let mut selected_rows = use_signal(BTreeSet::<usize>::new);
    let mut editing_cell = use_signal(|| None::<CellKey>);
    let mut editing_value = use_signal(String::new);
    let mut added_rows = use_signal(Vec::<Vec<String>>::new);
    let mut show_add_row = use_signal(|| false);
    let mut new_row_inputs = use_signal(HashMap::<String, String>::new);
    let mut context_menu = use_signal(|| None::<(f64, f64)>);
    let mut context_row = use_signal(|| None::<usize>);
    let mut pending_action = use_signal(|| None::<PendingAction>);
    let mut show_save_prompt = use_signal(|| false);
    let mut show_save_as_prompt = use_signal(|| false);
    let mut save_as_name = use_signal(default_dataset_name_mmdd);

    let db_path = Arc::new(db_path);
    let repo = Arc::new(SqliteRepo {
        db_path: (*db_path).clone(),
    });
    let query_service = Arc::new(QueryService::new(repo.clone()));
    let edit_service = Arc::new(EditService::new(repo.clone()));
    let import_service = Arc::new(ImportService::new((*db_path).clone()));
    let repo_for_init = repo.clone();
    let query_service_for_init = query_service.clone();
    use_effect(move || {
        *busy.write() = true;
        let init_result = repo_for_init
            .init()
            .map_err(|err| anyhow!(err.to_string()))
            .and_then(|_| {
                query_service_for_init
                    .list_datasets(false)
                    .map_err(|err| anyhow!(err.to_string()))
            });
        match init_result {
            Ok(available) => {
                let groups = build_dataset_groups(&available);
                let first_dataset = groups
                    .first()
                    .and_then(|g| g.datasets.first())
                    .map(|dataset| dataset.id.0);
                *datasets.write() = available;
                *selected_group_key.write() = groups.first().map(|g| g.key.clone());
                *selected_dataset_id.write() = first_dataset;
                *page.write() = 0;

                match reload_page_data_usecase(
                    &query_service_for_init,
                    first_dataset,
                    0,
                    &QueryOptions::default(),
                ) {
                    Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                        *columns.write() = loaded_columns;
                        *rows.write() = loaded_rows;
                        *total_rows.write() = loaded_total;
                        *page.write() = loaded_page;
                        *status.write() = "已載入資料集".to_string();
                    }
                    Err(err) => {
                        *columns.write() = Vec::new();
                        *rows.write() = Vec::new();
                        *total_rows.write() = 0;
                        *page.write() = 0;
                        *status.write() = format!("載入資料失敗：{err}");
                    }
                }
            }
            Err(err) => {
                *datasets.write() = Vec::new();
                *selected_group_key.write() = None;
                *selected_dataset_id.write() = None;
                *columns.write() = Vec::new();
                *rows.write() = Vec::new();
                *total_rows.write() = 0;
                *page.write() = 0;
                *status.write() = format!("初始化資料庫失敗：{err}");
            }
        }
        *busy.write() = false;
    });

    let current_total_rows = total_rows();

    let query_service_for_import = query_service.clone();
    let import_service_for_import = import_service.clone();
    let query_service_for_dataset_change = query_service.clone();
    let query_service_for_global_search = query_service.clone();
    let query_service_for_column_select = query_service.clone();
    let query_service_for_column_search = query_service.clone();
    let query_service_for_sort_select = query_service.clone();
    let query_service_for_sort_toggle = query_service.clone();
    let query_service_for_tab_switch = query_service.clone();
    let query_service_for_show_deleted = query_service.clone();
    let query_service_for_soft_delete = query_service.clone();
    let query_service_for_purge = query_service.clone();
    let query_service_for_save = query_service.clone();
    let query_service_for_save_as = query_service.clone();
    let query_service_for_import_overwrite = query_service.clone();
    let query_service_for_import_save_as = query_service.clone();
    let edit_service_for_save = edit_service.clone();
    let edit_service_for_save_as = edit_service.clone();
    let edit_service_for_soft_delete = edit_service.clone();
    let edit_service_for_purge = edit_service.clone();
    let import_service_for_import_overwrite = import_service.clone();
    let import_service_for_import_save_as = import_service.clone();
    let grouped_datasets = build_dataset_groups(&datasets());
    let active_group =
        selected_group_key().and_then(|k| grouped_datasets.iter().find(|g| g.key == k).cloned());
    let current_columns = columns();
    let current_rows = rows();
    let added_rows_snapshot = added_rows();
    let datasets_snapshot = datasets();
    let staged_cells_snapshot = staged_cells();
    let deleted_rows_snapshot = deleted_rows();
    let selected_rows_snapshot = selected_rows();
    let editing_cell_snapshot = editing_cell();
    let column_alignments: Vec<&'static str> = current_columns
        .iter()
        .enumerate()
        .map(|(idx, header)| column_alignment(header, &current_rows, idx))
        .collect();
    let is_holdings = is_holdings_table(&current_columns);
    let editable_columns = editable_columns_for_holdings();
    let required_columns = required_columns_for_holdings();
    let base_row_count = current_rows.len();
    let total_row_count = base_row_count + added_rows_snapshot.len();
    let has_pending_changes = !staged_cells_snapshot.is_empty()
        || !deleted_rows_snapshot.is_empty()
        || !added_rows_snapshot.is_empty();
    let current_columns_for_add = current_columns.clone();
    let current_columns_for_save = current_columns.clone();
    let current_rows_for_save = current_rows.clone();
    let datasets_for_save = datasets_snapshot.clone();
    let current_columns_for_save_as = current_columns_for_save.clone();
    let current_rows_for_save_as = current_rows_for_save.clone();
    let get_raw_value = |row_idx: usize, col_idx: usize| -> String {
        if let Some(header) = current_columns.get(col_idx) {
            if let Some(value) = staged_cells_snapshot.get(&CellKey {
                row_idx,
                col_idx,
                column: header.clone(),
            }) {
                return value.clone();
            }
        }
        if row_idx < base_row_count {
            current_rows
                .get(row_idx)
                .and_then(|row| row.get(col_idx))
                .cloned()
                .unwrap_or_default()
        } else {
            let new_idx = row_idx - base_row_count;
            added_rows_snapshot
                .get(new_idx)
                .and_then(|row| row.get(col_idx))
                .cloned()
                .unwrap_or_default()
        }
    };
    let mut row_render_models = Vec::with_capacity(total_row_count);
    for row_idx in 0..total_row_count {
        let deleted = deleted_rows_snapshot.contains(&row_idx);
        let selected = selected_rows_snapshot.contains(&row_idx);
        let added = row_idx >= base_row_count;
        let style = format!(
            "{}{}",
            if selected {
                "background: #eef4ff;"
            } else if added {
                "background: #d9f7d9;"
            } else {
                ""
            },
            if deleted {
                "border-top: 2px solid #d33; border-bottom: 2px solid #d33;"
            } else {
                ""
            }
        );
        let mut cells = Vec::with_capacity(current_columns.len());
        for (col_idx, header) in current_columns.iter().enumerate() {
            let raw_value = get_raw_value(row_idx, col_idx);
            let formatted = format_cell_value(header, &raw_value);
            let is_editing = editing_cell_snapshot
                .as_ref()
                .map(|cell| cell.row_idx == row_idx && cell.column == *header)
                .unwrap_or(false);
            let is_modified = staged_cells_snapshot.contains_key(&CellKey {
                row_idx,
                col_idx,
                column: header.clone(),
            });
            let is_editable = editable_columns.iter().any(|c| c == header);
            let cell_style = format!(
                "border: 1px solid #bbb; padding: 6px; text-align: {};{}",
                column_alignments.get(col_idx).copied().unwrap_or("left"),
                if is_modified {
                    " background: #d9f7d9;"
                } else {
                    ""
                }
            );
            cells.push(CellRender {
                row_idx,
                col_idx,
                header: header.clone(),
                raw: raw_value,
                formatted,
                is_editing,
                is_editable,
                style: cell_style,
            });
        }
        row_render_models.push(RowRender {
            row_idx,
            is_deleted: deleted,
            style,
            cells,
        });
    }

    rsx! {
        div {
            onclick: move |_| {
                context_menu.set(None);
                context_row.set(None);
            },
            nav {
                style: "display: flex; gap: 12px; align-items: center; flex-wrap: wrap; padding: 8px 0;",
                button {
                    disabled: busy(),
                    onclick: move |_| {
                        if busy() {
                            return;
                        }

                        let Some(file_path) = FileDialog::new()
                            .add_filter("資料檔", &["csv", "xlsx"])
                            .pick_file() else {
                            *status.write() = "已取消匯入".to_string();
                            return;
                        };

                        if is_holdings && has_pending_changes {
                            pending_action.set(Some(PendingAction::Import(file_path.clone())));
                            show_save_prompt.set(true);
                            return;
                        }

                        *busy.write() = true;
                        *status.write() = format!("正在匯入 {}", file_path.display());

                        let ext = file_path
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(|s| s.to_ascii_lowercase())
                            .unwrap_or_default();

                        let import_result = if ext == "xlsx" {
                            import_service_for_import
                                .import_xlsx(&file_path)
                                .map(|items| (items.first().map(|it| it.dataset_id), items.len() as i64, true))
                        } else {
                            import_service_for_import
                                .import_csv(&file_path)
                                .map(|item| (Some(item.dataset_id), item.row_count, false))
                        };

                        match import_result {
                            Ok((selected_id, imported_count, is_xlsx)) => match query_service_for_import.list_datasets(show_deleted()) {
                                Ok(available) => {
                                    let groups = build_dataset_groups(&available);
                                    *datasets.write() = available;
                                    let next_group_key = selected_id.and_then(|id| {
                                        groups
                                            .iter()
                                            .find(|g| g.datasets.iter().any(|d| d.id.0 == id))
                                            .map(|g| g.key.clone())
                                    });
                                    *selected_group_key.write() = next_group_key;
                                    *selected_dataset_id.write() = selected_id;
                                    *column_search_col.write() = None;
                                    *column_search_text.write() = String::new();
                                    *sort_col.write() = None;
                                    *sort_desc.write() = false;
                                    *page.write() = 0;
                                    staged_cells.write().clear();
                                    deleted_rows.write().clear();
                                    selected_rows.write().clear();
                                    *editing_cell.write() = None;
                                    editing_value.set(String::new());
                                    added_rows.write().clear();
                                    show_add_row.set(false);
                                    new_row_inputs.write().clear();
                                    context_menu.set(None);
                                    context_row.set(None);

                                    let options = QueryOptions {
                                        global_search: global_search(),
                                        column_search_col: column_search_col(),
                                        column_search_text: column_search_text(),
                                        sort_col: sort_col(),
                                        sort_desc: sort_desc(),
                                    };

                                    match reload_page_data_usecase(
                                        &query_service_for_import,
                                        selected_id,
                                        0,
                                        &options,
                                    ) {
                                        Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                            *columns.write() = loaded_columns;
                                            *rows.write() = loaded_rows;
                                            *total_rows.write() = loaded_total;
                                            *page.write() = loaded_page;
                                            *status.write() = if is_xlsx {
                                                format!("已匯入 XLSX，共 {} 個資料表", imported_count)
                                            } else {
                                                format!("已匯入 CSV（{} 筆）", imported_count)
                                            };
                                        }
                                        Err(err) => {
                                            *columns.write() = Vec::new();
                                            *rows.write() = Vec::new();
                                            *total_rows.write() = 0;
                                            *page.write() = 0;
                                            *status.write() = format!("匯入成功，但載入資料失敗：{err}");
                                        }
                                    }
                                }
                                Err(err) => {
                                    *status.write() = format!("匯入成功，但刷新資料集失敗：{err}");
                                }
                            },
                            Err(err) => {
                                *status.write() = format!("匯入失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    "匯入資料"
                }

                label {
                    input {
                        r#type: "checkbox",
                        checked: show_deleted(),
                        onchange: move |event| {
                            let checked = event.value().parse::<bool>().unwrap_or(false);
                            *busy.write() = true;
                            *show_deleted.write() = checked;

                            match query_service_for_show_deleted.list_datasets(checked) {
                                Ok(available) => {
                                    let groups = build_dataset_groups(&available);
                                    *datasets.write() = available;

                                    let next_group = selected_group_key()
                                        .and_then(|current| groups.iter().find(|g| g.key == current).map(|g| g.key.clone()))
                                        .or_else(|| groups.first().map(|g| g.key.clone()));
                                    let next_dataset = next_group
                                        .as_ref()
                                        .and_then(|k| groups.iter().find(|g| &g.key == k))
                                        .and_then(|g| g.datasets.first())
                                        .map(|d| d.id.0);

                                    *selected_group_key.write() = next_group;
                                    *selected_dataset_id.write() = next_dataset;
                                    staged_cells.write().clear();
                                    deleted_rows.write().clear();
                                    selected_rows.write().clear();
                                    *editing_cell.write() = None;
                                    editing_value.set(String::new());
                                    added_rows.write().clear();
                                    show_add_row.set(false);
                                    new_row_inputs.write().clear();
                                    context_menu.set(None);
                                    context_row.set(None);

                                    let options = QueryOptions {
                                        global_search: global_search(),
                                        column_search_col: column_search_col(),
                                        column_search_text: column_search_text(),
                                        sort_col: sort_col(),
                                        sort_desc: sort_desc(),
                                    };

                                    match reload_page_data_usecase(
                                        &query_service_for_show_deleted,
                                        next_dataset,
                                        0,
                                        &options,
                                    ) {
                                        Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                            *columns.write() = loaded_columns;
                                            *rows.write() = loaded_rows;
                                            *total_rows.write() = loaded_total;
                                            *page.write() = loaded_page;
                                            *status.write() = if checked {
                                                "已顯示已刪除資料集".to_string()
                                            } else {
                                                "已隱藏已刪除資料集".to_string()
                                            };
                                        }
                                        Err(err) => {
                                            *columns.write() = Vec::new();
                                            *rows.write() = Vec::new();
                                            *total_rows.write() = 0;
                                            *page.write() = 0;
                                            *status.write() = format!("切換顯示狀態失敗：{err}");
                                        }
                                    }
                                }
                                Err(err) => {
                                    *status.write() = format!("刷新資料集失敗：{err}");
                                }
                            }

                            *busy.write() = false;
                        },
                    }
                    "顯示已刪除"
                }

                span { " {status}" }
            }

            div {
                label { "資料集 " }
                select {
                    disabled: busy(),
                    value: selected_group_key()
                        .map(|key| key.to_string())
                        .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                    onchange: {
                        let query_service_for_dataset_change =
                            query_service_for_dataset_change.clone();
                        move |event| {
                        let value = event.value();
                        let groups = build_dataset_groups(&datasets());
                        let next_group = if value == NONE_OPTION_VALUE {
                            None::<String>
                        } else {
                            Some(value)
                        };
                        let next_dataset = next_group
                            .as_ref()
                            .and_then(|group_key| groups.iter().find(|g| &g.key == group_key))
                            .and_then(|g| g.datasets.first())
                            .map(|d| d.id.0);

                        if is_holdings && has_pending_changes {
                            pending_action.set(Some(PendingAction::DatasetChange {
                                next_group: next_group.clone(),
                                next_dataset,
                            }));
                            show_save_prompt.set(true);
                            return;
                        }

                        *selected_group_key.write() = next_group;
                        *selected_dataset_id.write() = next_dataset;
                        *column_search_col.write() = None;
                        *column_search_text.write() = String::new();
                        *sort_col.write() = None;
                        *sort_desc.write() = false;
                        *page.write() = 0;
                        staged_cells.write().clear();
                        deleted_rows.write().clear();
                        selected_rows.write().clear();
                        *editing_cell.write() = None;
                        editing_value.set(String::new());
                        added_rows.write().clear();
                        show_add_row.set(false);
                        new_row_inputs.write().clear();
                        context_menu.set(None);
                        context_row.set(None);
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: global_search(),
                            column_search_col: column_search_col(),
                            column_search_text: column_search_text(),
                            sort_col: sort_col(),
                            sort_desc: sort_desc(),
                        };

                        match reload_page_data_usecase(
                            &query_service_for_dataset_change,
                            next_dataset,
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已切換資料集".to_string();
                            }
                            Err(err) => {
                                *columns.write() = Vec::new();
                                *rows.write() = Vec::new();
                                *total_rows.write() = 0;
                                *page.write() = 0;
                                *status.write() = format!("載入資料集失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                        }
                    },
                    option { value: "{NONE_OPTION_VALUE}", "請選擇資料集" }
                    for group in grouped_datasets.clone() {
                        option {
                            value: "{group.key}",
                            "{group.label}"
                        }
                    }
                }

                button {
                    disabled: busy() || selected_dataset_id().is_none(),
                    onclick: move |_| {
                        let Some(dataset_id) = selected_dataset_id() else {
                            return;
                        };

                        let confirmed = MessageDialog::new()
                            .set_level(MessageLevel::Warning)
                            .set_title("確認刪除")
                            .set_description("確定要刪除此資料集嗎？可在顯示已刪除中查看。")
                            .set_buttons(MessageButtons::YesNo)
                            .show();
                        if confirmed != MessageDialogResult::Yes {
                            return;
                        }

                        *busy.write() = true;
                        match edit_service_for_soft_delete
                            .soft_delete_dataset(DatasetId(dataset_id))
                            .map_err(|err| anyhow!(err.to_string()))
                            .and_then(|_| {
                                query_service_for_soft_delete
                                    .list_datasets(show_deleted())
                                    .map_err(|err| anyhow!(err.to_string()))
                            }) {
                            Ok(available) => {
                                let groups = build_dataset_groups(&available);
                                *datasets.write() = available;

                                let next_group = selected_group_key()
                                    .and_then(|current| groups.iter().find(|g| g.key == current).map(|g| g.key.clone()))
                                    .or_else(|| groups.first().map(|g| g.key.clone()));
                                let next_dataset = next_group
                                    .as_ref()
                                    .and_then(|k| groups.iter().find(|g| &g.key == k))
                                    .and_then(|g| g.datasets.first())
                                    .map(|d| d.id.0);

                                *selected_group_key.write() = next_group;
                                *selected_dataset_id.write() = next_dataset;

                                let options = QueryOptions {
                                    global_search: global_search(),
                                    column_search_col: column_search_col(),
                                    column_search_text: column_search_text(),
                                    sort_col: sort_col(),
                                    sort_desc: sort_desc(),
                                };

                                match reload_page_data_usecase(
                                    &query_service_for_soft_delete,
                                    next_dataset,
                                    0,
                                    &options,
                                ) {
                                    Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                        *columns.write() = loaded_columns;
                                        *rows.write() = loaded_rows;
                                        *total_rows.write() = loaded_total;
                                        *page.write() = loaded_page;
                                        *status.write() = "已刪除資料集（可復原）".to_string();
                                    }
                                    Err(err) => {
                                        *columns.write() = Vec::new();
                                        *rows.write() = Vec::new();
                                        *total_rows.write() = 0;
                                        *page.write() = 0;
                                        *status.write() = format!("刪除成功，但重新載入失敗：{err}");
                                    }
                                }
                            }
                            Err(err) => {
                                *status.write() = format!("刪除資料集失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    "刪除資料集"
                }

                button {
                    disabled: busy() || selected_dataset_id().is_none(),
                    onclick: move |_| {
                        let Some(dataset_id) = selected_dataset_id() else {
                            return;
                        };

                        let confirmed = MessageDialog::new()
                            .set_level(MessageLevel::Warning)
                            .set_title("確認永久刪除")
                            .set_description("確定要永久刪除此資料集嗎？此動作不可復原。")
                            .set_buttons(MessageButtons::YesNo)
                            .show();
                        if confirmed != MessageDialogResult::Yes {
                            return;
                        }

                        *busy.write() = true;
                        match edit_service_for_purge
                            .purge_dataset(DatasetId(dataset_id))
                            .map_err(|err| anyhow!(err.to_string()))
                            .and_then(|_| {
                                query_service_for_purge
                                    .list_datasets(show_deleted())
                                    .map_err(|err| anyhow!(err.to_string()))
                            }) {
                            Ok(available) => {
                                let groups = build_dataset_groups(&available);
                                *datasets.write() = available;

                                let next_group = selected_group_key()
                                    .and_then(|current| groups.iter().find(|g| g.key == current).map(|g| g.key.clone()))
                                    .or_else(|| groups.first().map(|g| g.key.clone()));
                                let next_dataset = next_group
                                    .as_ref()
                                    .and_then(|k| groups.iter().find(|g| &g.key == k))
                                    .and_then(|g| g.datasets.first())
                                    .map(|d| d.id.0);

                                *selected_group_key.write() = next_group;
                                *selected_dataset_id.write() = next_dataset;

                                let options = QueryOptions {
                                    global_search: global_search(),
                                    column_search_col: column_search_col(),
                                    column_search_text: column_search_text(),
                                    sort_col: sort_col(),
                                    sort_desc: sort_desc(),
                                };

                                match reload_page_data_usecase(
                                    &query_service_for_purge,
                                    next_dataset,
                                    0,
                                    &options,
                                ) {
                                    Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                        *columns.write() = loaded_columns;
                                        *rows.write() = loaded_rows;
                                        *total_rows.write() = loaded_total;
                                        *page.write() = loaded_page;
                                        *status.write() = "已永久刪除資料集".to_string();
                                    }
                                    Err(err) => {
                                        *columns.write() = Vec::new();
                                        *rows.write() = Vec::new();
                                        *total_rows.write() = 0;
                                        *page.write() = 0;
                                        *status.write() = format!("永久刪除成功，但重新載入失敗：{err}");
                                    }
                                }
                            }
                            Err(err) => {
                                *status.write() = format!("永久刪除資料集失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    "永久刪除"
                }
            }

            if let Some(group) = active_group.clone() {
                if group.datasets.len() > 1 {
                    div {
                        style: "display: flex; gap: 6px; margin: 8px 0;",
                        for sheet in group.datasets {
                            button {
                                disabled: busy(),
                                onclick: {
                                    let query_service_for_tab_switch =
                                        query_service_for_tab_switch.clone();
                                    move |_| {
                                        if is_holdings && has_pending_changes {
                                            pending_action.set(Some(PendingAction::TabSwitch {
                                                dataset_id: sheet.id.0,
                                            }));
                                            show_save_prompt.set(true);
                                            return;
                                        }
                                        *selected_dataset_id.write() = Some(sheet.id.0);
                                        *page.write() = 0;
                                        staged_cells.write().clear();
                                        deleted_rows.write().clear();
                                        selected_rows.write().clear();
                                        *editing_cell.write() = None;
                                        editing_value.set(String::new());
                                        added_rows.write().clear();
                                        show_add_row.set(false);
                                        new_row_inputs.write().clear();
                                        context_menu.set(None);
                                        context_row.set(None);
                                        *busy.write() = true;

                                        let options = QueryOptions {
                                            global_search: global_search(),
                                            column_search_col: column_search_col(),
                                            column_search_text: column_search_text(),
                                            sort_col: sort_col(),
                                            sort_desc: sort_desc(),
                                        };

                                        match reload_page_data_usecase(
                                            &query_service_for_tab_switch,
                                            Some(sheet.id.0),
                                            0,
                                            &options,
                                        ) {
                                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                                *columns.write() = loaded_columns;
                                                *rows.write() = loaded_rows;
                                                *total_rows.write() = loaded_total;
                                                *page.write() = loaded_page;
                                                *status.write() = format!("已切換工作表：{}", sheet.name);
                                            }
                                            Err(err) => {
                                                *status.write() = format!("切換工作表失敗：{err}");
                                            }
                                        }

                                        *busy.write() = false;
                                    }
                                },
                                if Some(sheet.id.0) == selected_dataset_id() {
                                    "[{sheet.name}]"
                                } else {
                                    "{sheet.name}"
                                }
                            }
                        }
                    }
                }
            }

            div {
                label { "全欄位搜尋 " }
                input {
                    disabled: busy(),
                    value: global_search(),
                    placeholder: "輸入關鍵字",
                    onchange: move |event| {
                        let next_global = event.value();
                        *global_search.write() = next_global.clone();
                        *page.write() = 0;
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: next_global,
                            column_search_col: column_search_col(),
                            column_search_text: column_search_text(),
                            sort_col: sort_col(),
                            sort_desc: sort_desc(),
                        };

                        match reload_page_data_usecase(
                            &query_service_for_global_search,
                            selected_dataset_id(),
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已套用全欄位搜尋".to_string();
                            }
                            Err(err) => {
                                *status.write() = format!("全欄位搜尋失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                }
            }

            div {
                label { "欄位篩選 " }
                select {
                    disabled: busy() || columns().is_empty(),
                    value: column_search_col()
                        .map(|col| col.to_string())
                        .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                    onchange: move |event| {
                        let value = event.value();
                        let next_col = if value == NONE_OPTION_VALUE {
                            None
                        } else {
                            value.parse::<i64>().ok()
                        };

                        *column_search_col.write() = next_col;
                        *page.write() = 0;
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: global_search(),
                            column_search_col: next_col,
                            column_search_text: column_search_text(),
                            sort_col: sort_col(),
                            sort_desc: sort_desc(),
                        };

                        match reload_page_data_usecase(
                            &query_service_for_column_select,
                            selected_dataset_id(),
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已更新欄位篩選".to_string();
                            }
                            Err(err) => {
                                *status.write() = format!("欄位篩選失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    option { value: "{NONE_OPTION_VALUE}", "任一欄位" }
                    for (idx, col_name) in columns().into_iter().enumerate() {
                        option {
                            value: "{idx as i64}",
                            "{col_name}"
                        }
                    }
                }

                input {
                    disabled: busy() || column_search_col().is_none(),
                    value: column_search_text(),
                    placeholder: "搜尋指定欄位",
                    onchange: move |event| {
                        let next_text = event.value();
                        *column_search_text.write() = next_text.clone();
                        *page.write() = 0;
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: global_search(),
                            column_search_col: column_search_col(),
                            column_search_text: next_text,
                            sort_col: sort_col(),
                            sort_desc: sort_desc(),
                        };

                        match reload_page_data_usecase(
                            &query_service_for_column_search,
                            selected_dataset_id(),
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已套用欄位搜尋".to_string();
                            }
                            Err(err) => {
                                *status.write() = format!("欄位搜尋失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                }
            }

            div {
                label { "排序 " }
                select {
                    disabled: busy() || columns().is_empty(),
                    value: sort_col()
                        .map(|col| col.to_string())
                        .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                    onchange: move |event| {
                        let value = event.value();
                        let next_sort_col = if value == NONE_OPTION_VALUE {
                            None
                        } else {
                            value.parse::<i64>().ok()
                        };

                        *sort_col.write() = next_sort_col;
                        *page.write() = 0;
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: global_search(),
                            column_search_col: column_search_col(),
                            column_search_text: column_search_text(),
                            sort_col: next_sort_col,
                            sort_desc: sort_desc(),
                        };

                        match reload_page_data_usecase(
                            &query_service_for_sort_select,
                            selected_dataset_id(),
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已更新排序欄位".to_string();
                            }
                            Err(err) => {
                                *status.write() = format!("排序欄位設定失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    option { value: "{NONE_OPTION_VALUE}", "列原始順序" }
                    for (idx, col_name) in columns().into_iter().enumerate() {
                        option {
                            value: "{idx as i64}",
                            "{col_name}"
                        }
                    }
                }

                button {
                    disabled: busy(),
                    onclick: move |_| {
                        let next_desc = !sort_desc();
                        *sort_desc.write() = next_desc;
                        *page.write() = 0;
                        *busy.write() = true;

                        let options = QueryOptions {
                            global_search: global_search(),
                            column_search_col: column_search_col(),
                            column_search_text: column_search_text(),
                            sort_col: sort_col(),
                            sort_desc: next_desc,
                        };

                        match reload_page_data_usecase(
                            &query_service_for_sort_toggle,
                            selected_dataset_id(),
                            0,
                            &options,
                        ) {
                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                *columns.write() = loaded_columns;
                                *rows.write() = loaded_rows;
                                *total_rows.write() = loaded_total;
                                *page.write() = loaded_page;
                                *status.write() = "已更新排序方向".to_string();
                            }
                            Err(err) => {
                                *status.write() = format!("排序方向設定失敗：{err}");
                            }
                        }

                        *busy.write() = false;
                    },
                    if sort_desc() { "降冪" } else { "升冪" }
                }
            }

            div {
                span { "共 {current_total_rows} 筆" }
            }

            if is_holdings {
                div { style: "display: flex; gap: 8px; align-items: center; margin: 8px 0;",
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            if busy() {
                                return;
                            }
                            let mut inputs = new_row_inputs.write();
                            inputs.clear();
                            for col in &required_columns {
                                inputs.insert(col.clone(), String::new());
                            }
                            show_add_row.set(true);
                        },
                        "新增列"
                    }
                    if has_pending_changes {
                        span { style: "color: #0f5132;", "尚未儲存變更" }
                    }
                }
                if show_add_row() {
                    div { style: "border: 1px solid #c7c7c7; padding: 8px; margin-bottom: 8px;",
                        div { style: "margin-bottom: 6px;", "新增列（必填欄位）" }
                        for col in required_columns.iter() {
                            div { style: "display: flex; gap: 8px; align-items: center; margin-bottom: 6px;",
                                label { style: "min-width: 90px;", "{col}" }
                                input {
                                    value: new_row_inputs().get(col).cloned().unwrap_or_default(),
                                    oninput: {
                                        let col = col.clone();
                                        move |event| {
                                            new_row_inputs
                                                .write()
                                                .insert(col.clone(), event.value());
                                        }
                                    }
                                }
                            }
                        }
                        div { style: "display: flex; gap: 8px;",
                            button {
                                onclick: move |_| {
                                    let mut row = vec![String::new(); current_columns_for_add.len()];
                                    for (idx, header) in current_columns_for_add.iter().enumerate() {
                                        if let Some(value) = new_row_inputs().get(header).cloned() {
                                            row[idx] = value;
                                        }
                                    }
                                    match validate_required_holdings_row(&current_columns_for_add, &row) {
                                        Ok(_) => {
                                            added_rows.write().push(row);
                                            show_add_row.set(false);
                                            new_row_inputs.write().clear();
                                            *status.write() = "已新增列（待儲存）".to_string();
                                        }
                                        Err(err) => {
                                            *status.write() = format!("新增列失敗：{err}");
                                        }
                                    }
                                },
                                "新增"
                            }
                            button {
                                onclick: move |_| {
                                    show_add_row.set(false);
                                    new_row_inputs.write().clear();
                                },
                                "取消"
                            }
                        }
                    }
                }
            }

            table { style: "border-collapse: collapse; width: 100%; border: 1px solid #bbb;",
                thead {
                    tr {
                        for (idx, header) in current_columns.iter().enumerate() {
                            th {
                                style: "border: 1px solid #bbb; padding: 6px; background: #f2f2f2; text-align: {column_alignments[idx]};",
                                "{header}"
                            }
                        }
                    }
                }
                tbody {
                    if total_row_count == 0 {
                        tr {
                            td { style: "border: 1px solid #bbb; padding: 6px;",
                                colspan: current_columns.len().max(1),
                                "無資料"
                            }
                        }
                    } else {
                        for row in row_render_models.clone() {
                            tr {
                                style: "{row.style}",
                                onclick: move |_| {
                                    if !is_holdings {
                                        return;
                                    }
                                    let mut selected = selected_rows.write();
                                    if selected.contains(&row.row_idx) {
                                        selected.clear();
                                    } else {
                                        selected.clear();
                                        selected.insert(row.row_idx);
                                    }
                                },
                                oncontextmenu: move |event| {
                                    if !is_holdings {
                                        return;
                                    }
                                    let coords = event.client_coordinates();
                                    context_menu.set(Some((coords.x, coords.y)));
                                    context_row.set(Some(row.row_idx));
                                },
                                for cell in row.cells.clone() {
                                    td {
                                        style: "{cell.style}",
                                        ondoubleclick: move |_| {
                                            if !is_holdings || row.is_deleted {
                                                return;
                                            }
                                            if !cell.is_editable {
                                                return;
                                            }
                                            *editing_cell.write() = Some(CellKey {
                                                row_idx: cell.row_idx,
                                                col_idx: cell.col_idx,
                                                column: cell.header.clone(),
                                            });
                                            editing_value.set(cell.raw.clone());
                                        },
                                        if cell.is_editing {
                                            input {
                                                value: "{editing_value()}",
                                                oninput: move |event| {
                                                    editing_value.set(event.value());
                                                },
                                                onkeydown: move |event| {
                                                    if event.key() == Key::Enter {
                                                        if let Some(active) = editing_cell() {
                                                            let value = editing_value();
                                                            let numeric_required = matches!(
                                                                active.column.as_str(),
                                                                "買進" | "市價" | "數量" | "期數"
                                                            );
                                                            if numeric_required
                                                                && parse_numeric_value(&value).is_none()
                                                            {
                                                                *status.write() = format!(
                                                                    "欄位 {} 必須是數字",
                                                                    active.column
                                                                );
                                                                return;
                                                            }
                                                            if active.row_idx < base_row_count {
                                                                staged_cells.write().insert(active, value);
                                                            } else {
                                                                let new_idx = active.row_idx - base_row_count;
                                                                if let Some(row) = added_rows.write().get_mut(new_idx) {
                                                                    if active.col_idx < row.len() {
                                                                        row[active.col_idx] = value;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        *editing_cell.write() = None;
                                                    } else if event.key() == Key::Escape {
                                                        *editing_cell.write() = None;
                                                    }
                                                }
                                            }
                                        } else {
                                            "{cell.formatted}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if is_holdings {
                if let Some((x, y)) = context_menu() {
                    div {
                        style: "position: fixed; left: {x}px; top: {y}px; background: #fff; border: 1px solid #999; padding: 6px; z-index: 1000;",
                        button {
                            onclick: move |_| {
                                let mut targets = selected_rows();
                                if targets.is_empty() {
                                    if let Some(row) = context_row() {
                                        targets.insert(row);
                                    }
                                }
                                for row in targets {
                                    deleted_rows.write().insert(row);
                                }
                                context_menu.set(None);
                                context_row.set(None);
                                *status.write() = "已標記刪除（待儲存）".to_string();
                            },
                            "刪除"
                        }
                    }
                }
            }

            if show_save_prompt() {
                div {
                    style: "position: fixed; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 1100;",
                    div {
                        style: "background: #fff; padding: 16px; border: 1px solid #999; min-width: 280px;",
                        div { style: "margin-bottom: 8px; font-weight: 600;", "未儲存變更" }
                        div { style: "margin-bottom: 12px;", "你要覆蓋目前資料集，或另存舊內容？" }
                        div { style: "display: flex; gap: 8px;",
                            button {
                                onclick: {
                                    let query_service_for_dataset_change =
                                        query_service_for_dataset_change.clone();
                                    let query_service_for_tab_switch =
                                        query_service_for_tab_switch.clone();
                                    move |_| {
                                        let Some(dataset_id) = selected_dataset_id() else {
                                            show_save_prompt.set(false);
                                            pending_action.set(None);
                                            return;
                                        };

                                        let edits = StagedEdits {
                                            staged_cells: staged_cells(),
                                            deleted_rows: deleted_rows(),
                                            added_rows: added_rows(),
                                        };
                                        if let Err(err) = edit_service_for_save
                                            .apply_edits(DatasetId(dataset_id), edits)
                                            .map_err(|err| anyhow!(err.to_string()))
                                        {
                                            *status.write() = format!("覆蓋失敗：{err}");
                                            return;
                                        }

                                        staged_cells.write().clear();
                                        deleted_rows.write().clear();
                                        selected_rows.write().clear();
                                        added_rows.write().clear();
                                        *editing_cell.write() = None;
                                        editing_value.set(String::new());
                                        show_add_row.set(false);
                                        new_row_inputs.write().clear();

                                        match reload_page_data_usecase(
                                            &query_service_for_save,
                                            Some(dataset_id),
                                            0,
                                            &QueryOptions {
                                                global_search: global_search(),
                                                column_search_col: column_search_col(),
                                                column_search_text: column_search_text(),
                                                sort_col: sort_col(),
                                                sort_desc: sort_desc(),
                                            },
                                        ) {
                                            Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                                *columns.write() = loaded_columns;
                                                *rows.write() = loaded_rows;
                                                *total_rows.write() = loaded_total;
                                                *page.write() = loaded_page;
                                            }
                                            Err(err) => {
                                                *status.write() = format!("覆蓋後重新載入失敗：{err}");
                                            }
                                        }

                                        show_save_prompt.set(false);
                                        if let Some(action) = pending_action() {
                                            pending_action.set(None);
                                            match action {
                                                PendingAction::Import(file_path) => {
                                                    *busy.write() = true;
                                                    *status.write() =
                                                        format!("正在匯入 {}", file_path.display());
                                                    let ext = file_path
                                                        .extension()
                                                        .and_then(|e| e.to_str())
                                                        .map(|s| s.to_ascii_lowercase())
                                                        .unwrap_or_default();
                                                    let import_result = if ext == "xlsx" {
                                                        import_service_for_import_overwrite
                                                            .import_xlsx(&file_path)
                                                            .map(|items| {
                                                                (
                                                                    items.first().map(|it| it.dataset_id),
                                                                    items.len() as i64,
                                                                    true,
                                                                )
                                                            })
                                                    } else {
                                                        import_service_for_import_overwrite
                                                            .import_csv(&file_path)
                                                            .map(|item| {
                                                                (Some(item.dataset_id), item.row_count, false)
                                                            })
                                                    };
                                                    match import_result {
                                                        Ok((selected_id, imported_count, is_xlsx)) => {
                                                            match query_service_for_import_overwrite
                                                                .list_datasets(show_deleted())
                                                            {
                                                                Ok(available) => {
                                                                    let groups =
                                                                        build_dataset_groups(&available);
                                                                    *datasets.write() = available;
                                                                    let next_group_key =
                                                                        selected_id.and_then(|id| {
                                                                            groups
                                                                                .iter()
                                                                                .find(|g| {
                                                                                    g.datasets
                                                                                        .iter()
                                                                                        .any(|d| d.id.0 == id)
                                                                                })
                                                                                .map(|g| g.key.clone())
                                                                        });
                                                                    *selected_group_key.write() = next_group_key;
                                                                    *selected_dataset_id.write() = selected_id;
                                                                    *column_search_col.write() = None;
                                                                    *column_search_text.write() = String::new();
                                                                    *sort_col.write() = None;
                                                                    *sort_desc.write() = false;
                                                                    *page.write() = 0;
                                                                    match reload_page_data_usecase(
                                                                        &query_service_for_import_overwrite,
                                                                        selected_id,
                                                                        0,
                                                                        &QueryOptions::default(),
                                                                    ) {
                                                                        Ok((
                                                                            loaded_columns,
                                                                            loaded_rows,
                                                                            loaded_total,
                                                                            loaded_page,
                                                                        )) => {
                                                                            *columns.write() = loaded_columns;
                                                                            *rows.write() = loaded_rows;
                                                                            *total_rows.write() = loaded_total;
                                                                            *page.write() = loaded_page;
                                                                            *status.write() = if is_xlsx {
                                                                                format!(
                                                                                    "已匯入 XLSX，共 {} 個資料表",
                                                                                    imported_count
                                                                                )
                                                                            } else {
                                                                                format!(
                                                                                    "已匯入 CSV（{} 筆）",
                                                                                    imported_count
                                                                                )
                                                                            };
                                                                        }
                                                                        Err(err) => {
                                                                            *status.write() =
                                                                                format!("匯入成功，但載入資料失敗：{err}");
                                                                        }
                                                                    }
                                                                }
                                                                Err(err) => {
                                                                    *status.write() = format!(
                                                                        "匯入成功，但刷新資料集失敗：{err}"
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Err(err) => {
                                                            *status.write() = format!("匯入失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                                PendingAction::DatasetChange { next_group, next_dataset } => {
                                                    *selected_group_key.write() = next_group;
                                                    *selected_dataset_id.write() = next_dataset;
                                                    *column_search_col.write() = None;
                                                    *column_search_text.write() = String::new();
                                                    *sort_col.write() = None;
                                                    *sort_desc.write() = false;
                                                    *page.write() = 0;
                                                    *busy.write() = true;
                                                    match reload_page_data_usecase(
                                                        &query_service_for_dataset_change,
                                                        next_dataset,
                                                        0,
                                                        &QueryOptions::default(),
                                                    ) {
                                                        Ok((
                                                            loaded_columns,
                                                            loaded_rows,
                                                            loaded_total,
                                                            loaded_page,
                                                        )) => {
                                                            *columns.write() = loaded_columns;
                                                            *rows.write() = loaded_rows;
                                                            *total_rows.write() = loaded_total;
                                                            *page.write() = loaded_page;
                                                            *status.write() = "已切換資料集".to_string();
                                                        }
                                                        Err(err) => {
                                                            *status.write() =
                                                                format!("載入資料集失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                                PendingAction::TabSwitch { dataset_id } => {
                                                    *selected_dataset_id.write() = Some(dataset_id);
                                                    *page.write() = 0;
                                                    *busy.write() = true;
                                                    match reload_page_data_usecase(
                                                        &query_service_for_tab_switch,
                                                        Some(dataset_id),
                                                        0,
                                                        &QueryOptions::default(),
                                                    ) {
                                                        Ok((
                                                            loaded_columns,
                                                            loaded_rows,
                                                            loaded_total,
                                                            loaded_page,
                                                        )) => {
                                                            *columns.write() = loaded_columns;
                                                            *rows.write() = loaded_rows;
                                                            *total_rows.write() = loaded_total;
                                                            *page.write() = loaded_page;
                                                            *status.write() = "已切換工作表".to_string();
                                                        }
                                                        Err(err) => {
                                                            *status.write() =
                                                                format!("切換工作表失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                            }
                                        }
                                    }
                                },
                            "覆蓋"
                            }
                            button {
                                onclick: move |_| {
                                    save_as_name.set(default_dataset_name_mmdd());
                                    show_save_prompt.set(false);
                                    show_save_as_prompt.set(true);
                                },
                                "另存"
                            }
                            button {
                                onclick: move |_| {
                                    show_save_prompt.set(false);
                                    pending_action.set(None);
                                },
                                "取消"
                            }
                        }
                    }
                }
            }

            if show_save_as_prompt() {
                div {
                    style: "position: fixed; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 1200;",
                    div {
                        style: "background: #fff; padding: 16px; border: 1px solid #999; min-width: 280px;",
                        div { style: "margin-bottom: 8px; font-weight: 600;", "另存舊內容" }
                        div { style: "margin-bottom: 8px;", "請輸入新資料集名稱（預設 MMDD）" }
                        input {
                            value: save_as_name(),
                            oninput: move |event| {
                                save_as_name.set(event.value());
                            }
                        }
                        div { style: "display: flex; gap: 8px; margin-top: 12px;",
                            button {
                                onclick: {
                                    let query_service_for_dataset_change =
                                        query_service_for_dataset_change.clone();
                                    let query_service_for_tab_switch =
                                        query_service_for_tab_switch.clone();
                                    let query_service_for_import_save_as =
                                        query_service_for_import_save_as.clone();
                                    let import_service_for_import_save_as =
                                        import_service_for_import_save_as.clone();
                                    move |_| {
                                        let name = save_as_name().trim().to_string();
                                        if name.is_empty() {
                                            *status.write() = "資料集名稱不可空白".to_string();
                                            return;
                                        }
                                        let Some(dataset_id) = selected_dataset_id() else {
                                            show_save_as_prompt.set(false);
                                            pending_action.set(None);
                                            return;
                                        };
                                        if let Some(current) =
                                            datasets_for_save.iter().find(|d| d.id.0 == dataset_id)
                                        {
                                            if current.name == name {
                                                *status.write() = "資料集名稱必須不同".to_string();
                                                return;
                                            }
                                        }
                                        let existing =
                                            datasets_for_save.iter().find(|d| d.name == name).cloned();
                                        if let Some(existing) = existing {
                                            let overwrite = MessageDialog::new()
                                                .set_level(MessageLevel::Warning)
                                                .set_title("名稱已存在")
                                                .set_description("已有相同名稱，是否覆蓋？")
                                                .set_buttons(MessageButtons::YesNo)
                                                .show();
                                            if overwrite != MessageDialogResult::Yes {
                                                return;
                                            }
                                            if let Err(err) = edit_service_for_save_as
                                                .purge_dataset(existing.id)
                                                .map_err(|err| anyhow!(err.to_string()))
                                            {
                                                *status.write() = format!("覆蓋失敗：{err}");
                                                return;
                                            }
                                        }

                                        let Some(current) =
                                            datasets_for_save.iter().find(|d| d.id.0 == dataset_id)
                                        else {
                                            *status.write() = "找不到目前資料集".to_string();
                                            return;
                                        };
                                        let prefix = current
                                            .source_path
                                            .split_once('#')
                                            .map(|(p, _)| p)
                                            .unwrap_or(&current.source_path);
                                        let backup_source = format!("{prefix}#{name}");

                                        if let Err(err) = edit_service_for_save_as
                                            .create_dataset(
                                                NewDatasetMeta {
                                                    name: name.clone(),
                                                    source_path: backup_source,
                                                },
                                                TabularData {
                                                    columns: current_columns_for_save_as.clone(),
                                                    rows: current_rows_for_save_as.clone(),
                                                },
                                            )
                                            .map_err(|err| anyhow!(err.to_string()))
                                        {
                                            *status.write() = format!("另存失敗：{err}");
                                            return;
                                        }

                                        let edits = StagedEdits {
                                            staged_cells: staged_cells(),
                                            deleted_rows: deleted_rows(),
                                            added_rows: added_rows(),
                                        };
                                        if let Err(err) = edit_service_for_save_as
                                            .apply_edits(DatasetId(dataset_id), edits)
                                            .map_err(|err| anyhow!(err.to_string()))
                                        {
                                            *status.write() = format!("覆蓋失敗：{err}");
                                            return;
                                        }

                                        match query_service_for_save_as.list_datasets(show_deleted()) {
                                            Ok(available) => {
                                                *datasets.write() = available;
                                            }
                                            Err(err) => {
                                                *status.write() =
                                                    format!("更新資料集清單失敗：{err}");
                                            }
                                        }

                                        staged_cells.write().clear();
                                        deleted_rows.write().clear();
                                        selected_rows.write().clear();
                                        added_rows.write().clear();
                                        *editing_cell.write() = None;
                                        editing_value.set(String::new());
                                        show_add_row.set(false);
                                        new_row_inputs.write().clear();

                                        show_save_as_prompt.set(false);

                                        if let Some(action) = pending_action() {
                                            pending_action.set(None);
                                            match action {
                                                PendingAction::DatasetChange { next_group, next_dataset } => {
                                                    *selected_group_key.write() = next_group;
                                                    *selected_dataset_id.write() = next_dataset;
                                                    *column_search_col.write() = None;
                                                    *column_search_text.write() = String::new();
                                                    *sort_col.write() = None;
                                                    *sort_desc.write() = false;
                                                    *page.write() = 0;
                                                    *busy.write() = true;
                                                    match reload_page_data_usecase(
                                                        &query_service_for_dataset_change,
                                                        next_dataset,
                                                        0,
                                                        &QueryOptions::default(),
                                                    ) {
                                                        Ok((
                                                            loaded_columns,
                                                            loaded_rows,
                                                            loaded_total,
                                                            loaded_page,
                                                        )) => {
                                                            *columns.write() = loaded_columns;
                                                            *rows.write() = loaded_rows;
                                                            *total_rows.write() = loaded_total;
                                                            *page.write() = loaded_page;
                                                            *status.write() =
                                                                "已切換資料集".to_string();
                                                        }
                                                        Err(err) => {
                                                            *status.write() =
                                                                format!("載入資料集失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                                PendingAction::TabSwitch { dataset_id } => {
                                                    *selected_dataset_id.write() = Some(dataset_id);
                                                    *page.write() = 0;
                                                    *busy.write() = true;
                                                    match reload_page_data_usecase(
                                                        &query_service_for_tab_switch,
                                                        Some(dataset_id),
                                                        0,
                                                        &QueryOptions::default(),
                                                    ) {
                                                        Ok((
                                                            loaded_columns,
                                                            loaded_rows,
                                                            loaded_total,
                                                            loaded_page,
                                                        )) => {
                                                            *columns.write() = loaded_columns;
                                                            *rows.write() = loaded_rows;
                                                            *total_rows.write() = loaded_total;
                                                            *page.write() = loaded_page;
                                                            *status.write() =
                                                                "已切換工作表".to_string();
                                                        }
                                                        Err(err) => {
                                                            *status.write() =
                                                                format!("切換工作表失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                                PendingAction::Import(file_path) => {
                                                    *busy.write() = true;
                                                    *status.write() =
                                                        format!("正在匯入 {}", file_path.display());
                                                    let ext = file_path
                                                        .extension()
                                                        .and_then(|e| e.to_str())
                                                        .map(|s| s.to_ascii_lowercase())
                                                        .unwrap_or_default();
                                                    let import_result = if ext == "xlsx" {
                                                        import_service_for_import_save_as
                                                            .import_xlsx(&file_path)
                                                            .map(|items| {
                                                                (
                                                                    items.first().map(|it| it.dataset_id),
                                                                    items.len() as i64,
                                                                    true,
                                                                )
                                                            })
                                                    } else {
                                                        import_service_for_import_save_as
                                                            .import_csv(&file_path)
                                                            .map(|item| {
                                                                (Some(item.dataset_id), item.row_count, false)
                                                            })
                                                    };
                                                    match import_result {
                                                        Ok((selected_id, imported_count, is_xlsx)) => {
                                                            match query_service_for_import_save_as
                                                                .list_datasets(show_deleted())
                                                            {
                                                                Ok(available) => {
                                                                    let groups =
                                                                        build_dataset_groups(&available);
                                                                    *datasets.write() = available;
                                                                    let next_group_key =
                                                                        selected_id.and_then(|id| {
                                                                            groups
                                                                                .iter()
                                                                                .find(|g| {
                                                                                    g.datasets
                                                                                        .iter()
                                                                                        .any(|d| d.id.0 == id)
                                                                                })
                                                                                .map(|g| g.key.clone())
                                                                        });
                                                                    *selected_group_key.write() = next_group_key;
                                                                    *selected_dataset_id.write() = selected_id;
                                                                    *column_search_col.write() = None;
                                                                    *column_search_text.write() = String::new();
                                                                    *sort_col.write() = None;
                                                                    *sort_desc.write() = false;
                                                                    *page.write() = 0;
                                                                    match reload_page_data_usecase(
                                                                        &query_service_for_import_save_as,
                                                                        selected_id,
                                                                        0,
                                                                        &QueryOptions::default(),
                                                                    ) {
                                                                        Ok((
                                                                            loaded_columns,
                                                                            loaded_rows,
                                                                            loaded_total,
                                                                            loaded_page,
                                                                        )) => {
                                                                            *columns.write() = loaded_columns;
                                                                            *rows.write() = loaded_rows;
                                                                            *total_rows.write() = loaded_total;
                                                                            *page.write() = loaded_page;
                                                                            *status.write() = if is_xlsx {
                                                                                format!(
                                                                                    "已匯入 XLSX，共 {} 個資料表",
                                                                                    imported_count
                                                                                )
                                                                            } else {
                                                                                format!(
                                                                                    "已匯入 CSV（{} 筆）",
                                                                                    imported_count
                                                                                )
                                                                            };
                                                                        }
                                                                        Err(err) => {
                                                                            *status.write() = format!(
                                                                                "匯入成功，但載入資料失敗：{err}"
                                                                            );
                                                                        }
                                                                    }
                                                                }
                                                                Err(err) => {
                                                                    *status.write() = format!(
                                                                        "匯入成功，但刷新資料集失敗：{err}"
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Err(err) => {
                                                            *status.write() = format!("匯入失敗：{err}");
                                                        }
                                                    }
                                                    *busy.write() = false;
                                                }
                                            }
                                        }
                                    }
                                },
                                "確認"
                            }
                            button {
                                onclick: move |_| {
                                    show_save_as_prompt.set(false);
                                    pending_action.set(None);
                                },
                                "取消"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatasetGroup {
    key: String,
    label: String,
    datasets: Vec<DatasetMeta>,
}

fn dataset_group_key(source_path: &str, id: i64) -> String {
    if let Some((prefix, _)) = source_path.split_once('#') {
        prefix.to_string()
    } else {
        format!("csv:{id}")
    }
}

fn dataset_group_label(source_path: &str, fallback_name: &str, id: i64) -> String {
    if let Some((prefix, _)) = source_path.split_once('#') {
        return Path::new(prefix)
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| fallback_name.to_string());
    }
    format!("{fallback_name}（#{id}）")
}

fn build_dataset_groups(list: &[DatasetMeta]) -> Vec<DatasetGroup> {
    let mut grouped: BTreeMap<String, DatasetGroup> = BTreeMap::new();
    for item in list {
        let id: i64 = item.id.into();
        let key = dataset_group_key(&item.source_path, id);
        let label = dataset_group_label(&item.source_path, &item.name, id);
        let entry = grouped.entry(key.clone()).or_insert_with(|| DatasetGroup {
            key,
            label,
            datasets: Vec::new(),
        });
        entry.datasets.push(item.clone());
    }

    let mut groups: Vec<DatasetGroup> = grouped.into_values().collect();
    for group in &mut groups {
        group.datasets.sort_by_key(|d| d.id.0);
    }
    groups
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImportResult {
    dataset_id: i64,
    row_count: i64,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
struct QueryOptions {
    global_search: String,
    column_search_col: Option<i64>,
    column_search_text: String,
    sort_col: Option<i64>,
    sort_desc: bool,
}

#[allow(dead_code)]
fn default_db_path() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("com", "hellhbbd", "bom")
        .ok_or_else(|| anyhow!("unable to resolve data directory"))?;
    Ok(project_dirs.data_local_dir().join("datasets.sqlite"))
}

fn ensure_webview_data_dir(base_data_dir: &Path) -> Result<PathBuf> {
    let webview_data_dir = base_data_dir.join("webview2");
    std::fs::create_dir_all(&webview_data_dir).with_context(|| {
        format!(
            "failed to create webview dir: {}",
            webview_data_dir.display()
        )
    })?;
    Ok(webview_data_dir)
}

fn default_webview_data_dir() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("com", "hellhbbd", "bom")
        .ok_or_else(|| anyhow!("unable to resolve data directory"))?;
    ensure_webview_data_dir(project_dirs.data_local_dir())
}

// moved to infra::sqlite::schema

// moved to infra::import

#[derive(Clone, Debug, Default)]
struct HoldingDerived {
    buy_price: f64,
    market_price: f64,
    quantity: f64,
    estimated_dividend: f64,
}

fn parse_f64(value: &str) -> f64 {
    value.trim().replace(',', "").parse::<f64>().unwrap_or(0.0)
}

fn format_f64(value: f64) -> String {
    if !value.is_finite() {
        return String::new();
    }
    if (value.fract()).abs() < f64::EPSILON {
        format!("{}", value as i64)
    } else {
        let mut text = format!("{value:.6}");
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    }
}

fn format_number_with_commas(value: f64, decimals: usize) -> String {
    if !value.is_finite() {
        return String::new();
    }

    let sign = if value < 0.0 { "-" } else { "" };
    let abs = value.abs();
    let raw = format!("{:.*}", decimals, abs);
    let (int_part, frac_part) = raw.split_once('.').unwrap_or((&raw, ""));
    let mut int_with_commas = String::new();
    for (idx, ch) in int_part.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            int_with_commas.push(',');
        }
        int_with_commas.push(ch);
    }
    let int_with_commas: String = int_with_commas.chars().rev().collect();
    if decimals == 0 {
        format!("{sign}{int_with_commas}")
    } else {
        format!("{sign}{int_with_commas}.{frac_part}")
    }
}

#[derive(Clone, Copy)]
enum NumericFormat {
    Integer,
    TwoDecimals,
    Percent,
}

fn is_text_header(header: &str) -> bool {
    matches!(
        header,
        "名稱"
            | "類別"
            | "性質"
            | "國內 /國外"
            | "代號"
            | "資產形式"
            | "所有權人"
            | "往來機構"
            | "帳號"
            | "幣別"
            | "配息方式"
    )
}

fn numeric_format_for_header(header: &str) -> NumericFormat {
    if matches!(header, "買進" | "市價" | "買入價") {
        NumericFormat::TwoDecimals
    } else if matches!(
        header,
        "損益率" | "報酬率" | "估計殖利率" | "最新殖利率" | "差異" | "殖利率" | "累計殖利率"
    ) {
        NumericFormat::Percent
    } else {
        NumericFormat::Integer
    }
}

fn parse_numeric_value(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (number_text, is_percent) = if trimmed.ends_with('%') {
        (trimmed.trim_end_matches('%'), true)
    } else {
        (trimmed, false)
    };
    let cleaned = number_text.replace(',', "");
    let parsed = cleaned.parse::<f64>().ok()?;
    if is_percent {
        Some(parsed / 100.0)
    } else {
        Some(parsed)
    }
}

fn format_cell_value(header: &str, raw: &str) -> String {
    if is_text_header(header) {
        return raw.to_string();
    }
    let Some(value) = parse_numeric_value(raw) else {
        return raw.to_string();
    };
    match numeric_format_for_header(header) {
        NumericFormat::Percent => format!("{}%", format_number_with_commas(value * 100.0, 2)),
        NumericFormat::TwoDecimals => format_number_with_commas(value, 2),
        NumericFormat::Integer => format_number_with_commas(value, 0),
    }
}

fn column_alignment(header: &str, rows: &[Vec<String>], column_idx: usize) -> &'static str {
    if is_text_header(header) {
        return "left";
    }
    let is_numeric = rows.iter().any(|row| {
        row.get(column_idx)
            .and_then(|value| parse_numeric_value(value))
            .is_some()
    });
    if is_numeric {
        "right"
    } else {
        "left"
    }
}

fn safe_div(numerator: f64, denominator: f64) -> f64 {
    if denominator.abs() < f64::EPSILON {
        0.0
    } else {
        numerator / denominator
    }
}

fn format_ratio_or_na(numerator: f64, denominator: f64) -> String {
    if denominator.abs() < f64::EPSILON {
        "N/A".to_string()
    } else {
        format_f64(numerator / denominator)
    }
}

fn parse_frequency(text: &str) -> f64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0.0;
    }
    if trimmed.contains('年') {
        return 1.0;
    }
    if trimmed.contains("半年") {
        return 2.0;
    }
    if trimmed.contains('季') {
        return 4.0;
    }
    if trimmed.contains('月') {
        return 12.0;
    }
    let count = trimmed
        .split(['、', ',', '，', '/', ' '])
        .filter(|item| !item.trim().is_empty())
        .count();
    if count > 0 {
        count as f64
    } else {
        parse_f64(trimmed)
    }
}

fn is_summary_label(value: &str) -> bool {
    ["小計", "合計", "總計", "加總", "平均"]
        .iter()
        .any(|token| value.contains(token))
}

fn row_value(row: &[String], idx: usize) -> String {
    row.get(idx).cloned().unwrap_or_default()
}

fn transform_holdings_sheet(rows: &[Vec<String>]) -> HoldingsTransform {
    let headers = vec![
        "名稱".to_string(),
        "類別".to_string(),
        "性質".to_string(),
        "國內 /國外".to_string(),
        "代號".to_string(),
        "買進".to_string(),
        "市價".to_string(),
        "數量".to_string(),
        "年配息".to_string(),
        "配息頻率".to_string(),
        "最新配息".to_string(),
        "總成本".to_string(),
        "資本利得".to_string(),
        "損益率".to_string(),
        "淨值".to_string(),
        "已收配息".to_string(),
        "總損益".to_string(),
        "報酬率".to_string(),
        "估計配息".to_string(),
        "估計殖利率".to_string(),
        "最新殖利率".to_string(),
        "最新領息".to_string(),
        "差異".to_string(),
        "股票成本".to_string(),
        "股票淨值".to_string(),
        "債券成本".to_string(),
        "債券淨值".to_string(),
        "最新股息".to_string(),
        "最新債息".to_string(),
    ];

    let mut output = Vec::new();
    let mut by_code = HashMap::new();
    let mut total_cost_sum = 0.0;
    let mut total_net_sum = 0.0;

    for row in rows {
        let name = row_value(row, 1);
        if name.trim().is_empty() || is_summary_label(&name) {
            continue;
        }
        let category = row_value(row, 2);
        let asset_kind = row_value(row, 3);
        let market = row_value(row, 4);
        let code = row_value(row, 5);
        let buy = parse_f64(&row_value(row, 6));
        let price = parse_f64(&row_value(row, 7));
        let qty = parse_f64(&row_value(row, 8));
        let annual_dividend = parse_f64(&row_value(row, 18));
        let freq = parse_frequency(&row_value(row, 21));
        let latest_dividend = parse_f64(&row_value(row, 22));

        let total_cost = buy * qty;
        let capital_gain = (price - buy) * qty;
        let net_value = total_cost + capital_gain;
        let received_dividend = 0.0;
        let total_gain = capital_gain + received_dividend;
        let estimated_dividend = annual_dividend * qty;
        let estimated_yield = safe_div(estimated_dividend, total_cost);
        let latest_yield = safe_div(latest_dividend * freq, price);
        let latest_income = latest_dividend * freq * qty;
        let diff = latest_yield - estimated_yield;

        let is_stock = asset_kind.contains('股');
        let is_bond = asset_kind.contains('債');

        total_cost_sum += total_cost;
        total_net_sum += net_value;

        by_code.insert(
            code.clone(),
            HoldingDerived {
                buy_price: buy,
                market_price: price,
                quantity: qty,
                estimated_dividend,
            },
        );

        output.push(vec![
            name,
            category,
            asset_kind,
            market,
            code,
            format_f64(buy),
            format_f64(price),
            format_f64(qty),
            format_f64(annual_dividend),
            format_f64(freq),
            format_f64(latest_dividend),
            format_f64(total_cost),
            format_f64(capital_gain),
            format_ratio_or_na(capital_gain, total_cost),
            format_f64(net_value),
            format_f64(received_dividend),
            format_f64(total_gain),
            format_ratio_or_na(total_gain, total_cost),
            format_f64(estimated_dividend),
            format_ratio_or_na(estimated_dividend, total_cost),
            format_ratio_or_na(latest_dividend * freq, price),
            format_f64(latest_income),
            format_f64(diff),
            format_f64(if is_stock { total_cost } else { 0.0 }),
            format_f64(if is_stock { net_value } else { 0.0 }),
            format_f64(if is_bond { total_cost } else { 0.0 }),
            format_f64(if is_bond { net_value } else { 0.0 }),
            format_f64(if is_stock { latest_income } else { 0.0 }),
            format_f64(if is_bond { latest_income } else { 0.0 }),
        ]);
    }

    HoldingsTransform {
        headers,
        rows: output,
        by_code,
        total_cost: total_cost_sum,
        total_net: total_net_sum,
    }
}

fn transform_assets_sheet(
    rows: &[Vec<String>],
    holdings_total_cost: f64,
    holdings_total_net: f64,
) -> (Vec<String>, Vec<Vec<String>>) {
    let headers = vec![
        "資產形式".to_string(),
        "所有權人".to_string(),
        "往來機構".to_string(),
        "帳號".to_string(),
        "幣別".to_string(),
        "餘額".to_string(),
        "交割款".to_string(),
    ];

    let mut output = Vec::new();
    for row in rows {
        let asset_form = row_value(row, 0);
        if asset_form.trim().is_empty()
            || is_summary_label(&asset_form)
            || asset_form.trim() == "交割款"
        {
            continue;
        }
        let owner = row_value(row, 1);
        let institution = row_value(row, 2);
        let account = row_value(row, 3);
        let currency = row_value(row, 4);
        if owner.trim().is_empty()
            || institution.trim().is_empty()
            || account.trim().is_empty()
            || currency.trim().is_empty()
        {
            continue;
        }
        let balance_raw = row_value(row, 5);
        let Some(balance_value) = parse_numeric_value(&balance_raw) else {
            continue;
        };
        let mut cost = balance_value;
        let is_investment = asset_form.contains("投資") || asset_form.contains("股票");
        if is_investment {
            cost = holdings_total_cost;
        }
        let balance = if is_investment {
            holdings_total_net
        } else {
            cost
        };
        let settlement = String::new();

        output.push(vec![
            asset_form,
            owner,
            institution,
            account,
            currency,
            format_f64(balance),
            settlement,
        ]);
    }

    (headers, output)
}

fn transform_dividend_sheet(
    rows: &[Vec<String>],
    by_code: &HashMap<String, HoldingDerived>,
) -> (Vec<String>, Vec<Vec<String>>) {
    let headers = vec![
        "名稱".to_string(),
        "性質".to_string(),
        "代號".to_string(),
        "所有權人".to_string(),
        "配息方式".to_string(),
        "期數".to_string(),
        "2023年".to_string(),
        "去年度累積".to_string(),
        "1月".to_string(),
        "2月".to_string(),
        "3月".to_string(),
        "4月".to_string(),
        "5月".to_string(),
        "6月".to_string(),
        "7月".to_string(),
        "8月".to_string(),
        "9月".to_string(),
        "10月".to_string(),
        "11月".to_string(),
        "12月".to_string(),
        "買入價".to_string(),
        "市價".to_string(),
        "股數".to_string(),
        "原始投入金額".to_string(),
        "債".to_string(),
        "股".to_string(),
        "估計配息金額".to_string(),
        "殖利率".to_string(),
        "2024年".to_string(),
        "今年度累積".to_string(),
        "總累積".to_string(),
        "預估累積".to_string(),
        "預算實際差異".to_string(),
        "累計殖利率".to_string(),
    ];

    let mut output = Vec::new();
    for row in rows {
        let name = row_value(row, 0);
        if name.trim().is_empty() || is_summary_label(&name) {
            continue;
        }
        let asset_kind = row_value(row, 1);
        let code = row_value(row, 2);
        let owner = row_value(row, 9);
        let payout_method = row_value(row, 10);
        let periods = parse_f64(&row_value(row, 11));
        let y2023 = parse_f64(&row_value(row, 14));
        let prev_total = parse_f64(&row_value(row, 16));

        let mut months = Vec::new();
        for idx in 22..34 {
            months.push(parse_f64(&row_value(row, idx)));
        }
        let current_total: f64 = months.iter().sum();

        let hold = by_code.get(&code).cloned().unwrap_or_default();
        let principal = hold.buy_price * hold.quantity;
        let debt = if asset_kind.contains('債') {
            principal
        } else {
            0.0
        };
        let stock = if asset_kind.contains('股') {
            principal
        } else {
            0.0
        };
        let estimated = hold.estimated_dividend;
        let y2024 = prev_total - y2023;
        let total = prev_total + current_total;
        let expected = estimated;
        let variance = current_total - expected;

        let mut result = vec![
            name,
            asset_kind,
            code,
            owner,
            payout_method,
            format_f64(periods),
            format_f64(y2023),
            format_f64(prev_total),
        ];
        for month in months {
            result.push(format_f64(month));
        }
        result.extend_from_slice(&[
            format_f64(hold.buy_price),
            format_f64(hold.market_price),
            format_f64(hold.quantity),
            format_f64(principal),
            format_f64(debt),
            format_f64(stock),
            format_f64(estimated),
            format_ratio_or_na(estimated, principal),
            format_f64(y2024),
            format_f64(current_total),
            format_f64(total),
            format_f64(expected),
            format_f64(variance),
            format_ratio_or_na(total, principal),
        ]);

        output.push(result);
    }

    (headers, output)
}

fn merge_holdings_and_dividends(
    holdings_headers: Vec<String>,
    holdings_rows: Vec<Vec<String>>,
    dividend_rows: &[Vec<String>],
) -> (Vec<String>, Vec<Vec<String>>) {
    let mut merged_headers = holdings_headers;
    merged_headers.extend_from_slice(&[
        "所有權人".to_string(),
        "配息方式".to_string(),
        "期數".to_string(),
        "2023年".to_string(),
        "去年度累積".to_string(),
        "1月".to_string(),
        "2月".to_string(),
        "3月".to_string(),
        "4月".to_string(),
        "5月".to_string(),
        "6月".to_string(),
        "7月".to_string(),
        "8月".to_string(),
        "9月".to_string(),
        "10月".to_string(),
        "11月".to_string(),
        "12月".to_string(),
        "2024年".to_string(),
        "今年度累積".to_string(),
        "總累積".to_string(),
        "預估累積".to_string(),
        "預算實際差異".to_string(),
        "累計殖利率".to_string(),
    ]);

    let mut dividend_by_code: HashMap<String, Vec<String>> = HashMap::new();
    for row in dividend_rows {
        let code = row_value(row, 2);
        if code.trim().is_empty() {
            continue;
        }
        let values = vec![
            row_value(row, 3),
            row_value(row, 4),
            row_value(row, 5),
            row_value(row, 6),
            row_value(row, 7),
            row_value(row, 8),
            row_value(row, 9),
            row_value(row, 10),
            row_value(row, 11),
            row_value(row, 12),
            row_value(row, 13),
            row_value(row, 14),
            row_value(row, 15),
            row_value(row, 16),
            row_value(row, 17),
            row_value(row, 18),
            row_value(row, 19),
            row_value(row, 28),
            row_value(row, 29),
            row_value(row, 30),
            row_value(row, 31),
            row_value(row, 32),
            row_value(row, 33),
        ];
        dividend_by_code.entry(code).or_insert(values);
    }

    let mut merged_rows = Vec::new();
    for mut row in holdings_rows {
        let code = row_value(&row, 4);
        if let Some(div) = dividend_by_code.get(&code) {
            row.extend(div.clone());
        } else {
            row.extend(std::iter::repeat_n(String::new(), 23));
        }
        merged_rows.push(row);
    }

    let preferred_order = [
        "所有權人",
        "名稱",
        "類別",
        "性質",
        "國內 /國外",
        "代號",
        "買進",
        "市價",
        "數量",
        "配息方式",
        "期數",
    ];
    reorder_headers_and_rows(&merged_headers, &merged_rows, &preferred_order)
}

fn reorder_headers_and_rows(
    headers: &[String],
    rows: &[Vec<String>],
    preferred_order: &[&str],
) -> (Vec<String>, Vec<Vec<String>>) {
    let mut indices = Vec::new();
    let mut used = vec![false; headers.len()];

    for &name in preferred_order {
        if let Some((idx, _)) = headers
            .iter()
            .enumerate()
            .find(|(_, header)| header.as_str() == name)
        {
            indices.push(idx);
            used[idx] = true;
        }
    }

    for (idx, _) in headers.iter().enumerate() {
        if !used[idx] {
            indices.push(idx);
        }
    }

    let new_headers = indices.iter().map(|&idx| headers[idx].clone()).collect();
    let mut new_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut reordered = Vec::with_capacity(indices.len());
        for &idx in &indices {
            reordered.push(row.get(idx).cloned().unwrap_or_default());
        }
        new_rows.push(reordered);
    }

    (new_headers, new_rows)
}

fn required_columns_for_holdings() -> Vec<String> {
    vec![
        "所有權人".to_string(),
        "名稱".to_string(),
        "類別".to_string(),
        "性質".to_string(),
        "國內 /國外".to_string(),
        "代號".to_string(),
        "買進".to_string(),
        "市價".to_string(),
        "數量".to_string(),
        "配息方式".to_string(),
        "期數".to_string(),
    ]
}

fn is_holdings_table(headers: &[String]) -> bool {
    let required = required_columns_for_holdings();
    required.iter().all(|col| headers.iter().any(|h| h == col))
}

fn editable_columns_for_holdings() -> Vec<String> {
    required_columns_for_holdings()
}

fn default_dataset_name_mmdd() -> String {
    let now = chrono::Local::now();
    now.format("%m%d").to_string()
}

// moved to domain::entities::edit::CellKey

#[derive(Clone)]
struct CellRender {
    row_idx: usize,
    col_idx: usize,
    header: String,
    raw: String,
    formatted: String,
    is_editing: bool,
    is_editable: bool,
    style: String,
}

#[derive(Clone)]
struct RowRender {
    row_idx: usize,
    is_deleted: bool,
    style: String,
    cells: Vec<CellRender>,
}

#[derive(Clone)]
enum PendingAction {
    Import(PathBuf),
    DatasetChange {
        next_group: Option<String>,
        next_dataset: Option<i64>,
    },
    TabSwitch {
        dataset_id: i64,
    },
}

struct HoldingsTransform {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    by_code: HashMap<String, HoldingDerived>,
    total_cost: f64,
    total_net: f64,
}

fn validate_required_holdings_row(headers: &[String], row: &[String]) -> Result<(), String> {
    for required in required_columns_for_holdings() {
        let Some(idx) = headers.iter().position(|h| h == &required) else {
            return Err(format!("missing header: {required}"));
        };
        let value = row.get(idx).map(|v| v.trim()).unwrap_or("");
        if value.is_empty() {
            return Err(format!("required field empty: {required}"));
        }

        let numeric_required = matches!(required.as_str(), "買進" | "市價" | "數量" | "期數");
        if numeric_required && parse_numeric_value(value).is_none() {
            return Err(format!("invalid number: {required}"));
        }
    }

    Ok(())
}

// moved to infra::import

// moved to infra::sqlite::queries

// moved to infra::sqlite::queries

// moved to infra::sqlite::queries

// moved to infra::sqlite::queries

#[cfg(test)]
mod tests;

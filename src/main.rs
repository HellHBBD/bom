use dioxus::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use csv::StringRecord;
use directories::ProjectDirs;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};
use rusqlite::{params, types::Value, Connection};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

const PAGE_SIZE: i64 = i64::MAX;
const NONE_OPTION_VALUE: &str = "__none__";

type QueryPageResult = (Vec<String>, Vec<Vec<String>>, i64);
type ReloadPageResult = (Vec<String>, Vec<Vec<String>>, i64, i64);

fn main() {
    let webview_data_dir =
        default_webview_data_dir().expect("should resolve and create WebView2 data directory");

    dioxus::LaunchBuilder::desktop()
        .with_cfg(
            dioxus::desktop::Config::new()
                .with_window(dioxus::desktop::WindowBuilder::new().with_title("BOM"))
                .with_data_directory(webview_data_dir),
        )
        .launch(App);
}

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

    let mut datasets = use_signal(Vec::<DatasetSummary>::new);
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

    let db_path = Arc::new(db_path);
    let db_path_for_init = db_path.clone();
    use_effect(move || {
        *busy.write() = true;
        match init_db(&db_path_for_init).and_then(|_| list_datasets(&db_path_for_init, false)) {
            Ok(available) => {
                let groups = build_dataset_groups(&available);
                let first_dataset = groups
                    .first()
                    .and_then(|g| g.datasets.first())
                    .map(|dataset| dataset.id);
                *datasets.write() = available;
                *selected_group_key.write() = groups.first().map(|g| g.key.clone());
                *selected_dataset_id.write() = first_dataset;
                *page.write() = 0;

                match reload_page_data(
                    &db_path_for_init,
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

    let db_path_for_import = db_path.clone();
    let db_path_for_dataset_change = db_path.clone();
    let db_path_for_global_search = db_path.clone();
    let db_path_for_column_select = db_path.clone();
    let db_path_for_column_search = db_path.clone();
    let db_path_for_sort_select = db_path.clone();
    let db_path_for_sort_toggle = db_path.clone();
    let db_path_for_tab_switch = db_path.clone();
    let db_path_for_show_deleted = db_path.clone();
    let db_path_for_soft_delete = db_path.clone();
    let db_path_for_purge = db_path.clone();
    let grouped_datasets = build_dataset_groups(&datasets());
    let active_group =
        selected_group_key().and_then(|k| grouped_datasets.iter().find(|g| g.key == k).cloned());
    let current_columns = columns();
    let current_rows = rows();
    let added_rows_snapshot = added_rows();
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
        let style = format!(
            "{}{}",
            if selected { "background: #eef4ff;" } else { "" },
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

                        *busy.write() = true;
                        *status.write() = format!("正在匯入 {}", file_path.display());

                        let ext = file_path
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(|s| s.to_ascii_lowercase())
                            .unwrap_or_default();

                        let import_result = if ext == "xlsx" {
                            import_xlsx_selected_sheets_to_sqlite(&db_path_for_import, &file_path)
                                .map(|items| (items.first().map(|it| it.dataset_id), items.len() as i64, true))
                        } else {
                            import_csv_to_sqlite(&db_path_for_import, &file_path)
                                .map(|item| (Some(item.dataset_id), item.row_count, false))
                        };

                        match import_result {
                            Ok((selected_id, imported_count, is_xlsx)) => match list_datasets(&db_path_for_import, show_deleted()) {
                                Ok(available) => {
                                    let groups = build_dataset_groups(&available);
                                    *datasets.write() = available;
                                    let next_group_key = selected_id.and_then(|id| {
                                        groups
                                            .iter()
                                            .find(|g| g.datasets.iter().any(|d| d.id == id))
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

                                    match reload_page_data(&db_path_for_import, selected_id, 0, &options) {
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

                            match list_datasets(&db_path_for_show_deleted, checked) {
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
                                        .map(|d| d.id);

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

                                    match reload_page_data(&db_path_for_show_deleted, next_dataset, 0, &options) {
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
                    onchange: move |event| {
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
                            .map(|d| d.id);

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

                        match reload_page_data(&db_path_for_dataset_change, next_dataset, 0, &options) {
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
                        match soft_delete_dataset(&db_path_for_soft_delete, dataset_id)
                            .and_then(|_| list_datasets(&db_path_for_soft_delete, show_deleted()))
                        {
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
                                    .map(|d| d.id);

                                *selected_group_key.write() = next_group;
                                *selected_dataset_id.write() = next_dataset;

                                let options = QueryOptions {
                                    global_search: global_search(),
                                    column_search_col: column_search_col(),
                                    column_search_text: column_search_text(),
                                    sort_col: sort_col(),
                                    sort_desc: sort_desc(),
                                };

                                match reload_page_data(&db_path_for_soft_delete, next_dataset, 0, &options) {
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
                        match purge_dataset(&db_path_for_purge, dataset_id)
                            .and_then(|_| list_datasets(&db_path_for_purge, show_deleted()))
                        {
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
                                    .map(|d| d.id);

                                *selected_group_key.write() = next_group;
                                *selected_dataset_id.write() = next_dataset;

                                let options = QueryOptions {
                                    global_search: global_search(),
                                    column_search_col: column_search_col(),
                                    column_search_text: column_search_text(),
                                    sort_col: sort_col(),
                                    sort_desc: sort_desc(),
                                };

                                match reload_page_data(&db_path_for_purge, next_dataset, 0, &options) {
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
                                    let db_path_for_tab = db_path_for_tab_switch.clone();
                                    move |_| {
                                        *selected_dataset_id.write() = Some(sheet.id);
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

                                        match reload_page_data(&db_path_for_tab, Some(sheet.id), 0, &options) {
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
                                if Some(sheet.id) == selected_dataset_id() {
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

                        match reload_page_data(&db_path_for_global_search, selected_dataset_id(), 0, &options) {
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

                        match reload_page_data(&db_path_for_column_select, selected_dataset_id(), 0, &options) {
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

                        match reload_page_data(&db_path_for_column_search, selected_dataset_id(), 0, &options) {
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

                        match reload_page_data(&db_path_for_sort_select, selected_dataset_id(), 0, &options) {
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

                        match reload_page_data(&db_path_for_sort_toggle, selected_dataset_id(), 0, &options) {
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
                                        selected.remove(&row.row_idx);
                                    } else {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatasetSummary {
    id: i64,
    name: String,
    row_count: i64,
    source_path: String,
    deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatasetGroup {
    key: String,
    label: String,
    datasets: Vec<DatasetSummary>,
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

fn build_dataset_groups(list: &[DatasetSummary]) -> Vec<DatasetGroup> {
    let mut grouped: BTreeMap<String, DatasetGroup> = BTreeMap::new();
    for item in list {
        let key = dataset_group_key(&item.source_path, item.id);
        let label = dataset_group_label(&item.source_path, &item.name, item.id);
        let entry = grouped.entry(key.clone()).or_insert_with(|| DatasetGroup {
            key,
            label,
            datasets: Vec::new(),
        });
        entry.datasets.push(item.clone());
    }

    let mut groups: Vec<DatasetGroup> = grouped.into_values().collect();
    for group in &mut groups {
        group.datasets.sort_by_key(|d| d.id);
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
fn open_connection(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open db: {}", db_path.display()))?;
    conn.execute("PRAGMA foreign_keys = ON", [])
        .context("failed to enable foreign key enforcement")?;
    Ok(conn)
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

#[allow(dead_code)]
fn init_db(db_path: &Path) -> Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir: {}", parent.display()))?;
    }

    let conn = open_connection(db_path)?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS dataset (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            source_path TEXT NOT NULL,
            row_count   INTEGER NOT NULL,
            deleted_at  TEXT,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS column_name (
            dataset_id  INTEGER NOT NULL,
            col_idx     INTEGER NOT NULL,
            name        TEXT NOT NULL,
            PRIMARY KEY (dataset_id, col_idx),
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE TABLE IF NOT EXISTS cell (
            dataset_id  INTEGER NOT NULL,
            row_idx     INTEGER NOT NULL,
            col_idx     INTEGER NOT NULL,
            value       TEXT NOT NULL,
            PRIMARY KEY (dataset_id, row_idx, col_idx),
            FOREIGN KEY (dataset_id) REFERENCES dataset(id)
        );

        CREATE INDEX IF NOT EXISTS idx_cell_dataset_row
            ON cell(dataset_id, row_idx);

        CREATE INDEX IF NOT EXISTS idx_cell_dataset_col_value
            ON cell(dataset_id, col_idx, value);
        ",
    )
    .context("failed to initialize schema")?;

    conn.execute("ALTER TABLE dataset ADD COLUMN deleted_at TEXT", [])
        .ok();

    Ok(())
}

#[allow(dead_code)]
fn import_csv_to_sqlite(db_path: &Path, csv_path: &Path) -> Result<ImportResult> {
    init_db(db_path)?;

    let mut reader = csv::Reader::from_path(csv_path)
        .with_context(|| format!("failed to open csv: {}", csv_path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("failed to read headers from csv: {}", csv_path.display()))?
        .clone();

    if headers.is_empty() {
        anyhow::bail!("csv header is required")
    }

    let source_path = csv_path.to_string_lossy().into_owned();
    let dataset_name = csv_path
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("dataset")
        .to_string();

    let mut conn = open_connection(db_path)?;
    let tx = conn.transaction().context("failed to start transaction")?;

    tx.execute(
        "INSERT INTO dataset(name, source_path, row_count) VALUES (?1, ?2, 0)",
        params![dataset_name, source_path],
    )
    .context("failed to insert dataset")?;
    let dataset_id = tx.last_insert_rowid();

    insert_headers(&tx, dataset_id, &headers)?;

    let mut insert_cell = tx
        .prepare("INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)")
        .context("failed to prepare cell insert")?;

    let mut row_count = 0_i64;
    let header_len = headers.len();
    for (row_idx, record) in reader.records().enumerate() {
        let record = record.context("failed to parse csv record")?;
        for col_idx in 0..header_len {
            let value = record.get(col_idx).unwrap_or("");
            insert_cell
                .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                .context("failed to insert cell")?;
        }
        row_count += 1;
    }
    drop(insert_cell);

    tx.execute(
        "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
        params![row_count, dataset_id],
    )
    .context("failed to update dataset row_count")?;

    tx.commit().context("failed to commit import transaction")?;

    Ok(ImportResult {
        dataset_id,
        row_count,
    })
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::String(v) => v.to_string(),
        Data::Float(v) => v.to_string(),
        Data::Int(v) => v.to_string(),
        Data::Bool(v) => v.to_string(),
        Data::DateTime(v) => v.to_string(),
        Data::DateTimeIso(v) => v.to_string(),
        Data::DurationIso(v) => v.to_string(),
        Data::Error(v) => format!("{v:?}"),
        Data::Empty => String::new(),
    }
}

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

fn transform_holdings_sheet(
    rows: &[Vec<String>],
) -> (
    Vec<String>,
    Vec<Vec<String>>,
    HashMap<String, HoldingDerived>,
    f64,
    f64,
) {
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

    (headers, output, by_code, total_cost_sum, total_net_sum)
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
    let mut editable = Vec::new();
    for column in required_columns_for_holdings() {
        if !editable.contains(&column) {
            editable.push(column);
        }
    }

    let additional = [
        "2023年",
        "去年度累積",
        "1月",
        "2月",
        "3月",
        "4月",
        "5月",
        "6月",
        "7月",
        "8月",
        "9月",
        "10月",
        "11月",
        "12月",
    ];
    for column in additional {
        let value = column.to_string();
        if !editable.contains(&value) {
            editable.push(value);
        }
    }

    editable
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CellKey {
    row_idx: usize,
    col_idx: usize,
    column: String,
}

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

struct EditingState {
    editable_columns: Vec<String>,
    staged: HashMap<CellKey, String>,
    active: Option<CellKey>,
}

impl EditingState {
    fn new_for_test() -> Self {
        Self {
            editable_columns: editable_columns_for_holdings(),
            staged: HashMap::new(),
            active: None,
        }
    }

    fn cell_id(&self, column: &str, row_idx: usize, col_idx: usize) -> CellKey {
        CellKey {
            row_idx,
            col_idx,
            column: column.to_string(),
        }
    }

    fn start_edit(&mut self, cell: CellKey) -> bool {
        if self.editable_columns.contains(&cell.column) {
            self.active = Some(cell);
            true
        } else {
            false
        }
    }

    fn apply_edit(&mut self, cell: CellKey, value: &str) {
        if self.editable_columns.contains(&cell.column) {
            self.staged.insert(cell, value.to_string());
            self.active = None;
        }
    }

    fn staged_value(&self, cell: CellKey) -> String {
        self.staged.get(&cell).cloned().unwrap_or_default()
    }
}

struct EditTableState {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    selected_rows: std::collections::BTreeSet<usize>,
    deleted_rows: std::collections::BTreeSet<usize>,
    staged_cells: HashMap<CellKey, String>,
}

impl EditTableState {
    fn new_for_test() -> Self {
        let mut headers = required_columns_for_holdings();
        headers.push("其他".to_string());
        let mut state = Self {
            headers,
            rows: Vec::new(),
            selected_rows: std::collections::BTreeSet::new(),
            deleted_rows: std::collections::BTreeSet::new(),
            staged_cells: HashMap::new(),
        };
        let mut row = state.empty_row();
        if let Some(idx) = state.header_index("所有權人") {
            row[idx] = "王小明".to_string();
        }
        if let Some(idx) = state.header_index("名稱") {
            row[idx] = "測試".to_string();
        }
        if let Some(idx) = state.header_index("類別") {
            row[idx] = "ETF".to_string();
        }
        if let Some(idx) = state.header_index("性質") {
            row[idx] = "股票".to_string();
        }
        if let Some(idx) = state.header_index("國內 /國外") {
            row[idx] = "國內".to_string();
        }
        if let Some(idx) = state.header_index("代號") {
            row[idx] = "0050".to_string();
        }
        if let Some(idx) = state.header_index("買進") {
            row[idx] = "100".to_string();
        }
        if let Some(idx) = state.header_index("市價") {
            row[idx] = "105".to_string();
        }
        if let Some(idx) = state.header_index("數量") {
            row[idx] = "10".to_string();
        }
        if let Some(idx) = state.header_index("配息方式") {
            row[idx] = "現金".to_string();
        }
        if let Some(idx) = state.header_index("期數") {
            row[idx] = "4".to_string();
        }
        state.rows.push(row);
        state
    }

    fn header_index(&self, name: &str) -> Option<usize> {
        self.headers.iter().position(|h| h == name)
    }

    fn empty_row(&self) -> Vec<String> {
        vec![String::new(); self.headers.len()]
    }

    fn add_row(&mut self, row: Vec<String>) -> Result<(), String> {
        validate_required_holdings_row(&self.headers, &row)?;
        self.rows.push(row);
        Ok(())
    }

    fn select_rows(&mut self, rows: Vec<usize>) {
        self.selected_rows = rows.into_iter().collect();
    }

    fn delete_selected(&mut self) {
        for row in &self.selected_rows {
            self.deleted_rows.insert(*row);
        }
    }

    fn is_deleted(&self, row_idx: usize) -> bool {
        self.deleted_rows.contains(&row_idx)
    }

    fn apply_edit(&mut self, cell: CellKey, value: &str) {
        self.staged_cells.insert(cell, value.to_string());
    }

    fn is_cell_modified(&self, column: &str, row_idx: usize) -> bool {
        let Some(col_idx) = self.header_index(column) else {
            return false;
        };
        self.staged_cells.contains_key(&CellKey {
            row_idx,
            col_idx,
            column: column.to_string(),
        })
    }
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

fn import_xlsx_selected_sheets_to_sqlite(
    db_path: &Path,
    xlsx_path: &Path,
) -> Result<Vec<ImportResult>> {
    init_db(db_path)?;

    let mut workbook = open_workbook_auto(xlsx_path)
        .with_context(|| format!("failed to open xlsx: {}", xlsx_path.display()))?;
    let source_path = xlsx_path.to_string_lossy().into_owned();

    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start xlsx import transaction")?;

    let assets_range = workbook
        .worksheet_range("資產總表")
        .context("failed to read sheet: 資產總表")?;
    let holdings_range = workbook
        .worksheet_range("持股明細")
        .context("failed to read sheet: 持股明細")?;
    let dividends_range = workbook
        .worksheet_range("股息收入明細表")
        .context("failed to read sheet: 股息收入明細表")?;

    let assets_rows: Vec<Vec<String>> = assets_range
        .rows()
        .skip(3)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();
    let holdings_rows: Vec<Vec<String>> = holdings_range
        .rows()
        .skip(2)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();
    let dividends_rows: Vec<Vec<String>> = dividends_range
        .rows()
        .skip(1)
        .map(|r| r.iter().map(cell_to_string).collect())
        .collect();

    let (holdings_headers, holdings_data, by_code, total_cost, total_net) =
        transform_holdings_sheet(&holdings_rows);
    let (assets_headers, assets_data) = transform_assets_sheet(&assets_rows, total_cost, total_net);
    let (_dividend_headers, dividend_data) = transform_dividend_sheet(&dividends_rows, &by_code);
    let (merged_headers, merged_data) =
        merge_holdings_and_dividends(holdings_headers, holdings_data, &dividend_data);

    let transformed = vec![
        ("資產總表", assets_headers, assets_data),
        ("持股股息合併表", merged_headers, merged_data),
    ];

    let mut imported = Vec::new();
    for (sheet_name, headers, rows) in transformed {
        tx.execute(
            "INSERT INTO dataset(name, source_path, row_count) VALUES (?1, ?2, 0)",
            params![sheet_name, format!("{source_path}#{sheet_name}")],
        )
        .with_context(|| format!("failed to insert dataset for sheet: {sheet_name}"))?;
        let dataset_id = tx.last_insert_rowid();

        insert_header_names(&tx, dataset_id, &headers)?;

        let mut insert_cell = tx
            .prepare(
                "INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)",
            )
            .context("failed to prepare xlsx cell insert")?;

        for (row_idx, row) in rows.iter().enumerate() {
            for (col_idx, value) in row.iter().enumerate() {
                insert_cell
                    .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                    .context("failed to insert transformed xlsx cell")?;
            }
        }
        drop(insert_cell);

        let row_count = rows.len() as i64;
        tx.execute(
            "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
            params![row_count, dataset_id],
        )
        .context("failed to update xlsx dataset row_count")?;

        imported.push(ImportResult {
            dataset_id,
            row_count,
        });
    }

    tx.commit()
        .context("failed to commit xlsx import transaction")?;

    Ok(imported)
}

#[allow(dead_code)]
fn insert_headers(
    tx: &rusqlite::Transaction<'_>,
    dataset_id: i64,
    headers: &StringRecord,
) -> Result<()> {
    let mut insert_header = tx
        .prepare("INSERT INTO column_name(dataset_id, col_idx, name) VALUES (?1, ?2, ?3)")
        .context("failed to prepare header insert")?;

    for (col_idx, name) in headers.iter().enumerate() {
        insert_header
            .execute(params![dataset_id, col_idx as i64, name])
            .context("failed to insert header")?;
    }

    Ok(())
}

fn insert_header_names(
    tx: &rusqlite::Transaction<'_>,
    dataset_id: i64,
    headers: &[String],
) -> Result<()> {
    let mut insert_header = tx
        .prepare("INSERT INTO column_name(dataset_id, col_idx, name) VALUES (?1, ?2, ?3)")
        .context("failed to prepare header insert")?;

    for (col_idx, name) in headers.iter().enumerate() {
        insert_header
            .execute(params![dataset_id, col_idx as i64, name])
            .context("failed to insert header")?;
    }

    Ok(())
}

#[allow(dead_code)]
fn query_page(
    db_path: &Path,
    dataset_id: i64,
    target_page: i64,
    page_size: i64,
    options: &QueryOptions,
) -> Result<QueryPageResult> {
    if page_size <= 0 {
        anyhow::bail!("page_size must be greater than zero")
    }

    let conn = open_connection(db_path)?;

    let mut columns_stmt = conn
        .prepare(
            "SELECT name
             FROM column_name
             WHERE dataset_id = ?1
             ORDER BY col_idx ASC",
        )
        .context("failed to prepare columns query")?;
    let columns = columns_stmt
        .query_map([dataset_id], |row| row.get::<_, String>(0))
        .context("failed to query columns")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect columns")?;
    drop(columns_stmt);

    if columns.is_empty() {
        return Ok((columns, Vec::new(), 0));
    }

    if let Some(column_search_col) = options.column_search_col {
        if column_search_col < 0 || column_search_col as usize >= columns.len() {
            anyhow::bail!(
                "column_search_col out of range: {column_search_col} (columns: {})",
                columns.len()
            );
        }
    }

    if let Some(sort_col) = options.sort_col {
        if sort_col < 0 || sort_col as usize >= columns.len() {
            anyhow::bail!(
                "sort_col out of range: {sort_col} (columns: {})",
                columns.len()
            );
        }
    }

    let mut filter_clauses = vec!["base.dataset_id = ?".to_string()];
    let mut filter_params = vec![Value::Integer(dataset_id)];

    let global_search = options.global_search.trim();
    if !global_search.is_empty() {
        filter_clauses.push(
            "EXISTS (
                SELECT 1 FROM cell gs
                WHERE gs.dataset_id = ?
                  AND gs.row_idx = base.row_idx
                  AND gs.value LIKE ?
            )"
            .to_string(),
        );
        filter_params.push(Value::Integer(dataset_id));
        filter_params.push(Value::Text(format!("%{global_search}%")));
    }

    let column_search_text = options.column_search_text.trim();
    if !column_search_text.is_empty() {
        if let Some(column_search_col) = options.column_search_col {
            filter_clauses.push(
                "EXISTS (
                    SELECT 1 FROM cell cs
                    WHERE cs.dataset_id = ?
                      AND cs.row_idx = base.row_idx
                      AND cs.col_idx = ?
                      AND cs.value LIKE ?
                )"
                .to_string(),
            );
            filter_params.push(Value::Integer(dataset_id));
            filter_params.push(Value::Integer(column_search_col));
            filter_params.push(Value::Text(format!("%{column_search_text}%")));
        }
    }

    let where_sql = filter_clauses.join(" AND ");

    let count_sql = format!(
        "SELECT COUNT(*)
         FROM (
             SELECT base.row_idx
             FROM cell base
             WHERE {where_sql}
             GROUP BY base.row_idx
         ) filtered"
    );
    let total_rows: i64 = conn
        .query_row(
            &count_sql,
            rusqlite::params_from_iter(filter_params.iter().cloned()),
            |row| row.get(0),
        )
        .context("failed to query filtered row count")?;

    let offset = target_page.max(0) * page_size;
    let sort_direction = if options.sort_desc { "DESC" } else { "ASC" };

    let mut row_params = Vec::<Value>::new();
    let mut row_sql = String::from("SELECT base.row_idx FROM cell base ");
    if let Some(sort_col) = options.sort_col {
        row_sql.push_str(
            "LEFT JOIN cell sort_cell
             ON sort_cell.dataset_id = base.dataset_id
            AND sort_cell.row_idx = base.row_idx
            AND sort_cell.col_idx = ? ",
        );
        row_params.push(Value::Integer(sort_col));
    }

    row_sql.push_str(&format!(
        "WHERE {where_sql} GROUP BY base.row_idx ORDER BY "
    ));
    if options.sort_col.is_some() {
        row_sql.push_str(&format!("COALESCE(sort_cell.value, '') {sort_direction}, "));
    }
    row_sql.push_str("base.row_idx ASC LIMIT ? OFFSET ?");

    row_params.extend(filter_params.iter().cloned());
    row_params.push(Value::Integer(page_size));
    row_params.push(Value::Integer(offset));

    let mut row_stmt = conn
        .prepare(&row_sql)
        .context("failed to prepare page row_idx query")?;
    let row_indices = row_stmt
        .query_map(rusqlite::params_from_iter(row_params), |row| {
            row.get::<_, i64>(0)
        })
        .context("failed to query page row_idx")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect page row_idx")?;
    drop(row_stmt);

    if row_indices.is_empty() {
        return Ok((columns, Vec::new(), total_rows));
    }

    let placeholders = std::iter::repeat_n("?", row_indices.len())
        .collect::<Vec<_>>()
        .join(",");
    let hydrate_sql = format!(
        "SELECT row_idx, col_idx, value
         FROM cell
         WHERE dataset_id = ? AND row_idx IN ({placeholders})
         ORDER BY row_idx ASC, col_idx ASC"
    );
    let mut hydrate_params = vec![Value::Integer(dataset_id)];
    hydrate_params.extend(row_indices.iter().copied().map(Value::Integer));

    let mut rows = vec![vec![String::new(); columns.len()]; row_indices.len()];
    let row_pos: HashMap<i64, usize> = row_indices
        .iter()
        .copied()
        .enumerate()
        .map(|(idx, row_idx)| (row_idx, idx))
        .collect();

    let mut hydrate_stmt = conn
        .prepare(&hydrate_sql)
        .context("failed to prepare row hydration query")?;
    let mut hydrate_rows = hydrate_stmt
        .query(rusqlite::params_from_iter(hydrate_params))
        .context("failed to run row hydration query")?;

    while let Some(row) = hydrate_rows.next().context("failed to read hydrated row")? {
        let row_idx: i64 = row.get(0).context("failed to read row_idx")?;
        let col_idx: i64 = row.get(1).context("failed to read col_idx")?;
        let value: String = row.get(2).context("failed to read value")?;

        if let Some(&dest_row_idx) = row_pos.get(&row_idx) {
            if let Some(dest_cell) = rows
                .get_mut(dest_row_idx)
                .and_then(|dest_row| dest_row.get_mut(col_idx as usize))
            {
                *dest_cell = value;
            }
        }
    }

    Ok((columns, rows, total_rows))
}

fn list_datasets(db_path: &Path, include_deleted: bool) -> Result<Vec<DatasetSummary>> {
    init_db(db_path)?;
    let conn = open_connection(db_path)?;
    let filter = if include_deleted {
        ""
    } else {
        "WHERE deleted_at IS NULL"
    };
    let mut stmt = conn
        .prepare(&format!(
            "SELECT id, name, row_count, source_path, deleted_at
             FROM dataset
             {filter}
             ORDER BY id DESC"
        ))
        .context("failed to prepare datasets query")?;

    let datasets = stmt
        .query_map([], |row| {
            Ok(DatasetSummary {
                id: row.get(0)?,
                name: row.get(1)?,
                row_count: row.get(2)?,
                source_path: row.get(3)?,
                deleted_at: row.get(4)?,
            })
        })
        .context("failed to query datasets")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed to collect datasets")?;

    Ok(datasets)
}

fn soft_delete_dataset(db_path: &Path, dataset_id: i64) -> Result<()> {
    init_db(db_path)?;
    let conn = open_connection(db_path)?;
    conn.execute(
        "UPDATE dataset SET deleted_at = datetime('now') WHERE id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to soft-delete dataset #{dataset_id}"))?;
    Ok(())
}

fn purge_dataset(db_path: &Path, dataset_id: i64) -> Result<()> {
    init_db(db_path)?;
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start purge transaction")?;
    tx.execute(
        "DELETE FROM cell WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to delete cells for dataset #{dataset_id}"))?;
    tx.execute(
        "DELETE FROM column_name WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .with_context(|| format!("failed to delete columns for dataset #{dataset_id}"))?;
    tx.execute("DELETE FROM dataset WHERE id = ?1", params![dataset_id])
        .with_context(|| format!("failed to delete dataset #{dataset_id}"))?;
    tx.commit().context("failed to commit purge transaction")?;
    Ok(())
}

fn reload_page_data(
    db_path: &Path,
    dataset_id: Option<i64>,
    target_page: i64,
    options: &QueryOptions,
) -> Result<ReloadPageResult> {
    let page = target_page.max(0);
    if let Some(dataset_id) = dataset_id {
        let (columns, rows, total_rows) =
            query_page(db_path, dataset_id, page, PAGE_SIZE, options)?;
        Ok((columns, rows, total_rows, page))
    } else {
        Ok((Vec::new(), Vec::new(), 0, 0))
    }
}

fn build_updated_rows(
    columns: &[String],
    rows: &[Vec<String>],
    staged_cells: &HashMap<CellKey, String>,
    deleted_rows: &BTreeSet<usize>,
    added_rows: &[Vec<String>],
) -> Vec<Vec<String>> {
    let mut updated = Vec::new();
    for (row_idx, row) in rows.iter().enumerate() {
        if deleted_rows.contains(&row_idx) {
            continue;
        }
        let mut next_row = row.clone();
        for (col_idx, header) in columns.iter().enumerate() {
            if let Some(value) = staged_cells.get(&CellKey {
                row_idx,
                col_idx,
                column: header.clone(),
            }) {
                if col_idx < next_row.len() {
                    next_row[col_idx] = value.clone();
                }
            }
        }
        updated.push(next_row);
    }
    for row in added_rows {
        updated.push(row.clone());
    }
    updated
}

fn apply_changes_to_dataset(
    db_path: &Path,
    dataset_id: i64,
    columns: &[String],
    rows: &[Vec<String>],
    staged_cells: &HashMap<CellKey, String>,
    deleted_rows: &BTreeSet<usize>,
    added_rows: &[Vec<String>],
) -> Result<()> {
    let updated_rows = build_updated_rows(columns, rows, staged_cells, deleted_rows, added_rows);
    let mut conn = open_connection(db_path)?;
    let tx = conn
        .transaction()
        .context("failed to start update transaction")?;

    tx.execute(
        "DELETE FROM cell WHERE dataset_id = ?1",
        params![dataset_id],
    )
    .context("failed to clear existing cells")?;

    let mut insert_cell = tx
        .prepare("INSERT INTO cell(dataset_id, row_idx, col_idx, value) VALUES (?1, ?2, ?3, ?4)")
        .context("failed to prepare cell insert")?;
    for (row_idx, row) in updated_rows.iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            insert_cell
                .execute(params![dataset_id, row_idx as i64, col_idx as i64, value])
                .context("failed to insert updated cell")?;
        }
    }
    drop(insert_cell);

    tx.execute(
        "UPDATE dataset SET row_count = ?1 WHERE id = ?2",
        params![updated_rows.len() as i64, dataset_id],
    )
    .context("failed to update dataset row_count")?;

    tx.commit().context("failed to commit dataset update")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("dioxus-{prefix}-{nanos}"))
    }

    #[test]
    fn init_db_creates_required_tables() {
        let temp_dir = unique_test_dir("init-db");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");

        let result = init_db(&db_path);

        assert!(result.is_ok(), "init_db should succeed: {result:?}");

        let conn = Connection::open(&db_path).expect("should open sqlite db");
        let table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('dataset','column_name','cell')",
                [],
                |row| row.get(0),
            )
            .expect("table count query should succeed");

        assert_eq!(table_count, 3, "required tables should exist");

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn import_creates_dataset_with_headers_and_rows() {
        let temp_dir = unique_test_dir("import-db");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");
        let csv_path = temp_dir.join("people.csv");
        fs::write(&csv_path, "name,city\nAlice,Paris\nBob,Tokyo\n")
            .expect("should write csv fixture");

        init_db(&db_path).expect("init_db should succeed");
        let import_result =
            import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");

        assert_eq!(import_result.row_count, 2, "row count should be stored");

        let conn = Connection::open(&db_path).expect("should open sqlite db");

        let dataset_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset", [], |row| row.get(0))
            .expect("dataset count query should succeed");
        assert_eq!(dataset_count, 1, "exactly one dataset should be inserted");

        let header_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM column_name WHERE dataset_id = ?1",
                [import_result.dataset_id],
                |row| row.get(0),
            )
            .expect("header count query should succeed");
        assert_eq!(header_count, 2, "header count should match csv headers");

        let alice_city: String = conn
            .query_row(
                "SELECT value FROM cell WHERE dataset_id = ?1 AND row_idx = 0 AND col_idx = 1",
                [import_result.dataset_id],
                |row| row.get(0),
            )
            .expect("cell value query should succeed");
        assert_eq!(alice_city, "Paris", "expected imported cell value");

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn import_xlsx_selected_sheets_creates_datasets() {
        let temp_dir = unique_test_dir("import-xlsx");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");
        let xlsx_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("BOM_test.xlsx");

        init_db(&db_path).expect("init_db should succeed");
        let imported = import_xlsx_selected_sheets_to_sqlite(&db_path, &xlsx_path)
            .expect("xlsx import should succeed");

        assert_eq!(imported.len(), 2, "should import assets and merged sheet");

        let conn = Connection::open(&db_path).expect("should open sqlite db");
        let dataset_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM dataset", [], |row| row.get(0))
            .expect("dataset count query should succeed");
        assert_eq!(dataset_count, 2, "should insert two datasets");

        let names: Vec<String> = conn
            .prepare("SELECT name FROM dataset ORDER BY id")
            .expect("prepare should succeed")
            .query_map([], |row| row.get(0))
            .expect("query should succeed")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect should succeed");

        assert!(names.iter().any(|n| n.contains("資產總表")));
        assert!(names.iter().any(|n| n.contains("持股股息合併表")));

        let mut col_stmt = conn
            .prepare(
                "SELECT c.name
                 FROM column_name c
                 JOIN dataset d ON d.id = c.dataset_id
                 WHERE d.name = '資產總表'
                 ORDER BY c.col_idx ASC",
            )
            .expect("prepare should succeed");
        let asset_cols = col_stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query should succeed")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect should succeed");
        assert!(
            !asset_cols.iter().any(|c| c == "小計"),
            "summary formula columns should not be imported"
        );

        let mut hold_stmt = conn
            .prepare(
                "SELECT c.name
                 FROM column_name c
                 JOIN dataset d ON d.id = c.dataset_id
                 WHERE d.name = '持股股息合併表'
                 ORDER BY c.col_idx ASC",
            )
            .expect("prepare should succeed");
        let hold_cols = hold_stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query should succeed")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect should succeed");
        assert!(
            !hold_cols
                .iter()
                .any(|c| c == "內" || c == "外" || c == "平均配息合計"),
            "merged sheet should hide summary columns"
        );
        assert!(
            hold_cols.iter().any(|c| c == "配息方式"),
            "merged sheet should contain dividend columns"
        );
        assert!(
            hold_cols.iter().any(|c| c == "總成本"),
            "merged sheet should contain holding columns"
        );

        let total_cost: String = conn
            .query_row(
                "SELECT cell.value
                 FROM cell
                 JOIN dataset d ON d.id = cell.dataset_id
                 JOIN column_name c ON c.dataset_id = d.id AND c.col_idx = cell.col_idx
                 WHERE d.name = '持股股息合併表'
                   AND c.name = '總成本'
                   AND EXISTS (
                     SELECT 1
                     FROM cell code
                     JOIN column_name cc ON cc.dataset_id = d.id AND cc.col_idx = code.col_idx
                     WHERE code.dataset_id = d.id
                       AND code.row_idx = cell.row_idx
                       AND cc.name = '代號'
                       AND code.value = '00882'
                   )
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("should find computed total cost");
        assert_eq!(total_cost, "63203.5");

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn soft_delete_hides_dataset_from_default_list() {
        let temp_dir = unique_test_dir("soft-delete");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");
        let csv_path = temp_dir.join("sample.csv");
        fs::write(&csv_path, "name\nAlice\n").expect("should write csv fixture");

        let imported = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");
        soft_delete_dataset(&db_path, imported.dataset_id).expect("soft delete should succeed");

        let visible = list_datasets(&db_path, false).expect("list visible should succeed");
        assert!(visible.is_empty(), "soft deleted dataset should be hidden");

        let with_deleted = list_datasets(&db_path, true).expect("list with deleted should succeed");
        assert_eq!(with_deleted.len(), 1, "deleted dataset should still exist");
        assert!(
            with_deleted[0].deleted_at.is_some(),
            "deleted_at should be set"
        );

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn purge_dataset_removes_related_records() {
        let temp_dir = unique_test_dir("purge-dataset");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");
        let csv_path = temp_dir.join("sample.csv");
        fs::write(&csv_path, "name,city\nAlice,Paris\n").expect("should write csv fixture");

        let imported = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");
        purge_dataset(&db_path, imported.dataset_id).expect("purge should succeed");

        let conn = Connection::open(&db_path).expect("should open sqlite db");
        let dataset_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM dataset WHERE id=?1",
                [imported.dataset_id],
                |row| row.get(0),
            )
            .expect("dataset count query should succeed");
        let column_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM column_name WHERE dataset_id=?1",
                [imported.dataset_id],
                |row| row.get(0),
            )
            .expect("column count query should succeed");
        let cell_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM cell WHERE dataset_id=?1",
                [imported.dataset_id],
                |row| row.get(0),
            )
            .expect("cell count query should succeed");

        assert_eq!(dataset_count, 0);
        assert_eq!(column_count, 0);
        assert_eq!(cell_count, 0);

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    fn seed_query_fixture() -> (PathBuf, i64) {
        let temp_dir = unique_test_dir("query-db");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");

        let db_path = temp_dir.join("app.sqlite");
        let csv_path = temp_dir.join("people.csv");
        fs::write(
            &csv_path,
            "name,city,dept\nAlice,Paris,Sales\nBob,Tokyo,Engineering\nCara,Boston,Sales\nDylan,Berlin,Support\n",
        )
        .expect("should write csv fixture");

        init_db(&db_path).expect("init_db should succeed");
        let imported = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");

        (temp_dir, imported.dataset_id)
    }

    #[test]
    fn query_page_returns_expected_first_page() {
        let (temp_dir, dataset_id) = seed_query_fixture();
        let db_path = temp_dir.join("app.sqlite");

        let (columns, rows, total_rows) =
            query_page(&db_path, dataset_id, 0, 2, &QueryOptions::default())
                .expect("query should succeed");

        assert_eq!(columns, vec!["name", "city", "dept"]);
        assert_eq!(total_rows, 4);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["Alice", "Paris", "Sales"]);
        assert_eq!(rows[1], vec!["Bob", "Tokyo", "Engineering"]);

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn query_page_supports_global_search() {
        let (temp_dir, dataset_id) = seed_query_fixture();
        let db_path = temp_dir.join("app.sqlite");

        let options = QueryOptions {
            global_search: "tok".to_string(),
            ..QueryOptions::default()
        };

        let (columns, rows, total_rows) =
            query_page(&db_path, dataset_id, 0, 10, &options).expect("query should succeed");

        assert_eq!(columns, vec!["name", "city", "dept"]);
        assert_eq!(total_rows, 1);
        assert_eq!(rows, vec![vec!["Bob", "Tokyo", "Engineering"]]);

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn query_page_supports_column_search_and_sort() {
        let (temp_dir, dataset_id) = seed_query_fixture();
        let db_path = temp_dir.join("app.sqlite");

        let options = QueryOptions {
            column_search_col: Some(2),
            column_search_text: "sale".to_string(),
            sort_col: Some(0),
            sort_desc: true,
            ..QueryOptions::default()
        };

        let (_columns, rows, total_rows) =
            query_page(&db_path, dataset_id, 0, 10, &options).expect("query should succeed");

        assert_eq!(total_rows, 2);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["Cara", "Boston", "Sales"]);
        assert_eq!(rows[1], vec!["Alice", "Paris", "Sales"]);

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn query_page_rejects_invalid_column_indices() {
        let (temp_dir, dataset_id) = seed_query_fixture();
        let db_path = temp_dir.join("app.sqlite");

        let bad_search = QueryOptions {
            column_search_col: Some(99),
            column_search_text: "x".to_string(),
            ..QueryOptions::default()
        };
        let err = query_page(&db_path, dataset_id, 0, 10, &bad_search)
            .expect_err("invalid search column should return error");
        assert!(
            err.to_string().contains("column_search_col out of range"),
            "unexpected error: {err:#}"
        );

        let bad_sort = QueryOptions {
            sort_col: Some(99),
            ..QueryOptions::default()
        };
        let err = query_page(&db_path, dataset_id, 0, 10, &bad_sort)
            .expect_err("invalid sort column should return error");
        assert!(
            err.to_string().contains("sort_col out of range"),
            "unexpected error: {err:#}"
        );

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn default_db_path_uses_bom_app_directory() {
        let db_path = default_db_path().expect("default db path should resolve");
        let app_dir = db_path
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .expect("db path should include app directory");

        assert_eq!(
            db_path.file_name().and_then(|name| name.to_str()),
            Some("datasets.sqlite")
        );
        assert_eq!(app_dir, "bom", "app data directory should be BOM");
    }

    #[test]
    fn ensure_webview_data_dir_creates_webview2_subdir() {
        let temp_dir = unique_test_dir("webview-data-dir");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");

        let webview_dir =
            ensure_webview_data_dir(&temp_dir).expect("webview data dir should be created");

        assert_eq!(webview_dir, temp_dir.join("webview2"));
        assert!(webview_dir.is_dir(), "webview2 directory should exist");

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }

    #[test]
    fn format_number_with_commas_handles_decimals() {
        assert_eq!(format_number_with_commas(12345.678, 0), "12,346");
        assert_eq!(format_number_with_commas(12345.678, 2), "12,345.68");
        assert_eq!(format_number_with_commas(-1234.5, 2), "-1,234.50");
    }

    #[test]
    fn format_cell_value_applies_header_rules() {
        assert_eq!(format_cell_value("買進", "1234.5"), "1,234.50");
        assert_eq!(format_cell_value("損益率", "0.1234"), "12.34%");
        assert_eq!(format_cell_value("代號", "0050"), "0050");
    }

    #[test]
    fn column_alignment_prefers_text_headers() {
        let rows = vec![vec!["0050".to_string()], vec!["006208".to_string()]];
        assert_eq!(column_alignment("代號", &rows, 0), "left");
    }

    #[test]
    fn format_ratio_or_na_handles_zero_denominator() {
        assert_eq!(format_ratio_or_na(10.0, 0.0), "N/A");
        assert_eq!(format_ratio_or_na(25.0, 200.0), "0.125");
    }

    #[test]
    fn transform_assets_sheet_keeps_required_columns_and_adds_settlement_column() {
        let rows = vec![
            vec![
                "證券戶".to_string(),
                "王小明".to_string(),
                "元大證券".to_string(),
                "A12345".to_string(),
                "TWD".to_string(),
                "100000".to_string(),
            ],
            vec![
                "銀行活存".to_string(),
                "王小明".to_string(),
                "台灣銀行".to_string(),
                "B67890".to_string(),
                "TWD".to_string(),
                "50000".to_string(),
            ],
            vec![
                "銀行活存".to_string(),
                "".to_string(),
                "台灣銀行".to_string(),
                "C00001".to_string(),
                "TWD".to_string(),
                "40000".to_string(),
            ],
            vec![
                "銀行活存".to_string(),
                "王小明".to_string(),
                "台灣銀行".to_string(),
                "C00002".to_string(),
                "TWD".to_string(),
                "N/A".to_string(),
            ],
            vec![
                "交割款".to_string(),
                "王小明".to_string(),
                "元大證券".to_string(),
                "A12345".to_string(),
                "TWD".to_string(),
                "777".to_string(),
            ],
        ];

        let (headers, data) = transform_assets_sheet(&rows, 0.0, 0.0);

        assert_eq!(
            headers,
            vec![
                "資產形式",
                "所有權人",
                "往來機構",
                "帳號",
                "幣別",
                "餘額",
                "交割款"
            ]
        );
        assert_eq!(data.len(), 2, "格式不正確與交割款列應移除");
        assert_eq!(data[0][0], "證券戶");
        assert_eq!(data[0][6], "", "交割款預設應為空白");
        assert_eq!(data[1][0], "銀行活存");
        assert_eq!(data[1][6], "");
    }

    #[test]
    fn reorder_headers_and_rows_applies_preferred_order() {
        let headers = vec![
            "名稱".to_string(),
            "類別".to_string(),
            "性質".to_string(),
            "國內 /國外".to_string(),
            "代號".to_string(),
            "買進".to_string(),
            "市價".to_string(),
            "數量".to_string(),
            "所有權人".to_string(),
            "配息方式".to_string(),
            "期數".to_string(),
            "其他".to_string(),
        ];
        let rows = vec![headers.iter().map(|h| h.clone()).collect::<Vec<_>>()];
        let preferred = [
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

        let (new_headers, new_rows) = reorder_headers_and_rows(&headers, &rows, &preferred);

        assert_eq!(
            new_headers,
            vec![
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
                "其他"
            ]
        );
        assert_eq!(new_rows[0], new_headers);
    }

    #[test]
    fn holdings_editable_and_required_columns_match_spec() {
        let required = required_columns_for_holdings();
        let editable = editable_columns_for_holdings();
        assert!(required.iter().all(|c| editable.contains(c)));
        assert!(required.contains(&"所有權人".to_string()));
        assert!(required.contains(&"配息方式".to_string()));
        assert!(!editable.contains(&"總成本".to_string()));
    }

    #[test]
    fn apply_edit_updates_staged_value_only_for_editable_columns() {
        let mut state = EditingState::new_for_test();
        let cell_id = state.cell_id("所有權人", 0, 0);
        assert!(state.start_edit(cell_id.clone()));
        state.apply_edit(cell_id.clone(), "王小明");
        assert_eq!(state.staged_value(cell_id), "王小明");
    }

    #[test]
    fn add_row_requires_required_fields() {
        let mut state = EditTableState::new_for_test();
        let mut row = state.empty_row();
        if let Some(idx) = state.header_index("所有權人") {
            row[idx] = String::new();
        }
        let result = state.add_row(row);
        assert!(result.is_err());
    }

    #[test]
    fn delete_rows_marks_rows_and_tracks_changes() {
        let mut state = EditTableState::new_for_test();
        state.select_rows(vec![0, 2]);
        state.delete_selected();
        assert!(state.is_deleted(0));
        assert!(state.is_deleted(2));
    }

    #[test]
    fn change_marking_applies_cell_and_row_styles() {
        let mut state = EditTableState::new_for_test();
        let col_idx = state.header_index("名稱").unwrap_or(0);
        state.apply_edit(
            CellKey {
                row_idx: 0,
                col_idx,
                column: "名稱".to_string(),
            },
            "新名稱",
        );
        assert!(state.is_cell_modified("名稱", 0));
    }

    #[test]
    fn apply_changes_to_dataset_updates_rows() {
        let temp_dir = unique_test_dir("apply-changes");
        fs::create_dir_all(&temp_dir).expect("should create temp dir");
        let db_path = temp_dir.join("app.sqlite");
        let csv_path = temp_dir.join("people.csv");
        fs::write(&csv_path, "name,city\nAlice,Paris\nBob,Tokyo\n")
            .expect("should write csv fixture");

        let imported = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");
        let (columns, rows, _total) = query_page(
            &db_path,
            imported.dataset_id,
            0,
            10,
            &QueryOptions::default(),
        )
        .expect("query should succeed");

        let mut staged = HashMap::new();
        staged.insert(
            CellKey {
                row_idx: 0,
                col_idx: 1,
                column: "city".to_string(),
            },
            "Berlin".to_string(),
        );
        let mut deleted = BTreeSet::new();
        deleted.insert(1);
        let added = vec![vec!["Cara".to_string(), "Rome".to_string()]];

        apply_changes_to_dataset(
            &db_path,
            imported.dataset_id,
            &columns,
            &rows,
            &staged,
            &deleted,
            &added,
        )
        .expect("apply changes should succeed");

        let (_columns, new_rows, total_rows) = query_page(
            &db_path,
            imported.dataset_id,
            0,
            10,
            &QueryOptions::default(),
        )
        .expect("query should succeed");

        assert_eq!(total_rows, 2);
        assert_eq!(new_rows[0], vec!["Alice", "Berlin"]);
        assert_eq!(new_rows[1], vec!["Cara", "Rome"]);

        fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
    }
}

use std::sync::Arc;

use anyhow::anyhow;
use dioxus::prelude::*;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};

use crate::domain::entities::dataset::DatasetId;
use crate::domain::entities::edit::{CellKey, StagedEdits};
use crate::infra::sqlite::repo::SqliteRepo;
use crate::platform::desktop::blocking::run_blocking;
use crate::ui::state::app_state::AppState;
use crate::usecase::ports::repo::{DatasetRepository, NewDatasetMeta, TabularData};
use crate::usecase::services::edit_service::EditService;
use crate::usecase::services::import_service::ImportService;
use crate::usecase::services::query_service::QueryService;
use crate::{
    build_dataset_groups, column_alignment, default_dataset_name_mmdd, default_db_path,
    editable_columns_for_holdings, format_cell_value, is_holdings_table, parse_numeric_value,
    reload_page_data_usecase, required_columns_for_holdings, validate_required_holdings_row,
    PendingAction, QueryOptions, NONE_OPTION_VALUE, PAGE_SIZE,
};

#[component]
pub fn App() -> Element {
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

    let AppState {
        mut datasets,
        mut selected_group_key,
        mut selected_dataset_id,
        mut columns,
        mut rows,
        mut page,
        mut total_rows,
        mut global_search,
        mut column_search_col,
        mut column_search_text,
        mut sort_col,
        mut sort_desc,
        mut show_deleted,
        mut busy,
        mut status,
        mut staged_cells,
        mut deleted_rows,
        mut selected_rows,
        mut editing_cell,
        mut editing_value,
        mut added_rows,
        mut show_add_row,
        mut new_row_inputs,
        mut context_menu,
        mut context_row,
        mut pending_action,
        mut show_save_prompt,
        mut show_save_as_prompt,
        mut save_as_name,
    } = AppState::new();

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
        let init_result = run_blocking(|| {
            repo_for_init
                .init()
                .map_err(|err| anyhow!(err.to_string()))
                .and_then(|_| {
                    query_service_for_init
                        .list_datasets(false)
                        .map_err(|err| anyhow!(err.to_string()))
                })
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
    let query_service_for_column_search = query_service.clone();
    let query_service_for_sort_select = query_service.clone();
    let query_service_for_sort_toggle = query_service.clone();
    let query_service_for_tab_switch = query_service.clone();
    let query_service_for_show_deleted = query_service.clone();
    let query_service_for_save = query_service.clone();
    let query_service_for_save_as = query_service.clone();
    let query_service_for_import_overwrite = query_service.clone();
    let query_service_for_import_save_as = query_service.clone();
    let edit_service_for_save = edit_service.clone();
    let edit_service_for_save_as = edit_service.clone();
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
    let editable_columns = Arc::new(editable_columns_for_holdings());
    let required_columns = Arc::new(required_columns_for_holdings());
    let base_row_count = current_rows.len();
    let has_pending_changes = !staged_cells_snapshot.is_empty()
        || !deleted_rows_snapshot.is_empty()
        || !added_rows_snapshot.is_empty();
    let current_columns_for_add = Arc::new(current_columns.clone());
    let current_columns_for_save = current_columns.clone();
    let current_rows_for_save = current_rows.clone();
    let datasets_for_save = datasets_snapshot.clone();
    let current_columns_for_save_as = current_columns_for_save.clone();
    let current_rows_for_save_as = current_rows_for_save.clone();
    let table_columns = Arc::new(current_columns.clone());
    let table_rows = Arc::new(current_rows.clone());

    rsx! {
        div {
            onclick: move |_| {
                context_menu.set(None);
                context_row.set(None);
            },
            style: "font-family: 'Noto Sans TC', sans-serif; padding: 12px;",

            h2 { "BOM" }

            div {
                style: "display: flex; gap: 8px; align-items: center; margin-bottom: 12px;",
                button {
                    disabled: busy(),
                    onclick: move |_| {
                        let query_service_for_import = query_service_for_import.clone();
                        let import_service_for_import = import_service_for_import.clone();

                        if is_holdings && has_pending_changes {
                            if let Some(file_path) = FileDialog::new()
                                .add_filter("CSV", &["csv"])
                                .add_filter("Excel", &["xlsx"])
                                .pick_file() {
                                    pending_action.set(Some(PendingAction::Import(file_path)));
                                    show_save_prompt.set(true);
                            }
                            return;
                        }

                        if let Some(file_path) = FileDialog::new()
                            .add_filter("CSV", &["csv"])
                            .add_filter("Excel", &["xlsx"])
                            .pick_file() {
                            *busy.write() = true;
                            *status.write() = format!("正在匯入 {}", file_path.display());
                            let ext = file_path
                                .extension()
                                .and_then(|e| e.to_str())
                                .map(|s| s.to_ascii_lowercase())
                                .unwrap_or_default();
                            let import_result = run_blocking(|| {
                                if ext == "xlsx" {
                                    import_service_for_import
                                        .import_xlsx(&file_path)
                                        .map(|items| {
                                            (items.first().map(|it| it.dataset_id), items.len() as i64, true)
                                        })
                                } else {
                                    import_service_for_import
                                        .import_csv(&file_path)
                                        .map(|item| (Some(item.dataset_id), item.row_count, false))
                                }
                            });

                            match import_result {
                                Ok((selected_id, imported_count, is_xlsx)) => match run_blocking(|| {
                                    query_service_for_import.list_datasets(show_deleted())
                                }) {
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
                                        match reload_page_data_usecase(
                                            &query_service_for_import,
                                            selected_id,
                                            0,
                                            &QueryOptions::default(),
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
                        }
                    },
                    "匯入 CSV / XLSX"
                }

                label { "顯示已刪除" }
                input {
                    r#type: "checkbox",
                    disabled: busy(),
                    checked: show_deleted(),
                    onchange: move |event| {
                        let checked = event.value().parse::<bool>().unwrap_or(false);
                        show_deleted.set(checked);
                        *busy.write() = true;

                        match query_service_for_show_deleted.list_datasets(checked) {
                            Ok(available) => {
                                let groups = build_dataset_groups(&available);
                                *datasets.write() = available;
                                let next_dataset = if checked {
                                    selected_dataset_id()
                                } else {
                                    groups
                                        .iter()
                                        .find(|g| g.datasets.iter().any(|d| d.id.0 == selected_dataset_id().unwrap_or(-1)))
                                        .and_then(|g| g.datasets.first())
                                        .map(|d| d.id.0)
                                };
                                *selected_dataset_id.write() = next_dataset;
                                *selected_group_key.write() = groups
                                    .iter()
                                    .find(|g| g.datasets.iter().any(|d| d.id.0 == next_dataset.unwrap_or(-1)))
                                    .map(|g| g.key.clone());
                                *page.write() = 0;

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
                                }
                                Err(err) => {
                                    *status.write() = format!("載入資料集失敗：{err}");
                                }
                            }

                            *busy.write() = false;
                        }
                    },
                    option { value: NONE_OPTION_VALUE, "(未選擇)" }
                    for group in grouped_datasets.iter() {
                        option { value: "{group.key}", "{group.label}" }
                    }
                }

                if let Some(active_group) = active_group {
                    span { style: "margin-left: 12px;", "工作表" }
                    select {
                        disabled: busy(),
                        value: selected_dataset_id().map(|id| id.to_string()).unwrap_or_default(),
                        onchange: {
                            let query_service_for_tab_switch =
                                query_service_for_tab_switch.clone();
                            move |event| {
                                let next_dataset = event.value().parse::<i64>().ok();
                                if is_holdings && has_pending_changes {
                                    if let Some(id) = next_dataset {
                                        pending_action.set(Some(PendingAction::TabSwitch { dataset_id: id }));
                                        show_save_prompt.set(true);
                                    }
                                    return;
                                }

                                *selected_dataset_id.write() = next_dataset;
                                *page.write() = 0;
                                *busy.write() = true;
                                match reload_page_data_usecase(
                                    &query_service_for_tab_switch,
                                    next_dataset,
                                    0,
                                    &QueryOptions::default(),
                                ) {
                                    Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                        *columns.write() = loaded_columns;
                                        *rows.write() = loaded_rows;
                                        *total_rows.write() = loaded_total;
                                        *page.write() = loaded_page;
                                    }
                                    Err(err) => {
                                        *status.write() = format!("載入工作表失敗：{err}");
                                    }
                                }
                                *busy.write() = false;
                            }
                        },
                        for sheet in active_group.datasets.iter() {
                            option { value: "{sheet.id.0}", "{sheet.name}" }
                        }
                    }
                }
            }

            div {
                style: "display: flex; gap: 12px; align-items: center; margin: 12px 0;",
                input {
                    placeholder: "全域搜尋",
                    value: global_search(),
                    oninput: move |event| global_search.set(event.value()),
                }
                button {
                    disabled: busy(),
                    onclick: {
                        let query_service_for_global_search =
                            query_service_for_global_search.clone();
                        move |_| {
                        if selected_dataset_id().is_none() {
                            return;
                        }
                        *busy.write() = true;
                        let options = QueryOptions {
                            global_search: global_search(),
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
                            }
                            Err(err) => {
                                *status.write() = format!("搜尋失敗：{err}");
                            }
                        }
                        *busy.write() = false;
                        }
                    },
                    "搜尋"
                }
            }

            if !current_columns.is_empty() {
                div { style: "margin-bottom: 12px;",
                    select {
                        disabled: busy(),
                        value: column_search_col()
                            .map(|idx| idx.to_string())
                            .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                        onchange: move |event| {
                            let value = event.value();
                            if value == NONE_OPTION_VALUE {
                                column_search_col.set(None);
                                return;
                            }
                            let idx = value.parse::<i64>().ok();
                            column_search_col.set(idx);
                        },
                        option { value: NONE_OPTION_VALUE, "選擇欄位" }
                        for (idx, header) in current_columns.iter().enumerate() {
                            option { value: "{idx}", "{header}" }
                        }
                    }
                    input {
                        placeholder: "欄位搜尋",
                        value: column_search_text(),
                        oninput: move |event| column_search_text.set(event.value()),
                    }
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            if selected_dataset_id().is_none() {
                                return;
                            }
                            *busy.write() = true;
                            let options = QueryOptions {
                                global_search: global_search(),
                                column_search_col: column_search_col(),
                                column_search_text: column_search_text(),
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
                                }
                                Err(err) => {
                                    *status.write() = format!("欄位搜尋失敗：{err}");
                                }
                            }
                            *busy.write() = false;
                        },
                        "欄位搜尋"
                    }
                }
            }

            if !current_columns.is_empty() {
                div { style: "margin-bottom: 12px;",
                    select {
                        disabled: busy(),
                        value: sort_col()
                            .map(|idx| idx.to_string())
                            .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                        onchange: move |event| {
                            let value = event.value();
                            if value == NONE_OPTION_VALUE {
                                sort_col.set(None);
                                return;
                            }
                            let idx = value.parse::<i64>().ok();
                            sort_col.set(idx);
                        },
                        option { value: NONE_OPTION_VALUE, "選擇排序欄位" }
                        for (idx, header) in current_columns.iter().enumerate() {
                            option { value: "{idx}", "{header}" }
                        }
                    }
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            if selected_dataset_id().is_none() {
                                return;
                            }
                            sort_desc.set(!sort_desc());
                            *busy.write() = true;
                            let options = QueryOptions {
                                global_search: global_search(),
                                column_search_col: column_search_col(),
                                column_search_text: column_search_text(),
                                sort_col: sort_col(),
                                sort_desc: sort_desc(),
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
                                }
                                Err(err) => {
                                    *status.write() = format!("排序失敗：{err}");
                                }
                            }
                            *busy.write() = false;
                        },
                        if sort_desc() { "降冪" } else { "升冪" }
                    }
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            if selected_dataset_id().is_none() {
                                return;
                            }
                            *busy.write() = true;
                            let options = QueryOptions {
                                global_search: global_search(),
                                column_search_col: column_search_col(),
                                column_search_text: column_search_text(),
                                sort_col: sort_col(),
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
                                }
                                Err(err) => {
                                    *status.write() = format!("排序失敗：{err}");
                                }
                            }
                            *busy.write() = false;
                        },
                        "套用排序"
                    }
                }
            }

            if is_holdings {
                div { style: "margin-bottom: 12px; display: flex; gap: 8px;",
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            show_add_row.set(true);
                        },
                        "新增列"
                    }
                    button {
                        disabled: busy() || !has_pending_changes,
                        onclick: move |_| {
                            show_save_prompt.set(true);
                        },
                        "儲存變更"
                    }
                }
            }

            if show_add_row() {
                div {
                    style: "position: fixed; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 1100;",
                    div {
                        style: "background: #fff; padding: 16px; border: 1px solid #999; min-width: 300px;",
                        div { style: "margin-bottom: 8px; font-weight: 600;", "新增列" }
                        div { style: "display: grid; grid-template-columns: 120px 1fr; gap: 6px;",
                            {current_columns_for_add.iter().map(|header| {
                                let header_for_input = header.clone();
                                rsx!(
                                    label { "{header}" }
                                    input {
                                        value: new_row_inputs().get(header).cloned().unwrap_or_default(),
                                        oninput: move |event| {
                                            new_row_inputs
                                                .write()
                                                .insert(header_for_input.clone(), event.value());
                                        }
                                    }
                                )
                            })}
                        }
                        div { style: "display: flex; gap: 8px;",
                            button {
                                onclick: move |_| {
                                    let current_columns_for_add = current_columns_for_add.clone();
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
                        for (idx, header) in table_columns.iter().enumerate() {
                            th { style: "border: 1px solid #bbb; padding: 6px; background: #f2f2f2; text-align: center;", "{header}" }
                            if idx + 1 == table_columns.len() {
                            }
                        }
                    }
                }
                tbody {
                    {table_rows.iter().enumerate().map(|(row_idx, row)| {
                        let table_columns = table_columns.clone();
                        let editable_columns = editable_columns.clone();
                        let required_columns = required_columns.clone();
                        let column_alignments = column_alignments.clone();
                        let row = row.clone();
                        let row_style = format!(
                            "{}{}",
                            if selected_rows_snapshot.contains(&row_idx) {
                                "background: #eef4ff;"
                            } else {
                                ""
                            },
                            if deleted_rows_snapshot.contains(&row_idx) {
                                "border-top: 2px solid #d24; border-bottom: 2px solid #d24;"
                            } else {
                                ""
                            }
                        );
                        rsx!(
                            tr {
                                style: "{row_style}",
                                onclick: move |_| {
                                    if !is_holdings {
                                        return;
                                    }
                                    let mut selected = selected_rows.write();
                                    if selected.contains(&row_idx) {
                                        selected.clear();
                                    } else {
                                        selected.clear();
                                        selected.insert(row_idx);
                                    }
                                },
                                oncontextmenu: move |event| {
                                    event.stop_propagation();
                                    context_menu.set(Some((event.client_coordinates().x, event.client_coordinates().y)));
                                    context_row.set(Some(row_idx));
                                },
                                {row.iter().enumerate().map(|(col_idx, value)| {
                                    let value = value.clone();
                                    let header = table_columns.get(col_idx).cloned().unwrap_or_default();
                                    let formatted = format_cell_value(&header, &value);
                                    let alignment = column_alignments.get(col_idx).copied().unwrap_or("left");
                                    let table_columns_for_cell = table_columns.clone();
                                    let required_columns_for_cell = required_columns.clone();
                                    let editable_columns_for_cell = editable_columns.clone();
                                    let cell_key = CellKey {
                                        row_idx,
                                        col_idx,
                                        column: header.clone(),
                                    };
                                    let is_editing = editing_cell_snapshot.as_ref() == Some(&cell_key);
                                    if is_editing {
                                        rsx!(
                                            td {
                                                style: "border: 1px solid #bbb; padding: 4px; text-align: {alignment};",
                                                input {
                                                    value: editing_value(),
                                                    oninput: move |event| {
                                                        editing_value.set(event.value());
                                                    },
                                                    onkeydown: move |event| {
                                                        if event.key() == Key::Enter {
                                                            let next_value = editing_value();
                                                            if let Some(col_name) = table_columns_for_cell.get(col_idx) {
                                                                if required_columns_for_cell.contains(col_name)
                                                                    && parse_numeric_value(&next_value).is_none()
                                                                {
                                                                    *status.write() = "必填欄位不可空白".to_string();
                                                                    return;
                                                                }
                                                            }
                                                            staged_cells.write().insert(cell_key.clone(), next_value.clone());
                                                            *editing_cell.write() = None;
                                                            editing_value.set(String::new());
                                                        } else if event.key() == Key::Escape {
                                                            *editing_cell.write() = None;
                                                            editing_value.set(String::new());
                                                        }
                                                    }
                                                }
                                            }
                                        )
                                    } else {
                                        rsx!(
                                            td {
                                                style: "border: 1px solid #bbb; padding: 4px; text-align: {alignment};",
                                            ondoubleclick: move |_| {
                                                    if !is_holdings {
                                                        return;
                                                    }
                                                    if editable_columns_for_cell.contains(&header) {
                                                        *editing_cell.write() = Some(cell_key.clone());
                                                        editing_value.set(value.clone());
                                                    }
                                                },
                                                "{formatted}"
                                            }
                                        )
                                    }
                                })}
                            }
                        )
                    })}

                    if !added_rows_snapshot.is_empty() {
                        {added_rows_snapshot.iter().enumerate().map(|(row_idx, row)| {
                            let table_columns = table_columns.clone();
                            let column_alignments = column_alignments.clone();
                            let row = row.clone();
                            let display_row = base_row_count + row_idx;
                            rsx!(
                                tr {
                                    style: "background: #d9f7d9;",
                                    onclick: move |_| {
                                        if !is_holdings {
                                            return;
                                        }
                                        let mut selected = selected_rows.write();
                                        if selected.contains(&display_row) {
                                            selected.clear();
                                        } else {
                                            selected.clear();
                                            selected.insert(display_row);
                                        }
                                    },
                                    {row.iter().enumerate().map(|(col_idx, value)| {
                                        let value = value.clone();
                                        let header =
                                            table_columns.get(col_idx).cloned().unwrap_or_default();
                                        let alignment =
                                            column_alignments.get(col_idx).copied().unwrap_or("left");
                                        rsx!(
                                            td {
                                                style: "border: 1px solid #bbb; padding: 4px; text-align: {alignment};",
                                                "{format_cell_value(&header, &value)}"
                                            }
                                        )
                                    })}
                                }
                            )
                        })}
                    }
                }
            }

            if let Some((x, y)) = context_menu() {
                div {
                    style: "position: fixed; left: {x}px; top: {y}px; background: #fff; border: 1px solid #aaa; z-index: 1000; padding: 4px;",
                    div {
                        style: "display: flex; flex-direction: column; gap: 4px;",
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
                                                    let import_result = run_blocking(|| {
                                                        if ext == "xlsx" {
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
                                                        }
                                                    });
                                                    match import_result {
                                                        Ok((selected_id, imported_count, is_xlsx)) => {
                                                            match run_blocking(|| {
                                                                query_service_for_import_overwrite
                                                                    .list_datasets(show_deleted())
                                                            }) {
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
                                                    let import_result = run_blocking(|| {
                                                        if ext == "xlsx" {
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
                                                        }
                                                    });
                                                    match import_result {
                                                        Ok((selected_id, imported_count, is_xlsx)) => {
                                                            match run_blocking(|| {
                                                                query_service_for_import_save_as
                                                                    .list_datasets(show_deleted())
                                                            }) {
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

            if let Some(dataset_id) = selected_dataset_id() {
                div { style: "display: flex; gap: 8px; align-items: center; margin-top: 12px;",
                    button {
                        disabled: busy() || page() == 0,
                        onclick: {
                            let query_service_for_global_search =
                                query_service_for_global_search.clone();
                            move |_| {
                            let next_page = (page() - 1).max(0);
                            let options = QueryOptions {
                                global_search: global_search(),
                                column_search_col: column_search_col(),
                                column_search_text: column_search_text(),
                                sort_col: sort_col(),
                                sort_desc: sort_desc(),
                            };
                            match reload_page_data_usecase(
                                &query_service_for_global_search,
                                Some(dataset_id),
                                next_page,
                                &options,
                            ) {
                                Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                    *columns.write() = loaded_columns;
                                    *rows.write() = loaded_rows;
                                    *total_rows.write() = loaded_total;
                                    *page.write() = loaded_page;
                                }
                                Err(err) => {
                                    *status.write() = format!("上一頁失敗：{err}");
                                }
                            }
                            }
                        },
                        "上一頁"
                    }
                    button {
                        disabled: busy() || (page() + 1) * PAGE_SIZE >= current_total_rows,
                        onclick: {
                            let query_service_for_global_search =
                                query_service_for_global_search.clone();
                            move |_| {
                            let next_page = page() + 1;
                            let options = QueryOptions {
                                global_search: global_search(),
                                column_search_col: column_search_col(),
                                column_search_text: column_search_text(),
                                sort_col: sort_col(),
                                sort_desc: sort_desc(),
                            };
                            match reload_page_data_usecase(
                                &query_service_for_global_search,
                                Some(dataset_id),
                                next_page,
                                &options,
                            ) {
                                Ok((loaded_columns, loaded_rows, loaded_total, loaded_page)) => {
                                    *columns.write() = loaded_columns;
                                    *rows.write() = loaded_rows;
                                    *total_rows.write() = loaded_total;
                                    *page.write() = loaded_page;
                                }
                                Err(err) => {
                                    *status.write() = format!("下一頁失敗：{err}");
                                }
                            }
                            }
                        },
                        "下一頁"
                    }
                    span { "第 {page() + 1} 頁" }
                }
            }
        }
    }
}

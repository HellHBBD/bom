use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::anyhow;
use dioxus::prelude::*;
use rfd::{FileDialog, MessageButtons, MessageDialog, MessageDialogResult, MessageLevel};

use crate::domain::entities::dataset::{DatasetId, PageQuery};
use crate::domain::entities::edit::{CellKey, StagedEdits};
use crate::infra::sqlite::repo::SqliteRepo;
use crate::platform::desktop::blocking::run_blocking;
use crate::ui::state::app_state::AppState;
use crate::usecase::ports::repo::{DatasetRepository, NewDatasetMeta, TabularData};
use crate::usecase::services::edit_service::EditService;
use crate::usecase::services::import_service::ImportService;
use crate::usecase::services::query_service::QueryService;
use crate::{
    apply_column_visibility, build_dataset_groups, choose_default_dataset_id,
    choose_next_dataset_after_delete, column_alignment, compute_summary_report, dataset_tab_kind,
    default_dataset_name_mmdd, default_db_path, editable_columns_for_assets,
    editable_columns_for_holdings, format_cell_value, is_holdings_table,
    normalize_column_visibility, parse_numeric_value, reload_page_data_usecase,
    required_columns_for_holdings, root_container_style_for_scroll,
    table_container_style_for_scroll, table_header_cell_style, table_overflow_style_for_scroll,
    table_scroll_mode, validate_required_holdings_row, DatasetTabKind, PendingAction, QueryOptions,
    SummaryReport, NONE_OPTION_VALUE, PAGE_SIZE,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropdownId {
    Dataset,
    Sheet,
    Column,
    ColumnVisibility,
    Sort,
}

#[derive(Clone, Debug, PartialEq)]
struct DropdownOption {
    value: String,
    label: String,
}

fn dropdown_label(options: &[DropdownOption], selected: Option<&str>) -> String {
    selected
        .and_then(|value| options.iter().find(|opt| opt.value == value))
        .map(|opt| opt.label.clone())
        .unwrap_or_else(|| "(未選擇)".to_string())
}

#[component]
fn DropdownSelect(
    id: DropdownId,
    label: &'static str,
    options: Vec<DropdownOption>,
    selected: Option<String>,
    mut open_dropdown: Signal<Option<DropdownId>>,
    mut dropdown_pos: Signal<Option<(f64, f64)>>,
    on_select: EventHandler<String>,
) -> Element {
    let is_open = open_dropdown() == Some(id);
    let selected_label = dropdown_label(&options, selected.as_deref());
    let (left, top) = dropdown_pos().unwrap_or((0.0, 0.0));

    rsx! {
        div {
            style: "position: relative; display: inline-flex; align-items: center; gap: 6px;",
            span { "{label}" }
            button {
                style: "border: 1px solid #bbb; background: #fff; padding: 4px 10px; border-radius: 6px; cursor: pointer;",
                onclick: move |event| {
                    event.stop_propagation();
                    if open_dropdown() == Some(id) {
                        open_dropdown.set(None);
                        return;
                    }
                    let point = event.client_coordinates();
                    dropdown_pos.set(Some((point.x, point.y + 24.0)));
                    open_dropdown.set(Some(id));
                },
                "{selected_label}"
            }
        }

        if is_open {
            div {
                style: "position: fixed; left: {left}px; top: {top}px; min-width: 200px; max-height: 320px; overflow-y: auto; background: #fff; border: 1px solid #bbb; border-radius: 8px; box-shadow: 0 10px 24px rgba(0,0,0,0.15); z-index: 1200;",
                onclick: move |event| event.stop_propagation(),
                {options.iter().map(|opt| {
                    let value = opt.value.clone();
                    let label = opt.label.clone();
                    let is_selected = selected.as_deref() == Some(value.as_str());
                    let background = if is_selected { "#eef4ff" } else { "transparent" };
                    rsx!(
                        div {
                            style: "padding: 8px 10px; cursor: pointer; background: {background};",
                            onclick: move |_| {
                                on_select.call(value.clone());
                                open_dropdown.set(None);
                            },
                            "{label}"
                        }
                    )
                })}
            }
        }
    }
}

#[component]
fn ColumnVisibilityDropdown(
    id: DropdownId,
    label: &'static str,
    columns: Vec<String>,
    visibility: BTreeMap<i64, bool>,
    mut open_dropdown: Signal<Option<DropdownId>>,
    mut dropdown_pos: Signal<Option<(f64, f64)>>,
    on_toggle: EventHandler<(i64, bool)>,
) -> Element {
    let is_open = open_dropdown() == Some(id);
    let (left, top) = dropdown_pos().unwrap_or((0.0, 0.0));

    rsx! {
        div {
            style: "position: relative; display: inline-flex; align-items: center; gap: 6px;",
            span { "{label}" }
            button {
                style: "border: 1px solid #bbb; background: #fff; padding: 4px 10px; border-radius: 6px; cursor: pointer;",
                onclick: move |event| {
                    event.stop_propagation();
                    if open_dropdown() == Some(id) {
                        open_dropdown.set(None);
                        return;
                    }
                    let point = event.client_coordinates();
                    dropdown_pos.set(Some((point.x, point.y + 24.0)));
                    open_dropdown.set(Some(id));
                },
                "顯示欄位"
            }
        }

        if is_open {
            div {
                style: "position: fixed; left: {left}px; top: {top}px; min-width: 220px; max-height: 320px; overflow-y: auto; background: #fff; border: 1px solid #bbb; border-radius: 8px; box-shadow: 0 10px 24px rgba(0,0,0,0.15); z-index: 1200; padding: 6px;",
                onclick: move |event| event.stop_propagation(),
                {columns.iter().enumerate().map(|(idx, header)| {
                    let checked = visibility.get(&(idx as i64)).copied().unwrap_or(true);
                    let header = header.clone();
                    rsx!(
                        label {
                            style: "display: flex; align-items: center; gap: 8px; padding: 6px 4px; cursor: pointer;",
                            input {
                                r#type: "checkbox",
                                checked: checked,
                                onclick: move |_| {
                                    on_toggle.call((idx as i64, !checked));
                                }
                            }
                            span { "{header}" }
                        }
                    )
                })}
            }
        }
    }
}

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
        mut column_visibility,
        mut rows,
        mut holdings_flags,
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
        mut edit_mode,
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

    let mut show_summary_report = use_signal(|| false);
    let mut summary_report = use_signal(SummaryReport::default);
    let mut show_dataset_manager = use_signal(|| false);
    let mut manage_dataset_id = use_signal(|| None::<i64>);
    let mut manage_name_input = use_signal(String::new);

    let db_path = Arc::new(db_path);
    let repo = Arc::new(SqliteRepo {
        db_path: (*db_path).clone(),
    });
    let query_service = Arc::new(QueryService::new(repo.clone()));
    let edit_service = Arc::new(EditService::new(repo.clone()));
    let import_service = Arc::new(ImportService::new((*db_path).clone()));
    let repo_for_init = repo.clone();
    let query_service_for_init = query_service.clone();
    let query_service_for_visibility = query_service.clone();
    let query_service_for_holdings_flags = query_service.clone();
    let mut open_dropdown = use_signal(|| None::<DropdownId>);
    let dropdown_pos = use_signal(|| None::<(f64, f64)>);
    let mut table_header_stuck = use_signal(|| false);
    let mut eval_started = use_signal(|| false);
    let mut eval_handle = use_signal(|| None::<document::Eval>);
    use_effect(move || {
        if eval_started() {
            return;
        }
        eval_started.set(true);
        let mut eval = document::eval(
            r#"
const sendState = () => {
  const header = document.getElementById("table-head");
  if (!header) {
    dioxus.send(false);
    return;
  }
  const top = header.getBoundingClientRect().top;
  dioxus.send(top <= 0);
};
const root = document.getElementById("app-root");
const scrollTarget = root ?? window;
const scrollOptions = { passive: true };
scrollTarget.addEventListener("scroll", sendState, scrollOptions);
window.addEventListener("resize", sendState);
sendState();
await dioxus.recv();
scrollTarget.removeEventListener("scroll", sendState, scrollOptions);
window.removeEventListener("resize", sendState);
"#,
        );
        let mut eval_for_recv = eval.clone();
        eval_handle.set(Some(eval));
        spawn(async move {
            loop {
                let next: bool = match eval_for_recv.recv().await {
                    Ok(value) => value,
                    Err(_) => break,
                };
                table_header_stuck.set(next);
            }
        });
    });
    use_on_unmount(move || {
        if let Some(eval) = eval_handle() {
            let _ = eval.send(());
        }
    });
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
                    .and_then(|g| choose_default_dataset_id(&g.datasets));
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

    use_effect(move || {
        let dataset_id = selected_dataset_id();
        let columns_snapshot = columns();
        if let Some(id) = dataset_id {
            if columns_snapshot.is_empty() {
                column_visibility.set(BTreeMap::new());
                return;
            }
            let visibility_result = run_blocking(|| {
                query_service_for_visibility
                    .load_column_visibility(DatasetId(id))
                    .map_err(|err| anyhow!(err.to_string()))
            });

            let visibility_loaded = visibility_result.is_ok();
            let visibility = match visibility_result {
                Ok(map) => map,
                Err(err) => {
                    *status.write() = format!("載入欄位顯示失敗：{err}");
                    BTreeMap::new()
                }
            };
            let normalized = normalize_column_visibility(&columns_snapshot, &visibility);
            let should_persist_default =
                visibility_loaded && visibility.is_empty() && is_holdings_table(&columns_snapshot);
            if should_persist_default {
                let save_result = run_blocking(|| {
                    query_service_for_visibility
                        .upsert_column_visibility(DatasetId(id), normalized.clone())
                        .map_err(|err| anyhow!(err.to_string()))
                });
                if let Err(err) = save_result {
                    *status.write() = format!("保存欄位顯示失敗：{err}");
                }
            }
            column_visibility.set(normalized);
        } else {
            column_visibility.set(BTreeMap::new());
        }
    });

    use_effect(move || {
        let dataset_count = datasets().len();
        if dataset_count == 0 {
            holdings_flags.set(BTreeMap::new());
            return;
        }
        let flags_result = run_blocking(|| {
            query_service_for_holdings_flags
                .load_holdings_flags()
                .map_err(|err| anyhow!(err.to_string()))
        });
        match flags_result {
            Ok(flags) => {
                holdings_flags.set(flags);
            }
            Err(err) => {
                *status.write() = format!("載入持股標記失敗：{err}");
            }
        }
    });

    let current_total_rows = total_rows();
    let report_snapshot = summary_report();

    let query_service_for_import = query_service.clone();
    let import_service_for_import = import_service.clone();
    let query_service_for_dataset_change = query_service.clone();
    let query_service_for_global_search = query_service.clone();
    let query_service_for_column_search = query_service.clone();
    let query_service_for_sort_select = query_service.clone();
    let query_service_for_sort_toggle = query_service.clone();
    let query_service_for_tab_switch = query_service.clone();
    let query_service_for_show_deleted = query_service.clone();
    let query_service_for_summary = query_service.clone();
    let query_service_for_visibility_update = query_service.clone();
    let query_service_for_save = query_service.clone();
    let query_service_for_save_as = query_service.clone();
    let query_service_for_import_overwrite = query_service.clone();
    let query_service_for_import_save_as = query_service.clone();
    let query_service_for_manage = query_service.clone();
    let edit_service_for_save = edit_service.clone();
    let edit_service_for_save_as = edit_service.clone();
    let edit_service_for_manage = edit_service.clone();
    let query_service_for_manage_rename = query_service_for_manage.clone();
    let query_service_for_manage_delete = query_service_for_manage.clone();
    let import_service_for_import_overwrite = import_service.clone();
    let import_service_for_import_save_as = import_service.clone();
    let grouped_datasets = build_dataset_groups(&datasets());
    let active_group =
        selected_group_key().and_then(|k| grouped_datasets.iter().find(|g| g.key == k).cloned());
    let query_service_for_dataset_change_dropdown = query_service_for_dataset_change.clone();
    let query_service_for_tab_switch_dropdown = query_service_for_tab_switch.clone();
    let dataset_options = std::iter::once(DropdownOption {
        value: NONE_OPTION_VALUE.to_string(),
        label: "(未選擇)".to_string(),
    })
    .chain(grouped_datasets.iter().map(|group| DropdownOption {
        value: group.key.clone(),
        label: group.label.clone(),
    }))
    .collect::<Vec<_>>();
    let sheet_options = active_group
        .as_ref()
        .map(|group| {
            group
                .datasets
                .iter()
                .map(|sheet| DropdownOption {
                    value: sheet.id.0.to_string(),
                    label: sheet.name.clone(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let (assets_sheet, holdings_sheet) = active_group
        .as_ref()
        .map(|group| {
            let mut assets = None;
            let mut holdings = None;
            for sheet in &group.datasets {
                match dataset_tab_kind(&sheet.name) {
                    Some(DatasetTabKind::Assets) => assets = Some(sheet.id.0),
                    Some(DatasetTabKind::Holdings) => holdings = Some(sheet.id.0),
                    None => {}
                }
            }
            (assets, holdings)
        })
        .unwrap_or((None, None));
    let current_columns = columns();
    let current_rows = rows();
    let visibility_snapshot = column_visibility();
    let (visible_columns, visible_rows) =
        apply_column_visibility(&current_columns, &current_rows, &visibility_snapshot);
    let column_options = if current_columns.is_empty() {
        Vec::new()
    } else {
        std::iter::once(DropdownOption {
            value: NONE_OPTION_VALUE.to_string(),
            label: "選擇欄位".to_string(),
        })
        .chain(
            current_columns
                .iter()
                .enumerate()
                .map(|(idx, header)| DropdownOption {
                    value: idx.to_string(),
                    label: header.clone(),
                }),
        )
        .collect::<Vec<_>>()
    };
    let sort_options = if current_columns.is_empty() {
        Vec::new()
    } else {
        std::iter::once(DropdownOption {
            value: NONE_OPTION_VALUE.to_string(),
            label: "選擇排序欄位".to_string(),
        })
        .chain(
            current_columns
                .iter()
                .enumerate()
                .map(|(idx, header)| DropdownOption {
                    value: idx.to_string(),
                    label: header.clone(),
                }),
        )
        .collect::<Vec<_>>()
    };
    let added_rows_snapshot = added_rows();
    let (_, visible_added_rows) =
        apply_column_visibility(&current_columns, &added_rows_snapshot, &visibility_snapshot);
    let datasets_snapshot = datasets();
    let staged_cells_snapshot = Arc::new(staged_cells());
    let deleted_rows_snapshot = deleted_rows();
    let selected_rows_snapshot = selected_rows();
    let editing_cell_snapshot = editing_cell();
    let column_alignments: Vec<&'static str> = visible_columns
        .iter()
        .map(|(idx, header)| column_alignment(header, &current_rows, *idx))
        .collect();
    let holdings_flags_snapshot = holdings_flags();
    let selected_dataset_name = selected_dataset_id().and_then(|id| {
        datasets_snapshot
            .iter()
            .find(|dataset| dataset.id.0 == id)
            .map(|dataset| dataset.name.clone())
    });
    let dataset_kind = selected_dataset_name.as_deref().and_then(dataset_tab_kind);
    let auto_holdings = dataset_kind
        .map(|kind| kind == DatasetTabKind::Holdings)
        .unwrap_or(false)
        || is_holdings_table(&current_columns);
    let is_assets = dataset_kind
        .map(|kind| kind == DatasetTabKind::Assets)
        .unwrap_or(false);
    let is_holdings = selected_dataset_id()
        .and_then(|id| holdings_flags_snapshot.get(&id).copied())
        .unwrap_or(auto_holdings);
    let is_editable_table = is_holdings || is_assets;
    let scroll_mode = table_scroll_mode(is_assets, is_holdings);
    let editable_columns = Arc::new(if is_holdings {
        editable_columns_for_holdings()
    } else if is_assets {
        editable_columns_for_assets(&current_columns)
    } else {
        Vec::new()
    });
    let required_columns = Arc::new(if is_holdings {
        required_columns_for_holdings()
    } else {
        Vec::new()
    });
    let base_row_count = current_rows.len();
    let has_pending_changes = !staged_cells_snapshot.is_empty()
        || !deleted_rows_snapshot.is_empty()
        || !added_rows_snapshot.is_empty();
    let edit_mode_snapshot = edit_mode();
    let editing_enabled = is_editable_table && edit_mode_snapshot;
    let current_columns_for_add = Arc::new(current_columns.clone());
    let current_columns_for_save = current_columns.clone();
    let current_rows_for_save = current_rows.clone();
    let datasets_for_save = datasets_snapshot.clone();
    let current_columns_for_save_as = current_columns_for_save.clone();
    let current_rows_for_save_as = current_rows_for_save.clone();
    let table_columns = Arc::new(visible_columns.clone());
    let table_rows = Arc::new(visible_rows.clone());
    let table_added_rows = Arc::new(visible_added_rows.clone());
    let table_rows_len = table_rows.len();
    let table_added_rows_len = table_added_rows.len();
    let total_row_count = table_rows_len + table_added_rows_len;
    let all_rows_selected = total_row_count > 0 && selected_rows_snapshot.len() == total_row_count;

    let switch_dataset = Rc::new(RefCell::new(move |next_dataset: Option<i64>| {
        let query_service_for_tab_switch = query_service_for_tab_switch_dropdown.clone();
        if is_editable_table && has_pending_changes {
            if let Some(id) = next_dataset {
                pending_action.set(Some(PendingAction::TabSwitch { dataset_id: id }));
                show_save_prompt.set(true);
            }
            return;
        }

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
        edit_mode.set(true);
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
    }));

    let switch_dataset_for_assets = switch_dataset.clone();
    let switch_dataset_for_holdings = switch_dataset.clone();
    let switch_dataset_for_sheet = switch_dataset.clone();

    let handle_import = Rc::new(RefCell::new(move || {
        let query_service_for_import = query_service_for_import.clone();
        let import_service_for_import = import_service_for_import.clone();

        if is_editable_table && has_pending_changes {
            if let Some(file_path) = FileDialog::new()
                .add_filter("Excel", &["xlsx"])
                .add_filter("CSV", &["csv"])
                .add_filter("所有檔案", &["*"])
                .pick_file()
            {
                pending_action.set(Some(PendingAction::Import(file_path)));
                show_save_prompt.set(true);
            }
            return;
        }

        if let Some(file_path) = FileDialog::new()
            .add_filter("Excel", &["xlsx"])
            .add_filter("CSV", &["csv"])
            .add_filter("所有檔案", &["*"])
            .pick_file()
        {
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
                            (
                                items.first().map(|it| it.dataset_id),
                                items.len() as i64,
                                true,
                            )
                        })
                } else {
                    import_service_for_import
                        .import_csv(&file_path)
                        .map(|item| (Some(item.dataset_id), item.row_count, false))
                }
            });

            match import_result {
                Ok((selected_id, imported_count, is_xlsx)) => {
                    match run_blocking(|| query_service_for_import.list_datasets(show_deleted())) {
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
                    }
                }
                Err(err) => {
                    *status.write() = format!("匯入失敗：{err}");
                }
            }
            *busy.write() = false;
        }
    }));

    let handle_import_for_manager = handle_import.clone();

    rsx! {
        div {
            id: "app-root",
            onclick: move |_| {
                context_menu.set(None);
                context_row.set(None);
                open_dropdown.set(None);
            },
            oncontextmenu: move |event| {
                event.prevent_default();
            },
            style: "{root_container_style_for_scroll(scroll_mode)}",

            div {
                style: "flex: 1 1 auto; min-height: 0; overflow: auto;",
                h2 { "BOM" }

                div {
                    style: "display: flex; gap: 8px; align-items: center; margin-bottom: 12px; background: #fff; padding: 8px 0;",
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            manage_dataset_id.set(selected_dataset_id());
                            let datasets_snapshot = datasets();
                            let current_name = selected_dataset_id()
                                .and_then(|id| datasets_snapshot.iter().find(|d| d.id.0 == id))
                                .map(|d| d.name.clone())
                                .unwrap_or_default();
                            manage_name_input.set(current_name);
                            show_dataset_manager.set(true);
                        },
                        "資料集管理"
                    }

                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            *busy.write() = true;
                            let Some(dataset_id) = selected_dataset_id() else {
                                *status.write() = "請先選擇資料集".to_string();
                                *busy.write() = false;
                                return;
                            };
                            let report_result = run_blocking(|| {
                                query_service_for_summary
                                    .query_page(PageQuery {
                                        dataset_id: DatasetId(dataset_id),
                                        page: 0,
                                        page_size: i64::MAX,
                                        global_search: String::new(),
                                        column_filter: None,
                                        sort: None,
                                    })
                                    .map_err(|err| anyhow!(err.to_string()))
                            });
                            match report_result {
                                Ok(page) => {
                                    let report = compute_summary_report(&page.columns, &page.rows);
                                    summary_report.set(report);
                                    show_summary_report.set(true);
                                }
                                Err(err) => {
                                    *status.write() = format!("載入總結報表失敗：{err}");
                                }
                            }
                            *busy.write() = false;
                        },
                        "總結報表"
                    }

                    span { " {status}" }
                }

                div {
                    DropdownSelect {
                        id: DropdownId::Dataset,
                        label: "資料集",
                        options: dataset_options.clone(),
                        selected: selected_group_key(),
                        open_dropdown: open_dropdown,
                        dropdown_pos: dropdown_pos,
                        on_select: move |value: String| {
                            let query_service_for_dataset_change =
                                query_service_for_dataset_change_dropdown.clone();
                            let groups = build_dataset_groups(&datasets());
                            let next_group = if value == NONE_OPTION_VALUE {
                                None::<String>
                            } else {
                                Some(value)
                            };
                            let next_dataset = next_group
                                .as_ref()
                                .and_then(|group_key| groups.iter().find(|g| &g.key == group_key))
                                .and_then(|g| choose_default_dataset_id(&g.datasets));

                            if is_editable_table && has_pending_changes {
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
                            edit_mode.set(false);
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
                    }

                    if let Some(_active_group) = active_group {
                        if assets_sheet.is_some() || holdings_sheet.is_some() {
                            div { style: "display: flex; gap: 8px; align-items: center;",
                                if let Some(assets_id) = assets_sheet {
                                    button {
                                        style: if selected_dataset_id() == Some(assets_id) {
                                            "padding: 4px 10px; border: 1px solid #4c6ef5; background: #eef4ff; border-radius: 6px;"
                                        } else {
                                            "padding: 4px 10px; border: 1px solid #bbb; background: #fff; border-radius: 6px;"
                                        },
                                        onclick: move |_| {
                                            switch_dataset_for_assets.borrow_mut()(Some(assets_id));
                                        },
                                        "資產總表"
                                    }
                                }
                                if let Some(holdings_id) = holdings_sheet {
                                    button {
                                        style: if selected_dataset_id() == Some(holdings_id) {
                                            "padding: 4px 10px; border: 1px solid #4c6ef5; background: #eef4ff; border-radius: 6px;"
                                        } else {
                                            "padding: 4px 10px; border: 1px solid #bbb; background: #fff; border-radius: 6px;"
                                        },
                                        onclick: move |_| {
                                            switch_dataset_for_holdings.borrow_mut()(Some(holdings_id));
                                        },
                                        "持股股息總表"
                                    }
                                }
                            }
                        } else {
                            DropdownSelect {
                                id: DropdownId::Sheet,
                                label: "工作表",
                                options: sheet_options.clone(),
                                selected: selected_dataset_id().map(|id| id.to_string()),
                                open_dropdown: open_dropdown,
                                dropdown_pos: dropdown_pos,
                                on_select: move |value: String| {
                                    let next_dataset = value.parse::<i64>().ok();
                                    switch_dataset_for_sheet.borrow_mut()(next_dataset);
                                }
                            }
                        }
                    }

                    if selected_dataset_id().is_some() {
                        label { "編輯模式" }
                        input {
                            r#type: "checkbox",
                            checked: edit_mode_snapshot,
                            onchange: move |event| {
                            let checked = event.value().parse::<bool>().unwrap_or(false);
                            edit_mode.set(checked);
                        }
                    }
                }
            }

            div {
                style: "display: flex; gap: 12px; align-items: center; margin: 12px 0;",
                input {
                    placeholder: "全域搜尋",
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
                    ColumnVisibilityDropdown {
                        id: DropdownId::ColumnVisibility,
                        label: "欄位顯示",
                        columns: current_columns.clone(),
                        visibility: visibility_snapshot.clone(),
                        open_dropdown: open_dropdown,
                        dropdown_pos: dropdown_pos,
                        on_toggle: move |(col_idx, visible)| {
                            let mut next_visibility = column_visibility();
                            next_visibility.insert(col_idx, visible);
                            column_visibility.set(next_visibility.clone());
                            if let Some(dataset_id) = selected_dataset_id() {
                                let result = run_blocking(|| {
                                    query_service_for_visibility_update
                                        .upsert_column_visibility(
                                            DatasetId(dataset_id),
                                            next_visibility.clone(),
                                        )
                                        .map_err(|err| anyhow!(err.to_string()))
                                });
                                if let Err(err) = result {
                                    *status.write() = format!("更新欄位顯示失敗：{err}");
                                }
                            }
                        }
                    }
                }
            }

            if !current_columns.is_empty() {
                div { style: "margin-bottom: 12px;",
                    DropdownSelect {
                        id: DropdownId::Column,
                        label: "欄位",
                        options: column_options.clone(),
                        selected: Some(
                            column_search_col()
                                .map(|idx| idx.to_string())
                                .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                        ),
                        open_dropdown: open_dropdown,
                        dropdown_pos: dropdown_pos,
                        on_select: move |value: String| {
                            if value == NONE_OPTION_VALUE {
                                column_search_col.set(None);
                                return;
                            }
                            let idx = value.parse::<i64>().ok();
                            column_search_col.set(idx);
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
                    DropdownSelect {
                        id: DropdownId::Sort,
                        label: "排序",
                        options: sort_options.clone(),
                        selected: Some(
                            sort_col()
                                .map(|idx| idx.to_string())
                                .unwrap_or_else(|| NONE_OPTION_VALUE.to_string()),
                        ),
                        open_dropdown: open_dropdown,
                        dropdown_pos: dropdown_pos,
                        on_select: move |value: String| {
                            if value == NONE_OPTION_VALUE {
                                sort_col.set(None);
                                return;
                            }
                            let idx = value.parse::<i64>().ok();
                            sort_col.set(idx);
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

            if editing_enabled {
                div { style: "margin-bottom: 12px; display: flex; gap: 8px;",
                    button {
                        disabled: busy(),
                        onclick: move |_| {
                            show_add_row.set(true);
                        },
                        "新增列"
                    }
                    button {
                        disabled: busy() || selected_rows_snapshot.is_empty(),
                        onclick: move |_| {
                            let targets = selected_rows();
                            if targets.is_empty() {
                                return;
                            }
                            for row in targets.iter() {
                                deleted_rows.write().insert(*row);
                            }
                            selected_rows.write().clear();
                            *status.write() = "已標記刪除（待儲存）".to_string();
                        },
                        "刪除選取列"
                    }
                    button {
                        disabled: busy() || selected_rows_snapshot.is_empty(),
                        onclick: move |_| {
                            let targets = selected_rows();
                            if targets.is_empty() {
                                return;
                            }
                            for row in targets.iter() {
                                deleted_rows.write().remove(row);
                            }
                            selected_rows.write().clear();
                            *status.write() = "已取消刪除".to_string();
                        },
                        "恢復選取列"
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
                                    let validation = if is_holdings {
                                        validate_required_holdings_row(&current_columns_for_add, &row)
                                    } else {
                                        Ok(())
                                    };
                                    match validation {
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

            div {
                style: "{table_container_style_for_scroll(scroll_mode)}{table_overflow_style_for_scroll(scroll_mode, table_header_stuck())} flex: 0 0 auto; min-height: calc(100vh - 72px); overflow: visible;",
                table { style: "border-collapse: collapse; width: 100%; background: #fff;",
                    thead { id: "table-head",
                        tr {
                            if editing_enabled {
                                th { style: "{table_header_cell_style()}",
                                    input {
                                        r#type: "checkbox",
                                        checked: all_rows_selected,
                                        onclick: move |_| {
                                            if all_rows_selected {
                                                selected_rows.write().clear();
                                                return;
                                            }
                                            let mut next = selected_rows.write();
                                            next.clear();
                                            for idx in 0..table_rows_len {
                                                next.insert(idx);
                                            }
                                            for idx in 0..table_added_rows_len {
                                                next.insert(base_row_count + idx);
                                            }
                                        }
                                    }
                                }
                            }
                            for (_col_idx, header) in table_columns.iter() {
                                th { style: "{table_header_cell_style()}", "{header}" }
                            }
                        }
                    }
                    tbody {
                        {table_rows.iter().enumerate().map(|(row_idx, row)| {
                        let table_columns = table_columns.clone();
                        let editable_columns = editable_columns.clone();
                        let required_columns = required_columns.clone();
                        let column_alignments = column_alignments.clone();
                        let staged_cells_for_row = staged_cells_snapshot.clone();
                        let row = row.clone();
                        let row_selected = selected_rows_snapshot.contains(&row_idx);
                        let row_deleted = deleted_rows_snapshot.contains(&row_idx);
                        let row_background = if row_selected { "#eef4ff" } else { "transparent" };
                        let row_border = if row_deleted { "#d24" } else { "transparent" };
                        let row_style =
                            format!("background: {row_background}; border-top: 2px solid {row_border}; border-bottom: 2px solid {row_border};");
                        rsx!(
                            tr {
                                style: "{row_style}",
                                if editing_enabled {
                                    td { style: "border: 1px solid #bbb; padding: 4px; text-align: center;",
                                        input {
                                            r#type: "checkbox",
                                            checked: selected_rows_snapshot.contains(&row_idx),
                                            onclick: move |_| {
                                                let mut selected = selected_rows.write();
                                                if selected.contains(&row_idx) {
                                                    selected.remove(&row_idx);
                                                } else {
                                                    selected.insert(row_idx);
                                                }
                                            }
                                        }
                                    }
                                }
                                {row.iter().enumerate().map(|(visible_idx, value)| {
                                    let value = value.clone();
                                    let (col_idx, header) = table_columns
                                        .get(visible_idx)
                                        .cloned()
                                        .unwrap_or((0, String::new()));
                                    let alignment = column_alignments
                                        .get(visible_idx)
                                        .copied()
                                        .unwrap_or("left");
                                    let required_columns_for_cell = required_columns.clone();
                                    let editable_columns_for_cell = editable_columns.clone();
                                    let cell_key = CellKey {
                                        row_idx,
                                        col_idx,
                                        column: header.clone(),
                                    };
                                    let staged_value = staged_cells_for_row
                                        .get(&cell_key)
                                        .cloned()
                                        .unwrap_or_else(|| value.clone());
                                    let formatted = format_cell_value(&header, &staged_value);
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
                                                            if required_columns_for_cell.contains(&header)
                                                                && next_value.trim().is_empty()
                                                            {
                                                                *status.write() = "必填欄位不可空白".to_string();
                                                                return;
                                                            }
                                                            let numeric_required = matches!(
                                                                header.as_str(),
                                                                "買進" | "市價" | "數量" | "期數"
                                                            );
                                                            if numeric_required
                                                                && parse_numeric_value(&next_value).is_none()
                                                            {
                                                                *status.write() =
                                                                    format!("欄位 {} 必須是數字", header);
                                                                return;
                                                            }
                                                            staged_cells
                                                                .write()
                                                                .insert(cell_key.clone(), next_value.clone());
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
                                                    if !editing_enabled {
                                                        return;
                                                    }
                                                    if editable_columns_for_cell.contains(&header) {
                                                        *editing_cell.write() = Some(cell_key.clone());
                                                        editing_value.set(staged_value.clone());
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

                        if !table_added_rows.is_empty() {
                            {table_added_rows.iter().enumerate().map(|(row_idx, row)| {
                            let table_columns = table_columns.clone();
                            let column_alignments = column_alignments.clone();
                            let row = row.clone();
                            let display_row = base_row_count + row_idx;
                            let added_selected = selected_rows_snapshot.contains(&display_row);
                            let added_deleted = deleted_rows_snapshot.contains(&display_row);
                            let added_background = if added_selected { "#eef4ff" } else { "#d9f7d9" };
                            let added_border = if added_deleted { "#d24" } else { "transparent" };
                            let row_style = format!(
                                "background: {added_background}; border-top: 2px solid {added_border}; border-bottom: 2px solid {added_border};"
                            );
                            rsx!(
                                tr {
                                    style: "{row_style}",
                                    if editing_enabled {
                                        td { style: "border: 1px solid #bbb; padding: 4px; text-align: center;",
                                            input {
                                                r#type: "checkbox",
                                                checked: selected_rows_snapshot.contains(&display_row),
                                                onclick: move |_| {
                                                    let mut selected = selected_rows.write();
                                                    if selected.contains(&display_row) {
                                                        selected.remove(&display_row);
                                                    } else {
                                                        selected.insert(display_row);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    {row.iter().enumerate().map(|(visible_idx, value)| {
                                        let value = value.clone();
                                        let (_col_idx, header) = table_columns
                                            .get(visible_idx)
                                            .cloned()
                                            .unwrap_or((0, String::new()));
                                        let alignment = column_alignments
                                            .get(visible_idx)
                                            .copied()
                                            .unwrap_or("left");
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
            }

            if let Some(dataset_id) = selected_dataset_id() {
                div { style: "display: flex; gap: 8px; align-items: center; margin-top: 12px; background: #fff; padding: 8px 0;",
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

            if show_summary_report() {
                div {
                    style: "position: fixed; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 1200;",
                    div {
                        style: "background: #fff; padding: 16px; border: 1px solid #999; min-width: 360px; max-width: 720px; max-height: 80vh; overflow: auto;",
                        div { style: "margin-bottom: 8px; font-weight: 600;", "{report_snapshot.title}" }
                        if report_snapshot.totals.is_empty() {
                            div { "沒有可計算的摘要欄位" }
                        } else {
                            div { style: "display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 6px 12px;",
                                for entry in report_snapshot.totals.clone() {
                                    div { "{entry.label}: {entry.value}" }
                                }
                            }
                        }
                        if !report_snapshot.owner_totals.is_empty() {
                            div { style: "margin-top: 12px; font-weight: 600;", "依所有權人" }
                            for owner in report_snapshot.owner_totals.clone() {
                                div { style: "margin-top: 6px; font-weight: 600;", "{owner.owner}" }
                                div { style: "display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 6px 12px;",
                                    for entry in owner.entries {
                                        div { "{entry.label}: {entry.value}" }
                                    }
                                }
                            }
                        }
                        if !report_snapshot.notes.is_empty() {
                            div { style: "margin-top: 12px; font-weight: 600;", "備註" }
                            for note in report_snapshot.notes.clone() {
                                div { "{note}" }
                            }
                        }
                        div { style: "display: flex; justify-content: flex-end; margin-top: 12px;",
                            button {
                                onclick: move |_| {
                                    show_summary_report.set(false);
                                },
                                "關閉"
                            }
                        }
                    }
                }
            }

            if show_dataset_manager() {
                div {
                    style: "position: fixed; inset: 0; background: rgba(0,0,0,0.35); display: flex; align-items: center; justify-content: center; z-index: 1200;",
                    div {
                        style: "background: #fff; padding: 16px; border: 1px solid #999; min-width: 420px; max-width: 720px; max-height: 80vh; overflow: auto;",
                        div { style: "margin-bottom: 8px; font-weight: 600;", "資料集管理" }
                        div { style: "display: flex; gap: 16px;",
                            div { style: "flex: 1;",
                                div { style: "margin-bottom: 6px; font-weight: 600;", "資料集" }
                                div { style: "border: 1px solid #ddd; max-height: 240px; overflow: auto; padding: 6px;",
                                    {datasets().iter().map(|dataset| {
                                        let dataset_id = dataset.id.0;
                                        let name = dataset.name.clone();
                                        let is_selected = manage_dataset_id() == Some(dataset_id);
                                        rsx!(
                                            label {
                                                style: "display: flex; align-items: center; gap: 8px; padding: 4px 2px; cursor: pointer;",
                                                input {
                                                    r#type: "radio",
                                                    name: "dataset-manager",
                                                    checked: is_selected,
                                                    onclick: move |_| {
                                                        manage_dataset_id.set(Some(dataset_id));
                                                        manage_name_input.set(name.clone());
                                                    }
                                                }
                                                span { "{name}" }
                                            }
                                        )
                                    })}
                                }
                            }
                            div { style: "flex: 1;",
                                div { style: "margin-bottom: 6px; font-weight: 600;", "操作" }
                                button {
                                    disabled: busy(),
                                    onclick: move |_| {
                                        handle_import_for_manager.borrow_mut()();
                                    },
                                    "匯入 CSV / XLSX"
                                }
                                div { style: "margin-top: 12px;",
                                    label { "重新命名" }
                                    input {
                                        value: manage_name_input(),
                                        oninput: move |event| {
                                            manage_name_input.set(event.value());
                                        }
                                    }
                                    button {
                                        disabled: busy(),
                                        onclick: move |_| {
                                            let Some(dataset_id) = manage_dataset_id() else {
                                                *status.write() = "請先選擇資料集".to_string();
                                                return;
                                            };
                                            let name = manage_name_input().trim().to_string();
                                            if name.is_empty() {
                                                *status.write() = "資料集名稱不可空白".to_string();
                                                return;
                                            }
                                            *busy.write() = true;
                                            let result = run_blocking(|| {
                                                query_service_for_manage_rename
                                                    .rename_dataset(DatasetId(dataset_id), name.clone())
                                                    .map_err(|err| anyhow!(err.to_string()))
                                            });
                                            if let Err(err) = result {
                                                *status.write() = format!("重新命名失敗：{err}");
                                            } else {
                                                if let Ok(available) = query_service_for_manage_rename.list_datasets(show_deleted()) {
                                                    *datasets.write() = available;
                                                }
                                                *status.write() = "已重新命名".to_string();
                                            }
                                            *busy.write() = false;
                                        },
                                        "套用" }
                                }
                                div { style: "margin-top: 12px;",
                                    button {
                                        disabled: busy(),
                                        onclick: move |_| {
                                            let Some(dataset_id) = manage_dataset_id() else {
                                                *status.write() = "請先選擇資料集".to_string();
                                                return;
                                            };
                                            let next_dataset_candidate =
                                                choose_next_dataset_after_delete(&datasets(), dataset_id);
                                            let confirm = MessageDialog::new()
                                                .set_level(MessageLevel::Warning)
                                                .set_title("永久刪除資料集")
                                                .set_description("確定要永久刪除資料集？此動作不可復原。")
                                                .set_buttons(MessageButtons::YesNo)
                                                .show();
                                            if confirm != MessageDialogResult::Yes {
                                                return;
                                            }
                                            *busy.write() = true;
                                            let result = run_blocking(|| {
                                                edit_service_for_manage
                                                    .hard_delete_dataset(DatasetId(dataset_id))
                                                    .map_err(|err| anyhow!(err.to_string()))
                                            });
                                            if let Err(err) = result {
                                                *status.write() = format!("刪除資料集失敗：{err}");
                                            } else if let Ok(available) = query_service_for_manage_delete.list_datasets(show_deleted()) {
                                                let groups = build_dataset_groups(&available);
                                                *datasets.write() = available;
                                                let next_dataset = next_dataset_candidate
                                                    .and_then(|id| {
                                                        groups
                                                            .iter()
                                                            .flat_map(|g| g.datasets.iter())
                                                            .find(|d| d.id.0 == id)
                                                            .map(|d| d.id.0)
                                                    })
                                                    .or_else(|| {
                                                        selected_group_key()
                                                            .and_then(|key| groups.iter().find(|g| g.key == key))
                                                            .or_else(|| groups.first())
                                                            .and_then(|g| choose_default_dataset_id(&g.datasets))
                                                    });
                                                *selected_group_key.write() = groups
                                                    .iter()
                                                    .find(|g| g.datasets.iter().any(|d| d.id.0 == next_dataset.unwrap_or(-1)))
                                                    .map(|g| g.key.clone());
                                                *selected_dataset_id.write() = next_dataset;
                                                *page.write() = 0;
                                                match reload_page_data_usecase(
                                                    &query_service_for_manage_delete,
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
                                                        *status.write() = format!("載入資料集失敗：{err}");
                                                    }
                                                }
                                                manage_dataset_id.set(next_dataset);
                                                *status.write() = "已永久刪除資料集".to_string();
                                            }
                                            *busy.write() = false;
                                        },
                                        "刪除" }
                                }
                            }
                        }
                        div { style: "display: flex; justify-content: flex-end; margin-top: 12px;",
                            button {
                                onclick: move |_| {
                                    show_dataset_manager.set(false);
                                },
                                "關閉"
                            }
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
        }
    }
}

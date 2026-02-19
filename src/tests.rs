use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

use crate::domain::entities::edit::CellKey;
use crate::infra::import::csv::import_csv_to_sqlite;
use crate::infra::import::xlsx::import_xlsx_selected_sheets_to_sqlite;
use crate::infra::sqlite::queries::{
    apply_changes_to_dataset, build_updated_rows, create_dataset_from_rows, list_datasets,
    load_column_visibility, load_holdings_flags, purge_dataset, query_page, rename_dataset,
    soft_delete_dataset, upsert_column_visibility, upsert_holdings_flag,
};
use crate::infra::sqlite::repo::SqliteRepo;
use crate::infra::sqlite::schema::init_db;
use crate::usecase::services::edit_service::EditService;
use crate::*;

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
fn column_visibility_persists_per_dataset() {
    let temp_dir = unique_test_dir("column-visibility");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");

    init_db(&db_path).expect("init_db should succeed");

    let dataset_id = create_dataset_from_rows(
        &db_path,
        "sample",
        "sample.csv",
        &["A".to_string(), "B".to_string(), "C".to_string()],
        &[vec!["1".to_string(), "2".to_string(), "3".to_string()]],
    )
    .expect("dataset should be created");

    let mut visibility = BTreeMap::new();
    visibility.insert(0, true);
    visibility.insert(1, false);
    visibility.insert(2, true);

    upsert_column_visibility(&db_path, dataset_id, &visibility)
        .expect("should store column visibility");

    let loaded =
        load_column_visibility(&db_path, dataset_id).expect("should load column visibility");

    assert_eq!(
        loaded, visibility,
        "loaded visibility should match saved data"
    );

    fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
}

#[test]
fn holdings_flag_persists_per_dataset() {
    let temp_dir = unique_test_dir("holdings-flag");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");

    init_db(&db_path).expect("init_db should succeed");

    let dataset_id = create_dataset_from_rows(
        &db_path,
        "sample",
        "sample.csv",
        &["A".to_string(), "B".to_string(), "C".to_string()],
        &[vec!["1".to_string(), "2".to_string(), "3".to_string()]],
    )
    .expect("dataset should be created");

    upsert_holdings_flag(&db_path, dataset_id, true).expect("should store holdings flag");

    let flags = load_holdings_flags(&db_path).expect("should load holdings flags");

    assert_eq!(flags.get(&dataset_id), Some(&true));

    fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
}

#[test]
fn rename_dataset_updates_name() {
    let temp_dir = unique_test_dir("rename-dataset");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");

    init_db(&db_path).expect("init_db should succeed");

    let dataset_id = create_dataset_from_rows(
        &db_path,
        "sample",
        "sample.csv",
        &["A".to_string()],
        &[vec!["1".to_string()]],
    )
    .expect("dataset should be created");

    rename_dataset(&db_path, dataset_id, "renamed").expect("should rename dataset");

    let datasets = list_datasets(&db_path, false).expect("should list datasets");
    let name = datasets
        .iter()
        .find(|dataset| dataset.id.0 == dataset_id)
        .map(|dataset| dataset.name.clone());

    assert_eq!(name, Some("renamed".to_string()));

    fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
}

#[test]
fn apply_column_visibility_filters_columns_and_rows() {
    let columns = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let rows = vec![vec!["1".to_string(), "2".to_string(), "3".to_string()]];

    let mut visibility = BTreeMap::new();
    visibility.insert(0, true);
    visibility.insert(1, false);
    visibility.insert(2, true);

    let (visible_columns, visible_rows) = apply_column_visibility(&columns, &rows, &visibility);

    let visible_names: Vec<String> = visible_columns
        .iter()
        .map(|(_idx, name)| name.clone())
        .collect();
    assert_eq!(visible_names, vec!["A".to_string(), "C".to_string()]);
    assert_eq!(visible_rows, vec![vec!["1".to_string(), "3".to_string()]]);
}

#[test]
fn apply_column_visibility_defaults_to_all_when_empty() {
    let columns = vec!["A".to_string(), "B".to_string()];
    let rows = vec![vec!["1".to_string(), "2".to_string()]];
    let visibility = BTreeMap::new();

    let (visible_columns, visible_rows) = apply_column_visibility(&columns, &rows, &visibility);

    let visible_names: Vec<String> = visible_columns
        .iter()
        .map(|(_idx, name)| name.clone())
        .collect();
    assert_eq!(visible_names, vec!["A".to_string(), "B".to_string()]);
    assert_eq!(visible_rows, rows);
}

#[test]
fn default_holdings_visibility_is_required_only() {
    let mut headers = required_columns_for_holdings();
    headers.push("總成本".to_string());
    headers.push("備註".to_string());
    let visibility = BTreeMap::new();

    let normalized = normalize_column_visibility(&headers, &visibility);

    for (idx, header) in headers.iter().enumerate() {
        let expected = required_columns_for_holdings().iter().any(|c| c == header);
        assert_eq!(normalized.get(&(idx as i64)), Some(&expected));
    }

    let rows = vec![headers.clone()];
    let (visible_columns, _visible_rows) = apply_column_visibility(&headers, &rows, &normalized);
    let visible_names: Vec<String> = visible_columns
        .iter()
        .map(|(_idx, name)| name.clone())
        .collect();
    assert_eq!(visible_names, required_columns_for_holdings());
}

#[test]
fn normalize_column_visibility_fills_missing_and_keeps_existing() {
    let headers = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let mut visibility = BTreeMap::new();
    visibility.insert(0, false);
    visibility.insert(2, true);

    let normalized = normalize_column_visibility(&headers, &visibility);

    assert_eq!(normalized.get(&0), Some(&false));
    assert_eq!(normalized.get(&1), Some(&true));
    assert_eq!(normalized.get(&2), Some(&true));
}

#[test]
fn sticky_header_styles_include_positioning() {
    let style = table_header_cell_style();

    assert!(style.contains("position: sticky"));
    assert!(style.contains("top: 0"));
    assert!(style.contains("z-index"));
}

#[test]
fn table_container_style_allows_scroll() {
    let style = table_container_style();

    assert!(style.contains("overflow: auto"));
    assert!(style.contains("flex: 1"));
}

#[test]
fn root_container_style_uses_viewport_height_and_flex() {
    let page_style = root_container_style_for_scroll(TableScrollMode::PageThenTable);
    let table_only_style = root_container_style_for_scroll(TableScrollMode::TableOnly);

    for style in [page_style, table_only_style] {
        assert!(style.contains("height: 100vh"));
        assert!(style.contains("display: flex"));
        assert!(style.contains("flex-direction: column"));
        assert!(style.contains("overflow: hidden"));
    }
}

#[test]
fn table_container_style_for_scroll_is_flexible() {
    let page_style = table_container_style_for_scroll(TableScrollMode::PageThenTable);
    let table_only_style = table_container_style_for_scroll(TableScrollMode::TableOnly);

    for style in [page_style, table_only_style] {
        assert!(style.contains("flex: 1"));
        assert!(style.contains("min-height: 0"));
        assert!(style.contains("overflow: auto"));
    }
}

#[test]
fn table_scroll_mode_defaults_to_table_only() {
    assert_eq!(table_scroll_mode(true, false), TableScrollMode::TableOnly);
    assert_eq!(table_scroll_mode(false, true), TableScrollMode::TableOnly);
    assert_eq!(table_scroll_mode(false, false), TableScrollMode::TableOnly);
}

#[test]
fn table_overflow_style_is_empty() {
    assert_eq!(
        table_overflow_style_for_scroll(TableScrollMode::PageThenTable, false),
        ""
    );
    assert_eq!(
        table_overflow_style_for_scroll(TableScrollMode::PageThenTable, true),
        ""
    );
    assert_eq!(
        table_overflow_style_for_scroll(TableScrollMode::TableOnly, true),
        ""
    );
}

#[test]
fn read_xlsx_summary_report_reads_bottom_rows() {
    let xlsx_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("BOM_test.xlsx");

    let report = read_xlsx_summary_report(&xlsx_path).expect("should read xlsx summary");

    let deposit = report
        .interest_rows
        .iter()
        .find(|row| row.label == "定存資金")
        .expect("should have deposit summary");
    assert_eq!(deposit.annual, "53840");
    assert_eq!(deposit.monthly, "4486.666667");
    assert_eq!(deposit.yield_rate, "0.016");

    assert_eq!(report.dividend_total.as_deref(), Some("23719.85119"));

    let alex = report
        .owner_dividends
        .iter()
        .find(|row| row.owner == "Alex")
        .expect("should have Alex summary");
    assert_eq!(alex.monthly, "11417.88194");
    assert_eq!(alex.monthly_with_pension.as_deref(), Some("32004.98194"));
}

#[test]
#[cfg(target_os = "linux")]
fn linux_menu_disabled_in_config() {
    assert!(linux_menu_disabled());
}

#[test]
fn dataset_tab_kind_detects_assets_and_holdings() {
    assert_eq!(dataset_tab_kind("資產總表"), Some(DatasetTabKind::Assets));
    assert_eq!(
        dataset_tab_kind("持股股息總表"),
        Some(DatasetTabKind::Holdings)
    );
    assert_eq!(dataset_tab_kind("其他"), None);
}

#[test]
fn choose_default_dataset_id_prefers_assets() {
    let datasets = vec![
        DatasetMeta {
            id: 1.into(),
            name: "持股股息總表".to_string(),
            row_count: 0,
            source_path: "x.xlsx#持股".to_string(),
            deleted_at: None,
        },
        DatasetMeta {
            id: 2.into(),
            name: "資產總表".to_string(),
            row_count: 0,
            source_path: "x.xlsx#資產".to_string(),
            deleted_at: None,
        },
    ];

    assert_eq!(choose_default_dataset_id(&datasets), Some(2));
}

#[test]
fn choose_default_dataset_id_falls_back_to_first() {
    let datasets = vec![DatasetMeta {
        id: 5.into(),
        name: "其他".to_string(),
        row_count: 0,
        source_path: "x.csv".to_string(),
        deleted_at: None,
    }];

    assert_eq!(choose_default_dataset_id(&datasets), Some(5));
}

#[test]
fn choose_next_dataset_after_delete_prefers_next_then_previous() {
    let datasets = vec![
        DatasetMeta {
            id: 3.into(),
            name: "第三".to_string(),
            row_count: 0,
            source_path: "x.csv".to_string(),
            deleted_at: None,
        },
        DatasetMeta {
            id: 2.into(),
            name: "第二".to_string(),
            row_count: 0,
            source_path: "x.csv".to_string(),
            deleted_at: None,
        },
        DatasetMeta {
            id: 1.into(),
            name: "第一".to_string(),
            row_count: 0,
            source_path: "x.csv".to_string(),
            deleted_at: None,
        },
    ];

    assert_eq!(choose_next_dataset_after_delete(&datasets, 2), Some(1));
    assert_eq!(choose_next_dataset_after_delete(&datasets, 1), Some(2));
    assert_eq!(choose_next_dataset_after_delete(&datasets, 3), Some(2));
}

#[test]
fn summary_report_aggregates_totals_and_owners() {
    let headers = vec![
        "所有權人".to_string(),
        "總成本".to_string(),
        "估計配息".to_string(),
        "今年度累積".to_string(),
    ];
    let rows = vec![
        vec![
            "Alex".to_string(),
            "100".to_string(),
            "10".to_string(),
            "5".to_string(),
        ],
        vec![
            "Paul".to_string(),
            "200".to_string(),
            "20".to_string(),
            "15".to_string(),
        ],
    ];

    let report = compute_summary_report(&headers, &rows);

    let total_cost = report
        .totals
        .iter()
        .find(|entry| entry.label == "總成本")
        .map(|entry| entry.value.clone());
    assert_eq!(total_cost, Some("300".to_string()));

    let owner_alex = report
        .owner_totals
        .iter()
        .find(|owner| owner.owner == "Alex")
        .cloned()
        .expect("Alex summary should exist");
    let alex_estimated = owner_alex
        .entries
        .iter()
        .find(|entry| entry.label == "估計配息")
        .map(|entry| entry.value.clone());
    assert_eq!(alex_estimated, Some("10".to_string()));
}

#[test]
fn summary_report_owner_totals_include_holdings_fields() {
    let headers = vec![
        "所有權人".to_string(),
        "數量".to_string(),
        "總成本".to_string(),
        "淨值".to_string(),
        "估計配息".to_string(),
    ];
    let rows = vec![
        vec![
            "Alex".to_string(),
            "2".to_string(),
            "100".to_string(),
            "110".to_string(),
            "5".to_string(),
        ],
        vec![
            "Alex".to_string(),
            "3".to_string(),
            "200".to_string(),
            "220".to_string(),
            "10".to_string(),
        ],
    ];

    let report = compute_summary_report(&headers, &rows);

    let owner_alex = report
        .owner_totals
        .iter()
        .find(|owner| owner.owner == "Alex")
        .cloned()
        .expect("Alex summary should exist");

    let qty = owner_alex
        .entries
        .iter()
        .find(|entry| entry.label == "數量")
        .map(|entry| entry.value.clone());
    let cost = owner_alex
        .entries
        .iter()
        .find(|entry| entry.label == "總成本")
        .map(|entry| entry.value.clone());
    let net = owner_alex
        .entries
        .iter()
        .find(|entry| entry.label == "淨值")
        .map(|entry| entry.value.clone());
    let dividend = owner_alex
        .entries
        .iter()
        .find(|entry| entry.label == "估計配息")
        .map(|entry| entry.value.clone());

    assert_eq!(qty, Some("5".to_string()));
    assert_eq!(cost, Some("300".to_string()));
    assert_eq!(net, Some("330".to_string()));
    assert_eq!(dividend, Some("15".to_string()));
}

#[test]
fn assets_summary_report_aggregates_cost_and_net() {
    let headers = vec![
        "資產形式".to_string(),
        "投入金額".to_string(),
        "目前淨值".to_string(),
    ];
    let rows = vec![
        vec!["活存".to_string(), "100".to_string(), "110".to_string()],
        vec!["定存".to_string(), "200".to_string(), "190".to_string()],
    ];

    let report = compute_summary_report(&headers, &rows);

    let total_cost = report
        .totals
        .iter()
        .find(|entry| entry.label == "合計-投入金額")
        .map(|entry| entry.value.clone());
    let total_net = report
        .totals
        .iter()
        .find(|entry| entry.label == "合計-目前淨值")
        .map(|entry| entry.value.clone());
    let total_profit = report
        .totals
        .iter()
        .find(|entry| entry.label == "合計-損益")
        .map(|entry| entry.value.clone());

    assert_eq!(total_cost, Some("300".to_string()));
    assert_eq!(total_net, Some("300".to_string()));
    assert_eq!(total_profit, Some("0".to_string()));
}

#[test]
fn assets_summary_report_reads_interest_rows() {
    let headers = vec![
        "資產形式".to_string(),
        "欄位B".to_string(),
        "欄位C".to_string(),
        "欄位D".to_string(),
        "投入金額".to_string(),
        "目前淨值".to_string(),
    ];
    let rows = vec![vec![
        "定存資金".to_string(),
        "1200".to_string(),
        "100".to_string(),
        "0.1".to_string(),
        "0".to_string(),
        "0".to_string(),
    ]];

    let report = compute_summary_report(&headers, &rows);

    let annual = report
        .totals
        .iter()
        .find(|entry| entry.label == "定存資金-年化")
        .map(|entry| entry.value.clone());
    let monthly = report
        .totals
        .iter()
        .find(|entry| entry.label == "定存資金-月化")
        .map(|entry| entry.value.clone());

    assert_eq!(annual, Some("1200".to_string()));
    assert_eq!(monthly, Some("100".to_string()));
    assert!(!report
        .totals
        .iter()
        .any(|entry| entry.label == "定存資金-殖利率"));
}

#[test]
fn assets_summary_report_reads_interest_rows_from_data() {
    let headers = vec![
        "資產形式".to_string(),
        "欄位B".to_string(),
        "欄位C".to_string(),
        "利率".to_string(),
        "欄位E".to_string(),
        "欄位F".to_string(),
        "欄位G".to_string(),
        "目前淨值".to_string(),
        "估計配息".to_string(),
    ];
    let rows = vec![
        vec![
            "定存".to_string(),
            "".to_string(),
            "".to_string(),
            "0.02".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "100000".to_string(),
            "".to_string(),
        ],
        vec![
            "定存".to_string(),
            "".to_string(),
            "".to_string(),
            "0.02".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "50000".to_string(),
            "".to_string(),
        ],
        vec![
            "投資".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "0".to_string(),
            "1200".to_string(),
        ],
        vec![
            "投資".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "0".to_string(),
            "600".to_string(),
        ],
    ];

    let report = compute_summary_report(&headers, &rows);

    let value_for = |label: &str| {
        report
            .totals
            .iter()
            .find(|entry| entry.label == label)
            .map(|entry| entry.value.clone())
    };

    assert_eq!(value_for("定存資金-年化"), Some("3000".to_string()));
    assert_eq!(value_for("定存資金-月化"), Some("250".to_string()));
    assert_eq!(value_for("股債息(平均)-年化"), Some("1800".to_string()));
    assert_eq!(value_for("股債息(平均)-月化"), Some("150".to_string()));
    assert_eq!(value_for("合計(平均)-年化"), Some("4800".to_string()));
    assert_eq!(value_for("合計(平均)-月化"), Some("400".to_string()));
}

#[test]
fn assets_summary_report_prefers_derived_interest_over_summary_rows() {
    let headers = vec![
        "資產形式".to_string(),
        "欄位B".to_string(),
        "欄位C".to_string(),
        "利率".to_string(),
        "欄位E".to_string(),
        "欄位F".to_string(),
        "欄位G".to_string(),
        "目前淨值".to_string(),
        "估計配息".to_string(),
    ];
    let rows = vec![
        vec![
            "定存".to_string(),
            "".to_string(),
            "".to_string(),
            "0.02".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "100000".to_string(),
            "".to_string(),
        ],
        vec![
            "投資".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "0".to_string(),
            "1200".to_string(),
        ],
        vec![
            "定存資金".to_string(),
            "9999".to_string(),
            "999".to_string(),
            "0.1".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
        ],
        vec![
            "股債息(平均)".to_string(),
            "8888".to_string(),
            "888".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
        ],
        vec![
            "合計(平均)".to_string(),
            "7777".to_string(),
            "777".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
            "0".to_string(),
        ],
    ];

    let report = compute_summary_report(&headers, &rows);

    let value_for = |label: &str| {
        report
            .totals
            .iter()
            .find(|entry| entry.label == label)
            .map(|entry| entry.value.clone())
    };

    assert_eq!(value_for("定存資金-年化"), Some("2000".to_string()));
    assert_eq!(value_for("定存資金-月化"), Some("166.666667".to_string()));
    assert_eq!(value_for("股債息(平均)-年化"), Some("1200".to_string()));
    assert_eq!(value_for("股債息(平均)-月化"), Some("100".to_string()));
    assert_eq!(value_for("合計(平均)-年化"), Some("3200".to_string()));
    assert_eq!(value_for("合計(平均)-月化"), Some("266.666667".to_string()));
}

#[test]
fn import_creates_dataset_with_headers_and_rows() {
    let temp_dir = unique_test_dir("import-db");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");
    let csv_path = temp_dir.join("people.csv");
    fs::write(&csv_path, "name,city\nAlice,Paris\nBob,Tokyo\n").expect("should write csv fixture");

    init_db(&db_path).expect("init_db should succeed");
    let import_result = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");

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
    assert!(names.iter().any(|n| n.contains("持股股息總表")));

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
             WHERE d.name = '持股股息總表'
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
             WHERE d.name = '持股股息總表'
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

#[test]
fn purge_dataset_removes_related_records_and_flags() {
    let temp_dir = unique_test_dir("purge-dataset-flags");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");

    let dataset_id = create_dataset_from_rows(
        &db_path,
        "sample",
        "sample.csv",
        &["A".to_string(), "B".to_string()],
        &[vec!["1".to_string(), "2".to_string()]],
    )
    .expect("dataset should be created");

    let mut visibility = BTreeMap::new();
    visibility.insert(0, true);
    upsert_column_visibility(&db_path, dataset_id, &visibility)
        .expect("should store column visibility");
    upsert_holdings_flag(&db_path, dataset_id, true).expect("should store holdings flag");

    purge_dataset(&db_path, dataset_id).expect("purge should succeed");

    let conn = Connection::open(&db_path).expect("should open sqlite db");
    let visibility_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM column_visibility WHERE dataset_id=?1",
            [dataset_id],
            |row| row.get(0),
        )
        .expect("column visibility count query should succeed");
    let flag_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dataset_flag WHERE dataset_id=?1",
            [dataset_id],
            |row| row.get(0),
        )
        .expect("dataset flag count query should succeed");

    assert_eq!(visibility_count, 0);
    assert_eq!(flag_count, 0);

    fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
}

#[test]
fn edit_service_hard_delete_removes_dataset() {
    let temp_dir = unique_test_dir("hard-delete");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");
    let csv_path = temp_dir.join("sample.csv");
    fs::write(&csv_path, "name\nAlice\n").expect("should write csv fixture");

    let imported = import_csv_to_sqlite(&db_path, &csv_path).expect("import should succeed");

    let repo = SqliteRepo {
        db_path: db_path.clone(),
    };
    let edit_service = EditService::new(std::sync::Arc::new(repo));
    edit_service
        .hard_delete_dataset(imported.dataset_id.into())
        .expect("hard delete should succeed");

    let visible = list_datasets(&db_path, false).expect("list visible should succeed");
    let with_deleted = list_datasets(&db_path, true).expect("list with deleted should succeed");

    assert!(visible.is_empty(), "hard deleted dataset should be gone");
    assert!(
        with_deleted.is_empty(),
        "hard deleted dataset should be gone"
    );

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
fn assets_editable_columns_include_all_headers() {
    let headers = vec!["欄位A".to_string(), "欄位B".to_string()];
    let editable = editable_columns_for_assets(&headers);
    assert_eq!(editable, headers);
}

#[test]
fn build_updated_rows_applies_staged_values() {
    let columns = vec!["所有權人".to_string(), "名稱".to_string()];
    let rows = vec![vec!["王小明".to_string(), "舊名稱".to_string()]];
    let mut staged = HashMap::new();
    staged.insert(
        CellKey {
            row_idx: 0,
            col_idx: 1,
            column: "名稱".to_string(),
        },
        "新名稱".to_string(),
    );
    let deleted = BTreeSet::new();
    let added = Vec::new();

    let updated = build_updated_rows(&columns, &rows, &staged, &deleted, &added);

    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0][1], "新名稱");
}

#[test]
fn validate_required_holdings_row_rejects_empty_required_field() {
    let headers = required_columns_for_holdings();
    let mut row = vec!["X".to_string(); headers.len()];
    let owner_idx = headers
        .iter()
        .position(|h| h == "所有權人")
        .expect("should have required header");
    row[owner_idx] = String::new();

    let result = validate_required_holdings_row(&headers, &row);

    assert!(result.is_err());
}

#[test]
fn build_updated_rows_skips_deleted_rows() {
    let columns = vec!["所有權人".to_string()];
    let rows = vec![
        vec!["A".to_string()],
        vec!["B".to_string()],
        vec!["C".to_string()],
    ];
    let staged = HashMap::new();
    let mut deleted = BTreeSet::new();
    deleted.insert(0);
    deleted.insert(2);
    let added = Vec::new();

    let updated = build_updated_rows(&columns, &rows, &staged, &deleted, &added);

    assert_eq!(updated, vec![vec!["B".to_string()]]);
}

#[test]
fn build_updated_rows_appends_added_rows() {
    let columns = vec!["所有權人".to_string(), "名稱".to_string()];
    let rows = vec![vec!["A".to_string(), "X".to_string()]];
    let staged = HashMap::new();
    let deleted = BTreeSet::new();
    let added = vec![vec!["B".to_string(), "Y".to_string()]];

    let updated = build_updated_rows(&columns, &rows, &staged, &deleted, &added);

    assert_eq!(updated.len(), 2);
    assert_eq!(updated[1], vec!["B".to_string(), "Y".to_string()]);
}

#[test]
fn apply_changes_to_dataset_updates_rows() {
    let temp_dir = unique_test_dir("apply-changes");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");
    let csv_path = temp_dir.join("people.csv");
    fs::write(&csv_path, "name,city\nAlice,Paris\nBob,Tokyo\n").expect("should write csv fixture");

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

#[test]
fn create_dataset_from_rows_inserts_dataset() {
    let temp_dir = unique_test_dir("create-dataset");
    fs::create_dir_all(&temp_dir).expect("should create temp dir");
    let db_path = temp_dir.join("app.sqlite");
    init_db(&db_path).expect("init_db should succeed");

    let columns = vec!["col1".to_string(), "col2".to_string()];
    let rows = vec![vec!["a".to_string(), "b".to_string()]];
    let dataset_id = create_dataset_from_rows(&db_path, "backup", "test#backup", &columns, &rows)
        .expect("create dataset should succeed");

    let conn = Connection::open(&db_path).expect("should open sqlite db");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dataset WHERE id = ?1 AND name = ?2",
            params![dataset_id, "backup"],
            |row| row.get(0),
        )
        .expect("dataset count query should succeed");
    assert_eq!(count, 1);

    let column_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM column_name WHERE dataset_id = ?1",
            params![dataset_id],
            |row| row.get(0),
        )
        .expect("column count query should succeed");
    assert_eq!(column_count, 2);

    let row_count: i64 = conn
        .query_row(
            "SELECT row_count FROM dataset WHERE id = ?1",
            params![dataset_id],
            |row| row.get(0),
        )
        .expect("row count query should succeed");
    assert_eq!(row_count, 1);

    fs::remove_dir_all(&temp_dir).expect("should cleanup temp dir");
}

use std::collections::{BTreeMap, HashMap, HashSet};

use dioxus::prelude::*;
use rust_decimal::{prelude::FromPrimitive, prelude::ToPrimitive, Decimal};

use crate::account_asset::upsert_manual_account_asset;
use crate::account_asset::{
    asset_type_label, is_foreign_currency_asset, validate_account_asset_input, AccountAssetInput,
};
use crate::db::{
    load_account_assets, load_applicable_exchange_rate, load_dashboard_summary,
    load_dividend_receipt_form_options, load_dividend_receipts, load_holding_metrics,
    load_legacy_dividends, load_recent_exchange_rates,
};
use crate::dividend_receipt::{
    delete_manual_dividend_receipt, update_manual_dividend_receipt, DividendReceiptDeleteInput,
    DividendReceiptUpdateInput,
};
use crate::dividend_receipt::{insert_manual_dividend_receipt, DividendReceiptInput};
use crate::exchange_rate::{upsert_manual_exchange_rate, ExchangeRateInput};
use crate::format::{decimal, money, percent};
use crate::holding::{
    save_current_holding_state, save_dividend_assumption, CurrentHoldingStateInput,
    DividendAssumptionInput,
};
use crate::master_data::{
    create_manual_account, create_manual_instrument, load_institution_options, AccountCreateInput,
    InstitutionOption, InstrumentCreateInput,
};
use crate::models::{
    AccountAsset, DashboardSummary, DividendReceiptAccountOption, DividendReceiptFormOptions,
    DividendReceiptInstrumentOption, DividendReceiptRow, ExchangeRateRow, HoldingMetric,
    LegacyDividendData, LegacyDividendMonthlyRow, LegacyDividendSummaryRow,
};
use crate::price::{upsert_manual_prices_batch, BatchPriceInput, BatchPriceRowInput};
use crate::ui_preference::{
    parse_visible_columns, persist_preference, preference_value, serialize_visible_columns,
    valid_option, valid_sort, UiPreferences, ACCOUNTS_ASSET_TYPE, ACCOUNTS_CURRENCY,
    ACCOUNTS_INSTITUTION, ACCOUNTS_OWNER, ACCOUNTS_SEARCH, ACCOUNTS_SORT, HOLDINGS_ASSET_CLASS,
    HOLDINGS_OWNER, HOLDINGS_REGION, HOLDINGS_SEARCH, HOLDINGS_SHOW_CLOSED, HOLDINGS_SORT,
    HOLDINGS_TYPE, HOLDINGS_VISIBLE_COLUMNS, LEGACY_DIVIDENDS_INSTRUMENT, LEGACY_DIVIDENDS_OWNER,
    LEGACY_DIVIDENDS_PERIOD, LEGACY_DIVIDENDS_SEARCH, LEGACY_DIVIDENDS_SORT, QUICK_PRICE_CURRENCY,
    QUICK_PRICE_DATE, QUICK_PRICE_SEARCH, QUICK_PRICE_SORT,
};

#[component]
pub fn DashboardPage() -> Element {
    let data_version = use_context::<Signal<u64>>();
    let summary = use_resource(move || async move {
        let _ = data_version();
        load_dashboard_summary()
    });

    rsx! {
        PageHeader {
            title: "資產總覽".to_string(),
            description: "彙總最新帳戶資產、投資市值與預估配息。".to_string(),
        }

        match summary() {
            None => rsx! { StatusCard { text: "載入資產總覽中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取資產總覽失敗：{error}") } },
            Some(Ok(summary)) => rsx! { DashboardCards { summary } },
        }
    }
}

#[component]
pub fn AccountsPage() -> Element {
    let data_version = use_context::<Signal<u64>>();
    let account_assets = use_resource(move || async move {
        let _ = data_version();
        load_account_assets()
    });

    rsx! {
        PageHeader {
            title: "帳戶資產".to_string(),
            description: "由 v_account_asset_value 讀取最新帳戶資產快照與台幣換算值。".to_string(),
        }

        match account_assets() {
            None => rsx! { StatusCard { text: "載入帳戶資產中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取帳戶資產失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有帳戶資產資料。".to_string() } },
            Some(Ok(rows)) => rsx! { AccountAssetsTable { rows } },
        }
    }
}

#[component]
pub fn HoldingsPage() -> Element {
    let data_version = use_context::<Signal<u64>>();
    let holdings = use_resource(move || async move {
        let _ = data_version();
        load_holding_metrics()
    });

    rsx! {
        PageHeader {
            title: "持股明細".to_string(),
            description: "由 v_holding_metrics 讀取最新持股、成本、市值、損益與預估配息。".to_string(),
        }

        match holdings() {
            None => rsx! { StatusCard { text: "載入持股資料中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取持股資料失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有持股資料。".to_string() } },
            Some(Ok(rows)) => rsx! { HoldingsTable { rows } },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QuickPriceUpdateRow {
    instrument_id: i64,
    symbol: String,
    instrument_name: String,
    currency_code: String,
    latest_price: Option<f64>,
    latest_price_date: Option<String>,
    holding_account_count: usize,
}

#[component]
pub fn QuickPriceUpdatePage() -> Element {
    let data_version = use_context::<Signal<u64>>();
    let preferences = use_context::<UiPreferences>();
    let holdings = use_resource(move || async move {
        let _ = data_version();
        load_holding_metrics()
    });
    let today = chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let mut price_date = use_signal(move || {
        let value = preference_value(&preferences(), QUICK_PRICE_DATE);
        valid_price_date(&value, &today)
    });
    let mut search = use_signal(move || preference_value(&preferences(), QUICK_PRICE_SEARCH));
    let mut currency_filter =
        use_signal(move || preference_value(&preferences(), QUICK_PRICE_CURRENCY));
    let mut sort_by = use_signal(move || {
        valid_sort(
            &preference_value(&preferences(), QUICK_PRICE_SORT),
            &["symbol", "name", "price"],
            "symbol",
        )
    });
    let mut draft_prices = use_signal(HashMap::<i64, String>::new);
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut success_message = use_signal(String::new);

    use_effect(move || {
        let value = price_date();
        if chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d").is_ok() {
            persist_preference(preferences, QUICK_PRICE_DATE, value);
        }
    });
    use_effect(move || persist_preference(preferences, QUICK_PRICE_SEARCH, search()));
    use_effect(move || {
        let selected = currency_filter();
        let value = match holdings() {
            Some(Ok(rows)) => valid_option(
                &selected,
                &unique_strings(
                    build_quick_price_update_rows(&rows)
                        .iter()
                        .map(|row| row.currency_code.as_str()),
                ),
                "",
            ),
            _ => selected,
        };
        persist_preference(preferences, QUICK_PRICE_CURRENCY, value)
    });
    use_effect(move || persist_preference(preferences, QUICK_PRICE_SORT, sort_by()));
    use_effect(move || {
        if let Some(Ok(rows)) = holdings() {
            let selected = currency_filter();
            let valid = valid_option(
                &selected,
                &unique_strings(
                    build_quick_price_update_rows(&rows)
                        .iter()
                        .map(|row| row.currency_code.as_str()),
                ),
                "",
            );
            if selected != valid {
                currency_filter.set(valid);
            }
        }
    });

    rsx! {
        PageHeader {
            title: "快速市價更新".to_string(),
            description: "以商品層級批次更新市場價格；空白列跳過，整批驗證通過後一次寫入。".to_string(),
        }

        match holdings() {
            None => rsx! { StatusCard { text: "載入可更新商品中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取可更新商品失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有可更新市價的持股資料。".to_string() } },
            Some(Ok(rows)) => {
                let price_rows = build_quick_price_update_rows(&rows);
                let currency_options = unique_strings(price_rows.iter().map(|row| row.currency_code.as_str()));
                let draft_values = draft_prices();
                let save_rows = price_rows.clone();
                let visible_rows = filter_quick_price_rows(
                    &price_rows,
                    &search(),
                    &currency_filter(),
                    &sort_by(),
                );
                let display_rows = visible_rows
                    .iter()
                    .map(|row| {
                        (
                            row.clone(),
                            draft_values
                                .get(&row.instrument_id)
                                .cloned()
                                .unwrap_or_default(),
                        )
                    })
                    .collect::<Vec<_>>();

                rsx! {
                    section { class: "card table-card",
                        if !success_message().is_empty() {
                            div { class: "status-message success", "{success_message}" }
                        }
                        if !error_message().is_empty() {
                            div { class: "status-message error quick-price-error", "{error_message}" }
                        }
                        div { class: "filters quick-price-controls",
                            input {
                                placeholder: "搜尋商品名稱或代號",
                                value: "{search}",
                                oninput: move |event| search.set(event.value()),
                            }
                            SelectFilter {
                                label: "幣別".to_string(),
                                value: currency_filter(),
                                options: currency_options,
                                translate_options: false,
                                on_change: move |value| currency_filter.set(value),
                            }
                            select {
                                value: "{sort_by}",
                                oninput: move |event| sort_by.set(event.value()),
                                option { value: "symbol", "依商品代號排序" }
                                option { value: "name", "依商品名稱排序" }
                                option { value: "price", "依目前市價排序" }
                            }
                            label { class: "filter-field",
                                span { "價格日期" }
                                input {
                                    r#type: "date",
                                    value: "{price_date}",
                                    oninput: move |event| {
                                        error_message.set(String::new());
                                        success_message.set(String::new());
                                        price_date.set(event.value());
                                    },
                                    disabled: is_saving(),
                                }
                            }
                            div { class: "filter-total", "{display_rows.len()} / {price_rows.len()} 檔商品可更新市價" }
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: is_saving(),
                                onclick: move |_| {
                                    search.set(String::new());
                                    currency_filter.set(String::new());
                                    sort_by.set("symbol".to_string());
                                },
                                "清除篩選"
                            }
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: is_saving(),
                                onclick: move |_| {
                                    error_message.set(String::new());
                                    success_message.set(String::new());
                                    draft_prices.set(HashMap::new());
                                },
                                "清空輸入"
                            }
                            button {
                                r#type: "button",
                                class: "primary-button",
                                disabled: is_saving(),
                                onclick: move |_| {
                                    if is_saving() {
                                        return;
                                    }

                                    is_saving.set(true);
                                    error_message.set(String::new());
                                    success_message.set(String::new());

                                    let current_price_date = price_date();
                                    let input = build_batch_price_input(
                                        current_price_date,
                                        &save_rows,
                                        &draft_values,
                                    );

                                    let mut is_saving = is_saving;
                                    let mut error_message = error_message;
                                    let mut success_message = success_message;
                                    let mut draft_prices = draft_prices;
                                    let mut data_version = data_version;

                                    spawn(async move {
                                        match run_batch_price_save(input).await {
                                            Ok(saved_count) => {
                                                draft_prices.set(HashMap::new());
                                                success_message.set(format!(
                                                    "已更新 {saved_count} 檔商品市價"
                                                ));
                                                data_version.with_mut(|value| *value += 1);
                                            }
                                            Err(error) => {
                                                error_message.set(error.to_string());
                                            }
                                        }

                                        is_saving.set(false);
                                    });
                                },
                                if is_saving() { "儲存中..." } else { "儲存有輸入的價格" }
                            }
                        }
                        if display_rows.is_empty() {
                            div { class: "empty-state",
                                h3 { "目前沒有符合條件的可更新商品" }
                            }
                        } else {
                            div { class: "table-wrap",
                            table { class: "price-update-table",
                                thead {
                                    tr {
                                        th { "商品代號" }
                                        th { "商品名稱" }
                                        th { "幣別" }
                                        th { "目前市價" }
                                        th { "價格日期" }
                                        th { "持有帳戶數" }
                                        th { "新市價" }
                                    }
                                }
                                tbody {
                                    for (row, draft_value) in display_rows {
                                        QuickPriceUpdateRowView {
                                            row,
                                            draft_value,
                                            is_saving: is_saving(),
                                            on_price_input: move |(instrument_id, value)| {
                                                error_message.set(String::new());
                                                success_message.set(String::new());
                                                draft_prices.with_mut(|prices| {
                                                    prices.insert(instrument_id, value);
                                                });
                                            },
                                        }
                                    }
                                }
                            }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn QuickPriceUpdateRowView(
    row: QuickPriceUpdateRow,
    draft_value: String,
    is_saving: bool,
    on_price_input: EventHandler<(i64, String)>,
) -> Element {
    let instrument_id = row.instrument_id;
    let latest_price_date = row.latest_price_date.as_deref().unwrap_or("-");

    rsx! {
        tr {
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { class: "mono", "{row.currency_code}" }
            td { class: "number", "{decimal(row.latest_price, 4)}" }
            td { class: "mono", "{latest_price_date}" }
            td { class: "number", "{row.holding_account_count}" }
            td {
                input {
                    class: "table-input mono",
                    value: "{draft_value}",
                    placeholder: "0",
                    disabled: is_saving,
                    oninput: move |event| on_price_input.call((instrument_id, event.value())),
                }
            }
        }
    }
}

fn filter_quick_price_rows(
    rows: &[QuickPriceUpdateRow],
    search: &str,
    currency_filter: &str,
    sort_by: &str,
) -> Vec<QuickPriceUpdateRow> {
    let search = search.to_lowercase();
    let currency_filter = if currency_filter.is_empty()
        || rows.iter().any(|row| row.currency_code == currency_filter)
    {
        currency_filter
    } else {
        ""
    };
    let mut filtered_rows = rows
        .iter()
        .filter(|row| currency_filter.is_empty() || row.currency_code == currency_filter)
        .filter(|row| {
            search.is_empty()
                || row.symbol.to_lowercase().contains(&search)
                || row.instrument_name.to_lowercase().contains(&search)
        })
        .cloned()
        .collect::<Vec<_>>();

    filtered_rows.sort_by(|left, right| match sort_by {
        "name" => left.instrument_name.cmp(&right.instrument_name),
        "price" => compare_optional_desc(left.latest_price, right.latest_price),
        _ => left.symbol.cmp(&right.symbol),
    });

    filtered_rows
}

fn valid_price_date(value: &str, default: &str) -> String {
    if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
        value.to_string()
    } else {
        default.to_string()
    }
}

fn build_batch_price_input(
    price_date: String,
    rows: &[QuickPriceUpdateRow],
    draft_prices: &HashMap<i64, String>,
) -> BatchPriceInput {
    BatchPriceInput {
        price_date,
        rows: rows
            .iter()
            .map(|row| BatchPriceRowInput {
                instrument_id: row.instrument_id,
                symbol: row.symbol.clone(),
                instrument_name: row.instrument_name.clone(),
                currency_code: row.currency_code.clone(),
                price: draft_prices
                    .get(&row.instrument_id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod quick_price_filter_tests {
    use super::*;

    fn row(
        symbol: &str,
        name: &str,
        currency_code: &str,
        latest_price: Option<f64>,
    ) -> QuickPriceUpdateRow {
        QuickPriceUpdateRow {
            instrument_id: 1,
            symbol: symbol.to_string(),
            instrument_name: name.to_string(),
            currency_code: currency_code.to_string(),
            latest_price,
            latest_price_date: None,
            holding_account_count: 1,
        }
    }

    #[test]
    fn filters_and_sorts_quick_price_rows_without_mutating_inputs() {
        let rows = vec![
            row("0050", "元大台灣50", "NTD", Some(190.0)),
            QuickPriceUpdateRow {
                instrument_id: 2,
                ..row("VOO", "Vanguard S&P 500", "USD", Some(500.0))
            },
        ];

        let filtered = filter_quick_price_rows(&rows, "vAnGuArD", "USD", "symbol");

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].symbol, "VOO");
        assert_eq!(
            filter_quick_price_rows(&rows, "", "", "price")[0].symbol,
            "VOO"
        );

        let drafts = HashMap::from([(1, "191.5".to_string()), (2, "501.5".to_string())]);
        let input = build_batch_price_input("2026-07-12".to_string(), &rows, &drafts);
        assert_eq!(input.rows.len(), 2);
        assert_eq!(input.rows[0].price, "191.5");
        assert_eq!(input.rows[1].price, "501.5");
    }

    #[test]
    fn invalid_saved_price_date_uses_today() {
        assert_eq!(valid_price_date("2026-07-12", "2026-07-01"), "2026-07-12");
        assert_eq!(valid_price_date("invalid", "2026-07-01"), "2026-07-01");
    }
}

#[component]
pub fn DividendIncomePage() -> Element {
    let mut data_version = use_context::<Signal<u64>>();
    let create_modal_key = "create-dividend-receipt-modal";
    let receipts = use_resource(move || async move {
        let _ = data_version();
        load_dividend_receipts()
    });
    let receipt_options = use_resource(move || async move {
        let _ = data_version();
        load_dividend_receipt_form_options()
    });
    let institution_options = use_resource(move || async move {
        let _ = data_version();
        load_institution_options()
    });
    let mut is_creating = use_signal(|| false);
    let mut editing_receipt = use_signal(|| None::<DividendReceiptRow>);
    let mut status_message = use_signal(String::new);

    rsx! {
        PageHeader {
            title: "股息收入".to_string(),
            description: "第一階段僅查看新制逐筆股息；舊 Excel 彙總請至 Excel 歷史股息頁。".to_string(),
        }

        if !status_message().is_empty() {
            div { class: "status-message success", "{status_message}" }
        }

        div { class: "stack",
            section { class: "section-block",
                h3 { "新制逐筆股息" }
                div { class: "section-actions",
                    button {
                        r#type: "button",
                        class: "primary-button",
                        onclick: move |_| is_creating.set(true),
                        "＋新增股息"
                    }
                    Link { class: "inline-link", to: crate::routes::Route::DividendsLegacyPage {}, "查看 Excel 歷史股息彙總" }
                }
                match receipts() {
                    None => rsx! { StatusCard { text: "正在載入資料…".to_string() } },
                    Some(Err(error)) => rsx! { StatusCard { text: format!("無法讀取逐筆股息資料：{error}") } },
                    Some(Ok(rows)) if rows.is_empty() => rsx! {
                        section { class: "card empty-state",
                            h3 { "目前沒有逐筆股息紀錄" }
                            p { "這不是錯誤。第一階段不新增資料；Excel 匯入的歷史彙總保留為唯讀歷史參考。" }
                        }
                    },
                    Some(Ok(rows)) => rsx! {
                        DividendReceiptTable {
                            rows,
                            on_edit: move |row| editing_receipt.set(Some(row)),
                        }
                    },
                }
            }
        }

        if is_creating() {
            match receipt_options() {
                None => rsx! { StatusCard { text: "載入新增股息選項中...".to_string() } },
                Some(Err(error)) => rsx! { StatusCard { text: format!("讀取新增股息選項失敗：{error}") } },
                Some(Ok(options)) => rsx! {
                    DividendReceiptUpsertModal {
                        key: "{create_modal_key}",
                        options,
                        institutions: institution_options()
                            .and_then(|result| result.ok())
                            .unwrap_or_default(),
                        receipt: None,
                        allow_delete: false,
                        on_close: move |_| is_creating.set(false),
                        on_saved: move |message| {
                            status_message.set(message);
                            is_creating.set(false);
                            data_version.with_mut(|value| *value += 1);
                        },
                        on_deleted: move |_| {},
                        on_account_created: move |account_id_value| {
                            status_message.set(format!("已新增帳戶 #{account_id_value}"));
                            data_version.with_mut(|value| *value += 1);
                        },
                        on_instrument_created: move |(instrument_id_value, _currency_code_value): (i64, String)| {
                            status_message.set(format!("已新增商品 #{instrument_id_value}"));
                            data_version.with_mut(|value| *value += 1);
                        },
                    }
                },
            }
        }

        if let Some(receipt) = editing_receipt() {
            match receipt_options() {
                None => rsx! { StatusCard { text: "載入編輯股息選項中...".to_string() } },
                Some(Err(error)) => rsx! { StatusCard { text: format!("讀取編輯股息選項失敗：{error}") } },
                Some(Ok(options)) => rsx! {
                    DividendReceiptUpsertModal {
                        key: "edit-dividend-receipt-modal-{receipt.receipt_id}",
                        options,
                        institutions: institution_options()
                            .and_then(|result| result.ok())
                            .unwrap_or_default(),
                        receipt: Some(receipt),
                        allow_delete: true,
                        on_close: move |_| editing_receipt.set(None),
                        on_saved: move |message| {
                            status_message.set(message);
                            editing_receipt.set(None);
                            data_version.with_mut(|value| *value += 1);
                        },
                        on_deleted: move |message| {
                            status_message.set(message);
                            editing_receipt.set(None);
                            data_version.with_mut(|value| *value += 1);
                        },
                        on_account_created: move |account_id_value| {
                            status_message.set(format!("已新增帳戶 #{account_id_value}"));
                            data_version.with_mut(|value| *value += 1);
                        },
                        on_instrument_created: move |(instrument_id_value, _currency_code_value): (i64, String)| {
                            status_message.set(format!("已新增商品 #{instrument_id_value}"));
                            data_version.with_mut(|value| *value += 1);
                        },
                    }
                },
            }
        }
    }
}

#[component]
pub fn ExchangeRatePage() -> Element {
    let data_version = use_context::<Signal<u64>>();
    let rates = use_resource(move || async move {
        let _ = data_version();
        load_recent_exchange_rates(20)
    });
    let today = chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let mut base_currency_code = use_signal(|| "USD".to_string());
    let mut rate_date = use_signal(|| today);
    let mut rate = use_signal(String::new);
    let mut note = use_signal(String::new);
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut success_message = use_signal(String::new);

    rsx! {
        PageHeader {
            title: "匯率維護".to_string(),
            description: "維護 {base_currency}/NTD 匯率，讓外幣估值與缺匯率警告可被修復。".replace("{base_currency}", &base_currency_code()),
        }

        section { class: "card",
            if !success_message().is_empty() {
                div { class: "status-message success", "{success_message}" }
            }
            if !error_message().is_empty() {
                div { class: "status-message error quick-price-error", "{error_message}" }
            }
            div { class: "form-grid two-column",
                label { class: "form-field",
                    span { "來源幣別" }
                    input {
                        value: "{base_currency_code}",
                        oninput: move |event| {
                            error_message.set(String::new());
                            success_message.set(String::new());
                            base_currency_code.set(event.value().to_uppercase());
                        },
                        disabled: is_saving(),
                        placeholder: "USD",
                    }
                }
                div { class: "form-field",
                    span { "目標幣別" }
                    div { class: "readonly-field", "NTD" }
                }
                label { class: "form-field",
                    span { "匯率日期" }
                    input {
                        r#type: "date",
                        value: "{rate_date}",
                        oninput: move |event| {
                            error_message.set(String::new());
                            success_message.set(String::new());
                            rate_date.set(event.value());
                        },
                        disabled: is_saving(),
                    }
                }
                label { class: "form-field",
                    span { "匯率" }
                    input {
                        value: "{rate}",
                        oninput: move |event| {
                            error_message.set(String::new());
                            success_message.set(String::new());
                            rate.set(event.value());
                        },
                        disabled: is_saving(),
                        placeholder: "31.25",
                    }
                }
                label { class: "form-field full-width",
                    span { "備註" }
                    textarea {
                        value: "{note}",
                        oninput: move |event| {
                            error_message.set(String::new());
                            success_message.set(String::new());
                            note.set(event.value());
                        },
                        disabled: is_saving(),
                        rows: "3",
                        placeholder: "選填",
                    }
                }
            }
            div { class: "modal-actions",
                button {
                    r#type: "button",
                    class: "ghost-button",
                    disabled: is_saving(),
                    onclick: move |_| {
                        error_message.set(String::new());
                        success_message.set(String::new());
                        rate.set(String::new());
                        note.set(String::new());
                    },
                    "清空"
                }
                button {
                    r#type: "button",
                    class: "primary-button",
                    disabled: is_saving(),
                    onclick: move |_| {
                        if is_saving() {
                            return;
                        }

                        is_saving.set(true);
                        error_message.set(String::new());
                        success_message.set(String::new());

                        let input = ExchangeRateInput {
                            base_currency_code: base_currency_code(),
                            rate_date: rate_date(),
                            rate: rate(),
                            note: note(),
                        };

                        let mut is_saving = is_saving;
                        let mut error_message = error_message;
                        let mut success_message = success_message;
                        let mut data_version = data_version;

                        spawn(async move {
                            match run_exchange_rate_save(input).await {
                                Ok(()) => {
                                    success_message.set("匯率已儲存".to_string());
                                    data_version.with_mut(|value| *value += 1);
                                }
                                Err(error) => error_message.set(error.to_string()),
                            }

                            is_saving.set(false);
                        });
                    },
                    if is_saving() { "儲存中..." } else { "儲存匯率" }
                }
            }
        }

        match rates() {
            None => rsx! { StatusCard { text: "載入最近匯率中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取最近匯率失敗：{error}") } },
            Some(Ok(rows)) if rows.is_empty() => rsx! { StatusCard { text: "目前沒有匯率資料。".to_string() } },
            Some(Ok(rows)) => rsx! { ExchangeRateTable { rows } },
        }
    }
}

#[component]
fn ExchangeRateTable(rows: Vec<ExchangeRateRow>) -> Element {
    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "最近匯率" }
                span { "{rows.len()} 筆" }
            }
            div { class: "table-wrap",
                table { class: "account-assets-table",
                    thead {
                        tr {
                            th { "日期" }
                            th { "來源幣別" }
                            th { "目標幣別" }
                            th { "匯率" }
                            th { "來源" }
                            th { "備註" }
                        }
                    }
                    tbody {
                        for row in rows {
                            tr {
                                td { class: "mono", "{row.rate_date}" }
                                td { class: "mono", "{row.base_currency_code}" }
                                td { class: "mono", "{row.quote_currency_code}" }
                                td { class: "number", "{row.rate_text}" }
                                td { "{row.origin}" }
                                td { "{row.note}" }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub fn DividendsLegacyPage() -> Element {
    let legacy_dividends = use_resource(move || async move { load_legacy_dividends() });

    rsx! {
        PageHeader {
            title: "Excel 歷史股息彙總".to_string(),
            description: "唯讀保存股息收入明細表中無法轉成逐筆股息紀錄的歷史彙總值。".to_string(),
        }

        section { class: "card legacy-note",
            strong { "唯讀歷史資料" }
            p { "這些資料保留原始 Excel 手動輸入的年度、累積與月份彙總，包含來源儲存格位置；不包含入帳日期、入帳帳戶、稅額、費用或除息資訊，因此不會轉成逐筆 dividend_receipt。" }
        }

        match legacy_dividends() {
            None => rsx! { StatusCard { text: "載入 Excel 歷史股息彙總中...".to_string() } },
            Some(Err(error)) => rsx! { StatusCard { text: format!("讀取 Excel 歷史股息彙總失敗：{error}") } },
            Some(Ok(data)) if data.summaries.is_empty() && data.monthly.is_empty() => rsx! { StatusCard { text: "目前沒有 Excel 歷史股息彙總資料。".to_string() } },
            Some(Ok(data)) => rsx! { LegacyDividendTables { data } },
        }
    }
}

#[component]
fn PageHeader(title: String, description: String) -> Element {
    rsx! {
        header { class: "page-header",
            h2 { "{title}" }
            p { class: "page-description", "{description}" }
        }
    }
}

#[component]
fn PlaceholderCard(text: String) -> Element {
    rsx! {
        section { class: "card placeholder", "{text}" }
    }
}

#[component]
fn StatusCard(text: String) -> Element {
    rsx! {
        section { class: "card status", "{text}" }
    }
}

#[component]
fn DashboardCards(summary: DashboardSummary) -> Element {
    let latest_account_asset_date = summary.latest_account_asset_date.as_deref().unwrap_or("-");
    let latest_holding_date = summary.latest_holding_date.as_deref().unwrap_or("-");

    rsx! {
        section { class: "dashboard-grid",
            MetricCard {
                label: "總資產".to_string(),
                value: money(summary.total_assets),
                hint: incomplete_total_hint(&summary),
                accent: "primary".to_string(),
            }
            MetricCard {
                label: "帳戶資產".to_string(),
                value: money(summary.account_assets),
                hint: missing_value_hint(
                    summary.account_asset_count,
                    summary.account_asset_missing_value_count,
                    "筆最新快照",
                    "筆缺台幣價值",
                ),
                accent: "cash".to_string(),
            }
            MetricCard {
                label: "投資資產".to_string(),
                value: money(summary.investment_assets),
                hint: missing_value_hint(
                    summary.holding_count,
                    summary.holding_missing_market_value_count,
                    "筆持股",
                    "筆缺市值",
                ),
                accent: "investment".to_string(),
            }
            MetricCard {
                label: "預估年配息".to_string(),
                value: money(summary.estimated_annual_dividend),
                hint: dividend_hint(summary.holding_count, summary.holding_missing_dividend_count),
                accent: "income".to_string(),
            }
            MetricCard {
                label: "預估月平均配息".to_string(),
                value: money(summary.estimated_monthly_dividend),
                hint: if summary.holding_missing_dividend_count == 0 {
                    "預估年配息 / 12".to_string()
                } else {
                    format!("預估年配息 / 12，{} 筆缺配息假設", summary.holding_missing_dividend_count)
                },
                accent: "income".to_string(),
            }
            section { class: "card dashboard-status",
                h3 { "資料狀態" }
                dl {
                    div {
                        dt { "帳戶資產筆數" }
                        dd { "{summary.account_asset_count} 筆 / 缺值 {summary.account_asset_missing_value_count} 筆" }
                    }
                    div {
                        dt { "持股筆數" }
                        dd { "{summary.holding_count} 筆 / 缺市值 {summary.holding_missing_market_value_count} 筆 / 缺配息 {summary.holding_missing_dividend_count} 筆" }
                    }
                    div {
                        dt { "最新帳戶資產日期" }
                        dd { class: "mono", "{latest_account_asset_date}" }
                    }
                    div {
                        dt { "最新持股日期" }
                        dd { class: "mono", "{latest_holding_date}" }
                    }
                }
            }
            section { class: "card owner-totals",
                h3 { "各所有權人資產總額" }
                div { class: "owner-total-list",
                    for owner_total in summary.owner_totals {
                        div {
                            span { "{owner_total.owner_name}" }
                            strong { "{money(owner_total.value_ntd)}" }
                        }
                    }
                }
            }
        }
    }
}

fn incomplete_total_hint(summary: &DashboardSummary) -> String {
    let missing_count =
        summary.account_asset_missing_value_count + summary.holding_missing_market_value_count;
    if missing_count == 0 {
        "帳戶資產 + 投資市值".to_string()
    } else {
        format!("帳戶資產 + 投資市值，另有 {missing_count} 筆缺值未計入")
    }
}

fn missing_value_hint(
    total_count: i64,
    missing_count: i64,
    total_label: &str,
    missing_label: &str,
) -> String {
    if missing_count == 0 {
        format!("{total_count} {total_label}")
    } else {
        format!("{total_count} {total_label}，{missing_count} {missing_label}")
    }
}

fn dividend_hint(total_count: i64, missing_count: i64) -> String {
    if missing_count == 0 {
        format!("由 {total_count} 筆持股配息假設加總")
    } else {
        format!("由持股配息假設加總，{missing_count} 筆缺配息假設未計入")
    }
}

#[component]
fn MetricCard(label: String, value: String, hint: String, accent: String) -> Element {
    rsx! {
        section { class: "card metric-card {accent}",
            p { class: "metric-label", "{label}" }
            strong { class: "metric-value", "{value}" }
            p { class: "metric-hint", "{hint}" }
        }
    }
}

#[component]
fn DividendReceiptTable(
    rows: Vec<DividendReceiptRow>,
    on_edit: EventHandler<DividendReceiptRow>,
) -> Element {
    let summary = build_dividend_receipt_summary(&rows);

    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "{rows.len()} 筆逐筆股息" }
                span { "來源 dividend_receipt / v_dividend_receipt_amount" }
            }
            DividendReceiptSummaryPanel { summary }
            div { class: "table-wrap",
                table { class: "dividend-receipt-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "入帳帳戶" }
                            th { "帳戶號碼" }
                            th { "代號" }
                            th { "商品" }
                            th { "來源" }
                            th { "入帳日期" }
                            th { "稅前金額" }
                            th { "稅額" }
                            th { "費用" }
                            th { "實收金額" }
                            th { "幣別" }
                            th { "備註" }
                            th { "操作" }
                        }
                    }
                    tbody {
                        for row in rows {
                            DividendReceiptRowView { row, on_edit: move |receipt| on_edit.call(receipt) }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn DividendReceiptRowView(
    row: DividendReceiptRow,
    on_edit: EventHandler<DividendReceiptRow>,
) -> Element {
    let can_edit = row.origin == "MANUAL";

    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { "{row.account_name}" }
            td { class: "mono", "{row.account_number.as_deref().unwrap_or(\"—\")}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { class: "mono", "{row.origin}" }
            td { class: "mono", "{row.received_on}" }
            td { class: "number", "{money(row.gross_amount)}" }
            td { class: "number", "{money(row.tax_amount)}" }
            td { class: "number", "{money(row.fee_amount)}" }
            td { class: "number strong", "{money(row.net_amount)}" }
            td { class: "mono", "{row.currency_code}" }
            td { "{row.note}" }
            td {
                if can_edit {
                    button {
                        r#type: "button",
                        class: "inline-action",
                        onclick: move |_| on_edit.call(row.clone()),
                        "編輯"
                    }
                } else {
                    span { class: "muted", "唯讀" }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DividendReceiptSummary {
    currency_totals: Vec<(String, Decimal)>,
    owner_totals: Vec<(String, Decimal)>,
    instrument_totals: Vec<(String, Decimal)>,
    year_totals: Vec<(String, Decimal)>,
    month_totals: Vec<(String, Decimal)>,
}

#[component]
fn DividendReceiptSummaryPanel(summary: DividendReceiptSummary) -> Element {
    rsx! {
        section { class: "table-summary-grid",
            DividendReceiptSummaryTable {
                title: "按幣別".to_string(),
                rows: summary.currency_totals,
            }
            DividendReceiptSummaryTable {
                title: "按所有權人".to_string(),
                rows: summary.owner_totals,
            }
            DividendReceiptSummaryTable {
                title: "按商品".to_string(),
                rows: summary.instrument_totals,
            }
            DividendReceiptSummaryTable {
                title: "按年度".to_string(),
                rows: summary.year_totals,
            }
            DividendReceiptSummaryTable {
                title: "按月份".to_string(),
                rows: summary.month_totals,
            }
        }
    }
}

#[component]
fn DividendReceiptSummaryTable(title: String, rows: Vec<(String, Decimal)>) -> Element {
    rsx! {
        section { class: "card summary-card",
            strong { "{title}" }
            if rows.is_empty() {
                p { class: "muted", "目前沒有資料" }
            } else {
                div { class: "summary-list",
                    for (label, value) in rows {
                        div {
                            span { "{label}" }
                            strong { "{money(Some(decimal_to_money(value)))}" }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DividendReceiptAccountChoice {
    account_id: i64,
    label: String,
}

#[derive(Clone, Debug, PartialEq)]
struct DividendReceiptInstrumentChoice {
    instrument_id: i64,
    label: String,
    currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
struct DividendReceiptModalForm {
    account_id: String,
    instrument_id: String,
    received_on: String,
    net_amount: String,
    currency_code: String,
    note: String,
}

#[derive(Clone, Debug, PartialEq)]
struct AccountCreateModalForm {
    display_name: String,
    account_number: String,
    institution_id: String,
}

#[derive(Clone, Debug, PartialEq)]
struct InstrumentCreateModalForm {
    symbol: String,
    name: String,
    instrument_type: String,
    asset_class: String,
    region_type: String,
    trading_currency_code: String,
}

#[derive(Clone, Debug, PartialEq)]
struct AccountAssetEditForm {
    snapshot_date: String,
    quantity: String,
    current_value_override: String,
    invested_amount: String,
    note: String,
}

#[derive(Clone, Debug, PartialEq)]
struct HoldingEditForm {
    as_of_date: String,
    quantity_text: String,
    average_cost_text: String,
    note: String,
}

#[derive(Clone, Debug, PartialEq)]
struct HoldingDividendAssumptionForm {
    effective_date: String,
    payments_per_year: String,
    latest_dividend_per_unit: String,
    estimated_annual_dividend_per_unit: String,
}

#[component]
fn DividendReceiptUpsertModal(
    options: DividendReceiptFormOptions,
    institutions: Vec<InstitutionOption>,
    receipt: Option<DividendReceiptRow>,
    allow_delete: bool,
    on_close: EventHandler<()>,
    on_saved: EventHandler<String>,
    on_deleted: EventHandler<String>,
    on_account_created: EventHandler<i64>,
    on_instrument_created: EventHandler<(i64, String)>,
) -> Element {
    let is_editing = receipt.is_some();
    let today = chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let receipt_for_account = receipt.clone();
    let receipt_for_instrument = receipt.clone();
    let receipt_for_received_on = receipt.clone();
    let receipt_for_net_amount = receipt.clone();
    let receipt_for_currency_code = receipt.clone();
    let receipt_for_note = receipt.clone();
    let receipt_for_save = receipt.clone();
    let receipt_for_delete = receipt;
    let institutions_for_select = institutions.clone();

    let account_choices = options
        .accounts
        .iter()
        .map(|option| DividendReceiptAccountChoice {
            account_id: option.account_id,
            label: dividend_receipt_account_label(option),
        })
        .collect::<Vec<_>>();
    let instrument_choices = options
        .instruments
        .iter()
        .map(|option| DividendReceiptInstrumentChoice {
            instrument_id: option.instrument_id,
            label: dividend_receipt_instrument_label(option),
            currency_code: option.currency_code.clone(),
        })
        .collect::<Vec<_>>();
    let account_choices_for_select = account_choices.clone();
    let instrument_choices_for_select = instrument_choices.clone();
    let instrument_choices_for_lookup = instrument_choices.clone();
    let initial_form = DividendReceiptModalForm {
        account_id: receipt_for_account
            .as_ref()
            .map(|row| row.account_id.to_string())
            .or_else(|| {
                account_choices
                    .first()
                    .map(|option| option.account_id.to_string())
            })
            .unwrap_or_default(),
        instrument_id: receipt_for_instrument
            .as_ref()
            .map(|row| row.instrument_id.to_string())
            .or_else(|| {
                instrument_choices
                    .first()
                    .map(|option| option.instrument_id.to_string())
            })
            .unwrap_or_default(),
        received_on: receipt_for_received_on
            .as_ref()
            .map(|row| row.received_on.clone())
            .unwrap_or_else(|| today.clone()),
        net_amount: receipt_for_net_amount
            .as_ref()
            .and_then(|row| row.net_amount)
            .map(|value| value.to_string())
            .unwrap_or_default(),
        currency_code: receipt_for_currency_code
            .as_ref()
            .map(|row| row.currency_code.clone())
            .or_else(|| {
                instrument_choices
                    .first()
                    .map(|option| option.currency_code.clone())
            })
            .or_else(|| options.currency_codes.first().cloned())
            .unwrap_or_else(|| "NTD".to_string()),
        note: receipt_for_note
            .as_ref()
            .map(|row| row.note.clone())
            .unwrap_or_default(),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());

    let mut account_id = use_signal(|| initial_form.account_id.clone());
    let mut instrument_id = use_signal(|| initial_form.instrument_id.clone());
    let mut received_on = use_signal(|| initial_form.received_on.clone());
    let mut net_amount = use_signal(|| initial_form.net_amount.clone());
    let mut currency_code = use_signal(|| initial_form.currency_code.clone());
    let mut note = use_signal(|| initial_form.note.clone());
    let mut is_saving = use_signal(|| false);
    let mut is_deleting = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_delete = use_signal(|| false);
    let mut confirm_close = use_signal(|| false);
    let mut creating_account = use_signal(|| false);
    let mut creating_instrument = use_signal(|| false);

    let interaction_locked = is_saving() || is_deleting();
    let is_dirty = DividendReceiptModalForm {
        account_id: account_id(),
        instrument_id: instrument_id(),
        received_on: received_on(),
        net_amount: net_amount(),
        currency_code: currency_code(),
        note: note(),
    } != initial_form_snapshot();

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card",
                div { class: "modal-header",
                    div {
                        p { class: "eyebrow", "手動新增" }
                        if is_editing {
                            h3 { "編輯股息收入" }
                        } else {
                            h3 { "新增股息收入" }
                        }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            confirm_delete.set(false);
                            let reset_form = initial_form_snapshot();
                            account_id.set(reset_form.account_id);
                            instrument_id.set(reset_form.instrument_id);
                            received_on.set(reset_form.received_on);
                            net_amount.set(reset_form.net_amount);
                            currency_code.set(reset_form.currency_code);
                            note.set(reset_form.note);
                        },
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                div { class: "form-grid two-column",
                    div { class: "form-field",
                        span { "入帳帳戶" }
                        SearchableSelect {
                            label: "入帳帳戶".to_string(),
                            value: "{account_id}",
                            options: account_choices_for_select
                                .iter()
                                .map(|option| (option.account_id.to_string(), option.label.clone()))
                                .collect(),
                            on_change: move |value| account_id.set(value),
                            disabled: interaction_locked,
                        }
                        button {
                            r#type: "button",
                            class: "ghost-button inline-action",
                            disabled: interaction_locked,
                            onclick: move |_| creating_account.set(true),
                            "＋新增帳戶"
                        }
                    }
                    div { class: "form-field",
                        span { "商品" }
                        SearchableSelect {
                            label: "商品".to_string(),
                            value: "{instrument_id}",
                            options: instrument_choices_for_select
                                .iter()
                                .map(|option| (option.instrument_id.to_string(), option.label.clone()))
                                .collect(),
                            on_change: move |selected_id: String| {
                                instrument_id.set(selected_id.clone());
                                if let Some(selected) = instrument_choices_for_lookup
                                    .iter()
                                    .find(|option| option.instrument_id.to_string() == selected_id)
                                {
                                    currency_code.set(selected.currency_code.clone());
                                }
                            },
                            disabled: interaction_locked,
                        }
                        button {
                            r#type: "button",
                            class: "ghost-button inline-action",
                            disabled: interaction_locked,
                            onclick: move |_| creating_instrument.set(true),
                            "＋新增商品"
                        }
                    }
                    label { class: "form-field",
                        span { "入帳日期" }
                        input {
                            r#type: "date",
                            value: "{received_on}",
                            oninput: move |event| received_on.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    label { class: "form-field",
                        span { "實收金額" }
                        input {
                            value: "{net_amount}",
                            oninput: move |event| net_amount.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "1000.50",
                        }
                    }
                    label { class: "form-field",
                        span { "幣別" }
                        select {
                            value: "{currency_code}",
                            oninput: move |event| currency_code.set(event.value()),
                            disabled: interaction_locked,
                            for currency in &options.currency_codes {
                                option { value: "{currency}", "{currency}" }
                            }
                        }
                    }
                    label { class: "form-field full-width",
                        span { "備註" }
                        textarea {
                            value: "{note}",
                            oninput: move |event| note.set(event.value()),
                            disabled: interaction_locked,
                            rows: "3",
                            placeholder: "選填",
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        "取消"
                    }
                    if allow_delete {
                        button {
                            r#type: "button",
                            class: "ghost-button danger",
                            disabled: interaction_locked,
                            onclick: move |_| {
                                error_message.set(String::new());
                                confirm_close.set(false);
                                confirm_delete.set(true);
                            },
                            "刪除"
                        }
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }

                            is_saving.set(true);
                            error_message.set(String::new());

                            let account_id_value = account_id().parse::<i64>().unwrap_or_default();
                            let instrument_id_value = instrument_id().parse::<i64>().unwrap_or_default();
                            let current_received_on = received_on();
                            let current_net_amount = net_amount();
                            let current_currency_code = currency_code();
                            let current_note = note();

                            let mut is_saving = is_saving;
                            let mut error_message = error_message;
                            let mut confirm_close = confirm_close;
                            let receipt_id = receipt_for_save.as_ref().map(|row| row.receipt_id);

                            spawn(async move {
                                let result = if let Some(receipt_id) = receipt_id {
                                    run_dividend_receipt_update(DividendReceiptUpdateInput {
                                        receipt_id,
                                        account_id: account_id_value,
                                        instrument_id: instrument_id_value,
                                        received_on: current_received_on,
                                        net_amount_text: current_net_amount,
                                        currency_code: current_currency_code,
                                        note: current_note,
                                    })
                                    .await
                                    .map(|_| "股息收入已更新".to_string())
                                } else {
                                    run_dividend_receipt_save(DividendReceiptInput {
                                        account_id: account_id_value,
                                        instrument_id: instrument_id_value,
                                        received_on: current_received_on,
                                        net_amount_text: current_net_amount,
                                        currency_code: current_currency_code,
                                        note: current_note,
                                    })
                                    .await
                                    .map(|_| "股息收入已新增".to_string())
                                };

                                match result {
                                    Ok(message) => {
                                        confirm_close.set(false);
                                        on_saved.call(message);
                                    }
                                    Err(error) => error_message.set(error.to_string()),
                                }

                                is_saving.set(false);
                            });
                        },
                        if is_saving() {
                            "儲存中..."
                        } else if is_editing {
                            "更新股息"
                        } else {
                            "儲存股息"
                        }
                    }
                }
                if confirm_delete() {
                    div { class: "delete-confirmation",
                        p { "確定要刪除這筆手動股息紀錄嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: is_saving() || is_deleting(),
                                onclick: move |_| confirm_delete.set(false),
                                "取消"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: is_saving() || is_deleting(),
                                onclick: move |_| {
                                    if is_deleting() {
                                        return;
                                    }

                                    is_deleting.set(true);
                                    confirm_close.set(false);
                                    error_message.set(String::new());
                                    let receipt_id = receipt_for_delete
                                        .as_ref()
                                        .map(|row| row.receipt_id)
                                        .unwrap_or_default();
                                    let mut is_deleting = is_deleting;
                                    let mut error_message = error_message;

                                    spawn(async move {
                                        match run_dividend_receipt_delete(DividendReceiptDeleteInput { receipt_id }).await {
                                            Ok(()) => on_deleted.call("股息收入已刪除".to_string()),
                                            Err(error) => error_message.set(error.to_string()),
                                        }

                                        is_deleting.set(false);
                                    });
                                },
                                "確認刪除"
                            }
                        }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
                if creating_account() {
                    AccountCreateModal {
                        institutions: institutions_for_select.clone(),
                        on_close: move |_| creating_account.set(false),
                        on_created: move |account_id_value: i64| {
                            account_id.set(account_id_value.to_string());
                            on_account_created.call(account_id_value);
                            creating_account.set(false);
                        },
                    }
                }
                if creating_instrument() {
                    InstrumentCreateModal {
                        currency_codes: options.currency_codes.clone(),
                        on_close: move |_| creating_instrument.set(false),
                        on_created: move |(instrument_id_value, currency_code_value): (i64, String)| {
                            instrument_id.set(instrument_id_value.to_string());
                            currency_code.set(currency_code_value.clone());
                            on_instrument_created.call((instrument_id_value, currency_code_value));
                            creating_instrument.set(false);
                        },
                    }
                }
            }
        }
    }
}

#[component]
fn AccountCreateModal(
    institutions: Vec<InstitutionOption>,
    on_close: EventHandler<()>,
    on_created: EventHandler<i64>,
) -> Element {
    let initial_form = AccountCreateModalForm {
        display_name: String::new(),
        account_number: String::new(),
        institution_id: institutions
            .first()
            .map(|institution| institution.institution_id.to_string())
            .unwrap_or_default(),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());
    let mut display_name = use_signal(|| initial_form.display_name.clone());
    let mut account_number = use_signal(|| initial_form.account_number.clone());
    let mut institution_id = use_signal(|| initial_form.institution_id.clone());
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_close = use_signal(|| false);

    let interaction_locked = is_saving();
    let is_dirty = AccountCreateModalForm {
        display_name: display_name(),
        account_number: account_number(),
        institution_id: institution_id(),
    } != initial_form_snapshot();

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card",
                div { class: "modal-header",
                    div {
                        p { class: "eyebrow", "最小主檔" }
                        h3 { "新增帳戶" }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            let reset_form = initial_form_snapshot();
                            display_name.set(reset_form.display_name);
                            account_number.set(reset_form.account_number);
                            institution_id.set(reset_form.institution_id);
                        },
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                div { class: "form-grid two-column",
                    label { class: "form-field full-width",
                        span { "帳戶名稱" }
                        input {
                            value: "{display_name}",
                            oninput: move |event| display_name.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "新帳戶",
                        }
                    }
                    label { class: "form-field full-width",
                        span { "帳戶號碼" }
                        input {
                            value: "{account_number}",
                            oninput: move |event| account_number.set(event.value()),
                            disabled: interaction_locked,
                            inputmode: "numeric",
                            placeholder: "完整帳戶號碼（選填）",
                        }
                    }
                    div { class: "form-field full-width",
                        span { "金融機構" }
                        SearchableSelect {
                            label: "金融機構".to_string(),
                            value: "{institution_id}",
                            options: institutions
                                .iter()
                                .map(|institution| {
                                    (institution.institution_id.to_string(), institution.name.clone())
                                })
                                .collect(),
                            on_change: move |value| institution_id.set(value),
                            disabled: interaction_locked,
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        "取消"
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }

                            is_saving.set(true);
                            error_message.set(String::new());
                            let input = AccountCreateInput {
                                institution_id: institution_id().parse::<i64>().unwrap_or_default(),
                                display_name: display_name(),
                                account_number: account_number(),
                            };
                            let mut is_saving = is_saving;
                            let mut error_message = error_message;
                            let mut confirm_close = confirm_close;

                            spawn(async move {
                                match run_account_create(input).await {
                                    Ok(account_id) => {
                                        confirm_close.set(false);
                                        on_created.call(account_id);
                                    }
                                    Err(error) => error_message.set(error.to_string()),
                                }
                                is_saving.set(false);
                            });
                        },
                        if is_saving() { "儲存中..." } else { "儲存帳戶" }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn InstrumentCreateModal(
    currency_codes: Vec<String>,
    on_close: EventHandler<()>,
    on_created: EventHandler<(i64, String)>,
) -> Element {
    let initial_form = InstrumentCreateModalForm {
        symbol: String::new(),
        name: String::new(),
        instrument_type: "ETF".to_string(),
        asset_class: "EQUITY".to_string(),
        region_type: "DOMESTIC".to_string(),
        trading_currency_code: currency_codes
            .first()
            .cloned()
            .unwrap_or_else(|| "NTD".to_string()),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());
    let mut symbol = use_signal(|| initial_form.symbol.clone());
    let mut name = use_signal(|| initial_form.name.clone());
    let mut instrument_type = use_signal(|| initial_form.instrument_type.clone());
    let mut asset_class = use_signal(|| initial_form.asset_class.clone());
    let mut region_type = use_signal(|| initial_form.region_type.clone());
    let mut trading_currency_code = use_signal(|| initial_form.trading_currency_code.clone());
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_close = use_signal(|| false);

    let interaction_locked = is_saving();
    let is_dirty = InstrumentCreateModalForm {
        symbol: symbol(),
        name: name(),
        instrument_type: instrument_type(),
        asset_class: asset_class(),
        region_type: region_type(),
        trading_currency_code: trading_currency_code(),
    } != initial_form_snapshot();

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card",
                div { class: "modal-header",
                    div {
                        p { class: "eyebrow", "最小主檔" }
                        h3 { "新增商品" }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            let reset_form = initial_form_snapshot();
                            symbol.set(reset_form.symbol);
                            name.set(reset_form.name);
                            instrument_type.set(reset_form.instrument_type);
                            asset_class.set(reset_form.asset_class);
                            region_type.set(reset_form.region_type);
                            trading_currency_code.set(reset_form.trading_currency_code);
                        },
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                div { class: "form-grid two-column",
                    label { class: "form-field",
                        span { "商品代號" }
                        input {
                            value: "{symbol}",
                            oninput: move |event| symbol.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "ABC",
                        }
                    }
                    label { class: "form-field",
                        span { "商品名稱" }
                        input {
                            value: "{name}",
                            oninput: move |event| name.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "新商品",
                        }
                    }
                    label { class: "form-field full-width",
                        span { "交易幣別" }
                        select {
                            value: "{trading_currency_code}",
                            oninput: move |event| trading_currency_code.set(event.value()),
                            disabled: interaction_locked,
                            for currency in &currency_codes {
                                option { value: "{currency}", "{currency}" }
                            }
                        }
                    }
                    label { class: "form-field",
                        span { "商品類型" }
                        select {
                            value: "{instrument_type}",
                            oninput: move |event| instrument_type.set(event.value()),
                            disabled: interaction_locked,
                            option { value: "STOCK", "股票" }
                            option { value: "ETF", "ETF" }
                            option { value: "BOND", "債券" }
                            option { value: "FUND", "基金" }
                            option { value: "OTHER", "其他" }
                        }
                    }
                    label { class: "form-field",
                        span { "資產類別" }
                        select {
                            value: "{asset_class}",
                            oninput: move |event| asset_class.set(event.value()),
                            disabled: interaction_locked,
                            option { value: "EQUITY", "股票" }
                            option { value: "BOND", "債券" }
                            option { value: "MIXED", "混合" }
                            option { value: "CASH_EQUIVALENT", "現金等價" }
                            option { value: "OTHER", "其他" }
                        }
                    }
                    label { class: "form-field",
                        span { "區域" }
                        select {
                            value: "{region_type}",
                            oninput: move |event| region_type.set(event.value()),
                            disabled: interaction_locked,
                            option { value: "DOMESTIC", "國內" }
                            option { value: "FOREIGN", "海外" }
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        "取消"
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }

                            is_saving.set(true);
                            error_message.set(String::new());
                            let input = InstrumentCreateInput {
                                symbol: symbol(),
                                name: name(),
                                instrument_type: instrument_type(),
                                asset_class: asset_class(),
                                region_type: region_type(),
                                trading_currency_code: trading_currency_code(),
                            };
                            let created_currency_code = trading_currency_code();
                            let mut is_saving = is_saving;
                            let mut error_message = error_message;
                            let mut confirm_close = confirm_close;

                            spawn(async move {
                                match run_instrument_create(input).await {
                                    Ok(instrument_id) => {
                                        confirm_close.set(false);
                                        on_created.call((instrument_id, created_currency_code));
                                    }
                                    Err(error) => error_message.set(error.to_string()),
                                }
                                is_saving.set(false);
                            });
                        },
                        if is_saving() { "儲存中..." } else { "儲存商品" }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
            }
        }
    }
}

fn dividend_receipt_account_label(option: &DividendReceiptAccountOption) -> String {
    let account_number = option.account_number.as_deref().unwrap_or("—");
    format!(
        "{} / {} / {}",
        option.owner_name.replace(',', "、"),
        option.account_name,
        account_number
    )
}

fn dividend_receipt_instrument_label(option: &DividendReceiptInstrumentOption) -> String {
    format!(
        "{} {} ({})",
        option.symbol, option.instrument_name, option.currency_code
    )
}

fn build_dividend_receipt_summary(rows: &[DividendReceiptRow]) -> DividendReceiptSummary {
    let mut currency_totals = BTreeMap::<String, Decimal>::new();
    let mut owner_totals = BTreeMap::<String, Decimal>::new();
    let mut instrument_totals = BTreeMap::<String, Decimal>::new();
    let mut year_totals = BTreeMap::<String, Decimal>::new();
    let mut month_totals = BTreeMap::<String, Decimal>::new();

    for row in rows {
        let Some(net_amount) = row.net_amount else {
            continue;
        };

        let net_amount_decimal = Decimal::from_f64(net_amount).unwrap_or(Decimal::ZERO);

        *currency_totals
            .entry(row.currency_code.clone())
            .or_default() += net_amount_decimal;
        *owner_totals.entry(row.owner_name.clone()).or_default() += net_amount_decimal;
        *instrument_totals
            .entry(format!("{} {}", row.symbol, row.instrument_name))
            .or_default() += net_amount_decimal;

        if let Some(year) = row.received_on.get(0..4) {
            if year.chars().all(|ch| ch.is_ascii_digit()) {
                *year_totals.entry(year.to_string()).or_default() += net_amount_decimal;
            }
        }

        if let Some(month) = row.received_on.get(0..7) {
            if month.len() == 7 && month.as_bytes().get(4) == Some(&b'-') {
                *month_totals.entry(month.to_string()).or_default() += net_amount_decimal;
            }
        }
    }

    DividendReceiptSummary {
        currency_totals: currency_totals.into_iter().collect(),
        owner_totals: owner_totals.into_iter().collect(),
        instrument_totals: instrument_totals.into_iter().collect(),
        year_totals: year_totals.into_iter().collect(),
        month_totals: month_totals.into_iter().collect(),
    }
}

fn decimal_to_money(value: Decimal) -> f64 {
    value.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod dividend_income_page_tests {
    use super::*;

    #[test]
    fn build_dividend_receipt_summary_uses_exact_decimal_totals() {
        let rows = vec![
            DividendReceiptRow {
                receipt_id: 1,
                account_id: 1,
                instrument_id: 1,
                origin: "MANUAL".to_string(),
                owner_name: "Alex".to_string(),
                account_name: "Account 1".to_string(),
                account_number: Some("001234567890".to_string()),
                symbol: "AAA".to_string(),
                instrument_name: "Alpha".to_string(),
                received_on: "2026-07-09".to_string(),
                gross_amount: None,
                tax_amount: None,
                fee_amount: None,
                net_amount: Some(0.1),
                currency_code: "NTD".to_string(),
                note: String::new(),
            },
            DividendReceiptRow {
                receipt_id: 2,
                account_id: 1,
                instrument_id: 1,
                origin: "MANUAL".to_string(),
                owner_name: "Alex".to_string(),
                account_name: "Account 1".to_string(),
                account_number: Some("001234567890".to_string()),
                symbol: "AAA".to_string(),
                instrument_name: "Alpha".to_string(),
                received_on: "2026-07-10".to_string(),
                gross_amount: None,
                tax_amount: None,
                fee_amount: None,
                net_amount: Some(0.2),
                currency_code: "NTD".to_string(),
                note: String::new(),
            },
        ];

        let summary = build_dividend_receipt_summary(&rows);

        assert_eq!(summary.currency_totals[0].1, Decimal::new(3, 1));
        assert_eq!(summary.month_totals[0].1, Decimal::new(3, 1));
    }

    #[test]
    fn dividend_receipt_account_label_keeps_all_owners_readable() {
        let label = dividend_receipt_account_label(&DividendReceiptAccountOption {
            account_id: 1,
            owner_name: "Alex,Beth".to_string(),
            account_name: "Account 1".to_string(),
            account_number: Some("001234567890".to_string()),
        });

        assert_eq!(label, "Alex、Beth / Account 1 / 001234567890");
    }
}

#[component]
fn LegacyDividendTables(data: LegacyDividendData) -> Element {
    let preferences = use_context::<UiPreferences>();
    let mut owner_filter =
        use_signal(move || preference_value(&preferences(), LEGACY_DIVIDENDS_OWNER));
    let mut instrument_filter =
        use_signal(move || preference_value(&preferences(), LEGACY_DIVIDENDS_INSTRUMENT));
    let mut period_filter =
        use_signal(move || preference_value(&preferences(), LEGACY_DIVIDENDS_PERIOD));
    let mut search = use_signal(move || preference_value(&preferences(), LEGACY_DIVIDENDS_SEARCH));
    let mut sort_by = use_signal(move || {
        valid_sort(
            &preference_value(&preferences(), LEGACY_DIVIDENDS_SORT),
            &["owner", "instrument", "amount"],
            "owner",
        )
    });

    let owner_options = unique_strings(
        data.summaries
            .iter()
            .map(|row| row.owner_name.as_str())
            .chain(data.monthly.iter().map(|row| row.owner_name.as_str())),
    );
    let instrument_options = unique_strings(
        data.summaries
            .iter()
            .map(|row| row.instrument_name.as_str())
            .chain(data.monthly.iter().map(|row| row.instrument_name.as_str())),
    );
    let period_options = unique_strings(
        data.summaries
            .iter()
            .map(|row| row.period_label.as_str())
            .chain(data.monthly.iter().map(|row| row.series_type.as_str())),
    );
    let owner_options_for_validation = owner_options.clone();
    let instrument_options_for_validation = instrument_options.clone();
    let period_options_for_validation = period_options.clone();
    let owner_options_for_persistence = owner_options.clone();
    let instrument_options_for_persistence = instrument_options.clone();
    let period_options_for_persistence = period_options.clone();
    use_effect(move || {
        persist_preference(
            preferences,
            LEGACY_DIVIDENDS_OWNER,
            valid_option(&owner_filter(), &owner_options_for_persistence, ""),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            LEGACY_DIVIDENDS_INSTRUMENT,
            valid_option(
                &instrument_filter(),
                &instrument_options_for_persistence,
                "",
            ),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            LEGACY_DIVIDENDS_PERIOD,
            valid_option(&period_filter(), &period_options_for_persistence, ""),
        )
    });
    use_effect(move || persist_preference(preferences, LEGACY_DIVIDENDS_SEARCH, search()));
    use_effect(move || persist_preference(preferences, LEGACY_DIVIDENDS_SORT, sort_by()));
    use_effect(move || {
        let valid = valid_option(&owner_filter(), &owner_options_for_validation, "");
        if owner_filter() != valid {
            owner_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&instrument_filter(), &instrument_options_for_validation, "");
        if instrument_filter() != valid {
            instrument_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&period_filter(), &period_options_for_validation, "");
        if period_filter() != valid {
            period_filter.set(valid);
        }
    });

    let owner_value = owner_filter();
    let instrument_value = instrument_filter();
    let period_value = period_filter();
    let summaries = filter_legacy_summary_rows(
        &data.summaries,
        &owner_value,
        &instrument_value,
        &period_value,
        &search(),
        &sort_by(),
    );
    let monthly = filter_legacy_monthly_rows(
        &data.monthly,
        &owner_value,
        &instrument_value,
        &period_value,
        &search(),
        &sort_by(),
    );
    let summary_total = summaries.iter().filter_map(|row| row.amount).sum::<f64>();
    let monthly_total = monthly.iter().filter_map(|row| row.amount).sum::<f64>();

    rsx! {
        div { class: "stack",
            div { class: "filters card",
                input {
                    placeholder: "搜尋商品名稱或代號",
                    value: "{search}",
                    oninput: move |event| search.set(event.value()),
                }
                SelectFilter { label: "所有權人".to_string(), value: owner_filter(), options: owner_options, translate_options: false, on_change: move |value| owner_filter.set(value) }
                SelectFilter { label: "商品".to_string(), value: instrument_filter(), options: instrument_options, translate_options: false, on_change: move |value| instrument_filter.set(value) }
                SelectFilter { label: "期間類型".to_string(), value: period_filter(), options: period_options, translate_options: true, on_change: move |value| period_filter.set(value) }
                select {
                    value: "{sort_by}",
                    oninput: move |event| sort_by.set(event.value()),
                    option { value: "owner", "依所有權人排序" }
                    option { value: "instrument", "依商品名稱排序" }
                    option { value: "amount", "依金額排序" }
                }
                div { class: "filter-total", "彙總金額：年度／累積 {money(Some(summary_total))}，月份 {money(Some(monthly_total))}" }
                button {
                    r#type: "button",
                    onclick: move |_| {
                        owner_filter.set(String::new());
                        instrument_filter.set(String::new());
                        period_filter.set(String::new());
                        search.set(String::new());
                        sort_by.set("owner".to_string());
                    },
                    "清除篩選"
                }
            }
            LegacySummaryTable { rows: summaries }
            LegacyMonthlyTable { rows: monthly }
        }
    }
}

fn filter_legacy_summary_rows(
    rows: &[LegacyDividendSummaryRow],
    owner_filter: &str,
    instrument_filter: &str,
    period_filter: &str,
    search: &str,
    sort_by: &str,
) -> Vec<LegacyDividendSummaryRow> {
    let mut filtered_rows = rows
        .iter()
        .filter(|row| {
            legacy_dividend_row_matches(
                &row.owner_name,
                &row.symbol,
                &row.instrument_name,
                &row.period_label,
                owner_filter,
                instrument_filter,
                period_filter,
                search,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    sort_legacy_summary_rows(&mut filtered_rows, sort_by);
    filtered_rows
}

fn filter_legacy_monthly_rows(
    rows: &[LegacyDividendMonthlyRow],
    owner_filter: &str,
    instrument_filter: &str,
    period_filter: &str,
    search: &str,
    sort_by: &str,
) -> Vec<LegacyDividendMonthlyRow> {
    let mut filtered_rows = rows
        .iter()
        .filter(|row| {
            legacy_dividend_row_matches(
                &row.owner_name,
                &row.symbol,
                &row.instrument_name,
                &row.series_type,
                owner_filter,
                instrument_filter,
                period_filter,
                search,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    sort_legacy_monthly_rows(&mut filtered_rows, sort_by);
    filtered_rows
}

#[allow(clippy::too_many_arguments)]
fn legacy_dividend_row_matches(
    owner_name: &str,
    symbol: &str,
    instrument_name: &str,
    period_value: &str,
    owner_filter: &str,
    instrument_filter: &str,
    period_filter: &str,
    search: &str,
) -> bool {
    let search = search.to_lowercase();
    (owner_filter.is_empty() || owner_name == owner_filter)
        && (instrument_filter.is_empty() || instrument_name == instrument_filter)
        && (period_filter.is_empty() || period_value == period_filter)
        && (search.is_empty()
            || symbol.to_lowercase().contains(&search)
            || instrument_name.to_lowercase().contains(&search))
}

fn sort_legacy_summary_rows(rows: &mut [LegacyDividendSummaryRow], sort_by: &str) {
    rows.sort_by(|left, right| match sort_by {
        "instrument" => left.instrument_name.cmp(&right.instrument_name),
        "amount" => compare_optional_desc(left.amount, right.amount),
        _ => left.owner_name.cmp(&right.owner_name),
    });
}

fn sort_legacy_monthly_rows(rows: &mut [LegacyDividendMonthlyRow], sort_by: &str) {
    rows.sort_by(|left, right| match sort_by {
        "instrument" => left.instrument_name.cmp(&right.instrument_name),
        "amount" => compare_optional_desc(left.amount, right.amount),
        _ => left.owner_name.cmp(&right.owner_name),
    });
}

#[cfg(test)]
mod legacy_dividend_filter_tests {
    use super::*;

    #[test]
    fn applies_keyword_search_to_summary_and_monthly_rows() {
        let mut summaries = vec![LegacyDividendSummaryRow {
            owner_name: "Bravo".to_string(),
            symbol: "0050".to_string(),
            instrument_name: "Zebra Fund".to_string(),
            period_label: "YEAR_2024".to_string(),
            amount: Some(100.0),
            source_cell: "A1".to_string(),
        }];
        summaries.push(LegacyDividendSummaryRow {
            owner_name: "Alpha".to_string(),
            symbol: "VOO".to_string(),
            instrument_name: "Apple Fund".to_string(),
            period_label: "YEAR_2024".to_string(),
            amount: Some(200.0),
            source_cell: "A2".to_string(),
        });

        let mut monthly = vec![LegacyDividendMonthlyRow {
            owner_name: "Bravo".to_string(),
            symbol: "0050".to_string(),
            instrument_name: "Zebra Fund".to_string(),
            series_type: "ACTUAL_CURRENT_YEAR".to_string(),
            month_num: 1,
            amount: Some(100.0),
            source_cell: "A1".to_string(),
        }];
        monthly.push(LegacyDividendMonthlyRow {
            owner_name: "Alpha".to_string(),
            symbol: "VOO".to_string(),
            instrument_name: "Apple Fund".to_string(),
            series_type: "ACTUAL_CURRENT_YEAR".to_string(),
            month_num: 1,
            amount: Some(200.0),
            source_cell: "A2".to_string(),
        });

        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "", "0050", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "", "0050", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "", "aPpLe", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "", "aPpLe", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "Bravo", "", "", "", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "Bravo", "", "", "", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "Zebra Fund", "", "", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "Zebra Fund", "", "", "owner").len(),
            1
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "YEAR_2024", "", "owner").len(),
            2
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "YEAR_2024", "", "owner").len(),
            0
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "", "", "amount")[0].symbol,
            "VOO"
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "", "", "amount")[0].symbol,
            "VOO"
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "", "", "owner")[0].symbol,
            "VOO"
        );
        assert_eq!(
            filter_legacy_summary_rows(&summaries, "", "", "", "", "instrument")[0].symbol,
            "VOO"
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "", "", "owner")[0].symbol,
            "VOO"
        );
        assert_eq!(
            filter_legacy_monthly_rows(&monthly, "", "", "", "", "instrument")[0].symbol,
            "VOO"
        );
    }
}

#[component]
fn LegacySummaryTable(rows: Vec<LegacyDividendSummaryRow>) -> Element {
    let is_empty = rows.is_empty();

    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "年度／累積資料" }
                span { "{rows.len()} 筆，來自 dividend_legacy_summary" }
            }
            if is_empty {
                div { class: "empty-state", h3 { "目前沒有符合條件的年度／累積資料" } }
            } else {
                div { class: "table-wrap",
                table { class: "legacy-summary-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "代號" }
                            th { "商品" }
                            th { "期間類型" }
                            th { "原始標籤" }
                            th { "金額" }
                            th { "Excel 儲存格" }
                        }
                    }
                    tbody {
                        for row in rows {
                            LegacySummaryRowView { row }
                        }
                    }
                }
                }
            }
        }
    }
}

#[component]
fn LegacySummaryRowView(row: LegacyDividendSummaryRow) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { "{period_label_text(&row.period_label)}" }
            td { class: "mono", "{row.period_label}" }
            td { class: "number strong", "{money(row.amount)}" }
            td { class: "mono", "{row.source_cell}" }
        }
    }
}

#[component]
fn LegacyMonthlyTable(rows: Vec<LegacyDividendMonthlyRow>) -> Element {
    let is_empty = rows.is_empty();

    rsx! {
        section { class: "card table-card",
            div { class: "table-summary",
                strong { "月份資料" }
                span { "{rows.len()} 筆，來自 dividend_legacy_monthly" }
            }
            if is_empty {
                div { class: "empty-state", h3 { "目前沒有符合條件的月份資料" } }
            } else {
                div { class: "table-wrap",
                table { class: "legacy-monthly-table",
                    thead {
                        tr {
                            th { "所有權人" }
                            th { "代號" }
                            th { "商品" }
                            th { "資料類型" }
                            th { "原始標籤" }
                            th { "月份" }
                            th { "金額" }
                            th { "Excel 儲存格" }
                        }
                    }
                    tbody {
                        for row in rows {
                            LegacyMonthlyRowView { row }
                        }
                    }
                }
                }
            }
        }
    }
}

#[component]
fn LegacyMonthlyRowView(row: LegacyDividendMonthlyRow) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { class: "mono", "{row.symbol}" }
            td { class: "name-cell", "{row.instrument_name}" }
            td { "{series_type_text(&row.series_type)}" }
            td { class: "mono", "{row.series_type}" }
            td { class: "number", "{row.month_num} 月" }
            td { class: "number strong", "{money(row.amount)}" }
            td { class: "mono", "{row.source_cell}" }
        }
    }
}

fn period_label_text(label: &str) -> &'static str {
    match label {
        "YEAR_2023" => "2023 年股息總額",
        "YEAR_2024" => "2024 年股息總額",
        "THROUGH_PREVIOUS_YEAR" => "截至上一年度累積",
        "TOTAL_CUMULATIVE" => "總累積",
        "CURRENT_YEAR_TO_DATE" => "今年度累積",
        _ => "其他",
    }
}

fn series_type_text(series_type: &str) -> &'static str {
    match series_type {
        "ACTUAL_CURRENT_YEAR" => "當年度實際月份股息",
        "FORECAST_AVERAGE" => "預估／平均月份配息",
        _ => "其他",
    }
}

fn select_option_label(value: &str) -> &str {
    match value {
        "FUND" => "基金",
        "STOCK" => "股票",
        "BOND" => "債券",
        "EQUITY" => "股票",
        "DOMESTIC" => "國內",
        "FOREIGN" => "國外",
        "BROKERAGE_CASH" => "證券戶現金",
        "DEMAND_DEPOSIT" => "活期存款",
        "FOREIGN_DEMAND_DEPOSIT" => "外幣活存",
        "FOREIGN_TIME_DEPOSIT" => "外幣定存",
        "SETTLEMENT_CASH" => "交割款",
        "TIME_DEPOSIT" => "定期存款",
        "CURRENT_YEAR_TO_DATE" => "今年度累積",
        "THROUGH_PREVIOUS_YEAR" => "截至上一年度累積",
        "TOTAL_CUMULATIVE" => "總累積",
        "YEAR_2023" => "2023 年股息總額",
        "YEAR_2024" => "2024 年股息總額",
        "ACTUAL_CURRENT_YEAR" => "當年度實際月份股息",
        "FORECAST_AVERAGE" => "預估／平均月份配息",
        _ => value,
    }
}

fn select_option_display(value: String, translate: bool) -> (String, String) {
    let label = if translate {
        select_option_label(&value).to_string()
    } else {
        value.clone()
    };
    (value, label)
}

#[cfg(test)]
mod select_option_label_tests {
    use super::{select_option_display, select_option_label};

    #[test]
    fn translates_internal_filter_codes() {
        for (value, expected) in [
            ("FUND", "基金"),
            ("STOCK", "股票"),
            ("BOND", "債券"),
            ("EQUITY", "股票"),
            ("DOMESTIC", "國內"),
            ("FOREIGN", "國外"),
            ("BROKERAGE_CASH", "證券戶現金"),
            ("DEMAND_DEPOSIT", "活期存款"),
            ("FOREIGN_DEMAND_DEPOSIT", "外幣活存"),
            ("FOREIGN_TIME_DEPOSIT", "外幣定存"),
            ("SETTLEMENT_CASH", "交割款"),
            ("TIME_DEPOSIT", "定期存款"),
            ("CURRENT_YEAR_TO_DATE", "今年度累積"),
            ("THROUGH_PREVIOUS_YEAR", "截至上一年度累積"),
            ("TOTAL_CUMULATIVE", "總累積"),
            ("YEAR_2023", "2023 年股息總額"),
            ("YEAR_2024", "2024 年股息總額"),
            ("ACTUAL_CURRENT_YEAR", "當年度實際月份股息"),
            ("FORECAST_AVERAGE", "預估／平均月份配息"),
        ] {
            assert_eq!(select_option_label(value), expected);
        }
    }

    #[test]
    fn preserves_standard_financial_codes_and_unknown_values() {
        assert_eq!(select_option_label("ETF"), "ETF");
        assert_eq!(select_option_label("USD"), "USD");
        assert_eq!(select_option_label("自訂選項"), "自訂選項");
    }

    #[test]
    fn keeps_raw_option_values_for_filtering() {
        assert_eq!(
            select_option_display("FOREIGN_TIME_DEPOSIT".to_string(), true),
            ("FOREIGN_TIME_DEPOSIT".to_string(), "外幣定存".to_string())
        );
    }

    #[test]
    fn preserves_dynamic_option_labels() {
        assert_eq!(
            select_option_display("FUND".to_string(), false),
            ("FUND".to_string(), "FUND".to_string())
        );
    }
}

#[component]
fn SelectFilter(
    label: String,
    value: String,
    options: Vec<String>,
    translate_options: bool,
    on_change: EventHandler<String>,
) -> Element {
    let option_displays = options
        .into_iter()
        .map(|option| select_option_display(option, translate_options))
        .collect::<Vec<_>>();

    rsx! {
        div { class: "filter-field",
            span { "{label}" }
            if option_displays.len() >= SEARCHABLE_SELECT_MIN_OPTIONS {
                SearchableSelect {
                    label,
                    value,
                    options: option_displays,
                    empty_label: Some("全部".to_string()),
                    on_change,
                }
            } else {
                select {
                    value: "{value}",
                    oninput: move |event| on_change.call(event.value()),
                    option { value: "", "全部" }
                    for (option_value, option_label) in option_displays {
                        option { value: "{option_value}", "{option_label}" }
                    }
                }
            }
        }
    }
}

const SEARCHABLE_SELECT_MIN_OPTIONS: usize = 10;

#[component]
fn SearchableSelect(
    label: String,
    value: String,
    options: Vec<(String, String)>,
    #[props(default)] empty_label: Option<String>,
    #[props(default = false)] disabled: bool,
    on_change: EventHandler<String>,
) -> Element {
    let mut query = use_signal(String::new);
    let mut is_open = use_signal(|| false);
    let selected_label = searchable_select_label(&value, &options, empty_label.as_deref());
    let visible_options = searchable_select_options(&query(), &options, empty_label.as_deref());
    let search_placeholder = format!("搜尋{label}");

    rsx! {
        div { class: if is_open() { "searchable-select open" } else { "searchable-select" },
            button {
                r#type: "button",
                class: "searchable-select-trigger",
                aria_haspopup: "listbox",
                aria_expanded: is_open(),
                disabled,
                onclick: move |_| {
                    if disabled {
                        return;
                    }
                    is_open.toggle();
                    query.set(String::new());
                },
                span { class: "searchable-select-trigger-label", title: "{selected_label}", "{selected_label}" }
                span { class: "searchable-select-trigger-icon", aria_hidden: "true", "▾" }
            }
            if is_open() {
                div {
                    class: "searchable-select-dismiss-layer",
                    aria_hidden: "true",
                    onclick: move |_| {
                        is_open.set(false);
                        query.set(String::new());
                    },
                }
                div {
                    class: "searchable-select-popover",
                    role: "dialog",
                    aria_label: "選擇{label}",
                    onkeydown: move |event| {
                        if event.key() == Key::Escape {
                            event.prevent_default();
                            is_open.set(false);
                            query.set(String::new());
                        }
                    },
                    input {
                        r#type: "search",
                        value: "{query}",
                        placeholder: "{search_placeholder}",
                        aria_label: "搜尋{label}",
                        disabled,
                        oninput: move |event| query.set(event.value()),
                    }
                    div { class: "searchable-select-options", role: "listbox", aria_label: "{label}",
                        if visible_options.is_empty() {
                            div { class: "searchable-select-empty", "沒有符合的選項" }
                        } else {
                            for (option_value, option_label) in visible_options {
                                button {
                                    key: "{option_value}",
                                    r#type: "button",
                                    class: if option_value == value { "searchable-select-option selected" } else { "searchable-select-option" },
                                    role: "option",
                                    aria_selected: option_value == value,
                                    title: "{option_label}",
                                    disabled,
                                    onclick: move |_| {
                                        on_change.call(option_value.clone());
                                        is_open.set(false);
                                        query.set(String::new());
                                    },
                                    if option_value == value {
                                        span { class: "searchable-select-option-check", aria_hidden: "true", "✓" }
                                    }
                                    span { class: "searchable-select-option-label", "{option_label}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn searchable_select_label(
    value: &str,
    options: &[(String, String)],
    empty_label: Option<&str>,
) -> String {
    if value.is_empty() {
        return empty_label.unwrap_or("未選擇").to_string();
    }

    options
        .iter()
        .find(|(option_value, _)| option_value == value)
        .map(|(_, option_label)| option_label.clone())
        .unwrap_or_else(|| value.to_string())
}

fn searchable_select_options(
    query: &str,
    options: &[(String, String)],
    empty_label: Option<&str>,
) -> Vec<(String, String)> {
    let query = query.trim().to_lowercase();
    let mut visible_options = empty_label
        .filter(|label| query.is_empty() || label.to_lowercase().contains(&query))
        .map(|label| vec![(String::new(), label.to_string())])
        .unwrap_or_default();
    visible_options.extend(
        options
            .iter()
            .filter(|(value, label)| {
                query.is_empty()
                    || value.to_lowercase().contains(&query)
                    || label.to_lowercase().contains(&query)
            })
            .cloned(),
    );
    visible_options
}

#[cfg(test)]
mod searchable_select_tests {
    use super::{searchable_select_label, searchable_select_options};

    #[test]
    fn searches_values_and_labels_without_case_sensitivity() {
        let options = vec![
            ("1".to_string(), "余俊霆 / 證券帳戶".to_string()),
            ("VOO".to_string(), "VOO Vanguard S&P 500 (USD)".to_string()),
        ];

        assert_eq!(
            searchable_select_options("vanguard", &options, Some("全部")),
            vec![("VOO".to_string(), "VOO Vanguard S&P 500 (USD)".to_string())]
        );
        assert_eq!(
            searchable_select_options("voo", &options, Some("全部")),
            vec![("VOO".to_string(), "VOO Vanguard S&P 500 (USD)".to_string())]
        );
    }

    #[test]
    fn includes_all_option_only_for_empty_search() {
        let options = vec![("1".to_string(), "元大南屯".to_string())];

        assert_eq!(
            searchable_select_options("", &options, Some("全部")),
            vec![
                (String::new(), "全部".to_string()),
                ("1".to_string(), "元大南屯".to_string()),
            ]
        );
        assert_eq!(
            searchable_select_options("元大", &options, Some("全部")),
            vec![("1".to_string(), "元大南屯".to_string())]
        );
    }

    #[test]
    fn resolves_labels_without_losing_the_underlying_value() {
        let options = vec![("42".to_string(), "星展-美金定存".to_string())];

        assert_eq!(
            searchable_select_label("42", &options, None),
            "星展-美金定存"
        );
        assert_eq!(searchable_select_label("", &options, Some("全部")), "全部");
        assert_eq!(searchable_select_label("99", &options, None), "99");
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_dividend_receipt_save(
    input: DividendReceiptInput,
) -> Result<(), crate::error::AppError> {
    tokio::task::spawn_blocking(move || insert_manual_dividend_receipt(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("股息新增工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_dividend_receipt_save(
    input: DividendReceiptInput,
) -> Result<(), crate::error::AppError> {
    insert_manual_dividend_receipt(input)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_dividend_receipt_update(
    input: DividendReceiptUpdateInput,
) -> Result<(), crate::error::AppError> {
    tokio::task::spawn_blocking(move || update_manual_dividend_receipt(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("股息更新工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_dividend_receipt_update(
    input: DividendReceiptUpdateInput,
) -> Result<(), crate::error::AppError> {
    update_manual_dividend_receipt(input)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_dividend_receipt_delete(
    input: DividendReceiptDeleteInput,
) -> Result<(), crate::error::AppError> {
    tokio::task::spawn_blocking(move || delete_manual_dividend_receipt(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("股息刪除工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_dividend_receipt_delete(
    input: DividendReceiptDeleteInput,
) -> Result<(), crate::error::AppError> {
    delete_manual_dividend_receipt(input)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_account_create(input: AccountCreateInput) -> Result<i64, crate::error::AppError> {
    tokio::task::spawn_blocking(move || create_manual_account(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("帳戶新增工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_account_create(input: AccountCreateInput) -> Result<i64, crate::error::AppError> {
    create_manual_account(input)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_instrument_create(
    input: InstrumentCreateInput,
) -> Result<i64, crate::error::AppError> {
    tokio::task::spawn_blocking(move || create_manual_instrument(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("商品新增工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_instrument_create(
    input: InstrumentCreateInput,
) -> Result<i64, crate::error::AppError> {
    create_manual_instrument(input)
}

fn unique_strings<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut values = values
        .filter(|value| !value.is_empty() && *value != "-")
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_batch_price_save(input: BatchPriceInput) -> Result<usize, crate::error::AppError> {
    tokio::task::spawn_blocking(move || upsert_manual_prices_batch(input))
        .await
        .map_err(|error| {
            crate::error::AppError::Validation(format!("批次市價儲存工作失敗：{error}"))
        })?
}

#[cfg(target_arch = "wasm32")]
async fn run_batch_price_save(input: BatchPriceInput) -> Result<usize, crate::error::AppError> {
    upsert_manual_prices_batch(input)
}

#[cfg(not(target_arch = "wasm32"))]
async fn run_exchange_rate_save(input: ExchangeRateInput) -> Result<(), crate::error::AppError> {
    tokio::task::spawn_blocking(move || upsert_manual_exchange_rate(input))
        .await
        .map_err(|error| crate::error::AppError::Validation(format!("匯率儲存工作失敗：{error}")))?
}

#[cfg(target_arch = "wasm32")]
async fn run_exchange_rate_save(input: ExchangeRateInput) -> Result<(), crate::error::AppError> {
    upsert_manual_exchange_rate(input)
}

fn build_quick_price_update_rows(rows: &[HoldingMetric]) -> Vec<QuickPriceUpdateRow> {
    #[derive(Clone)]
    struct AggregateRow {
        row: QuickPriceUpdateRow,
        account_ids: HashSet<i64>,
    }

    let mut deduped = BTreeMap::<i64, AggregateRow>::new();

    for row in rows {
        let entry = deduped
            .entry(row.instrument_id)
            .or_insert_with(|| AggregateRow {
                row: QuickPriceUpdateRow {
                    instrument_id: row.instrument_id,
                    symbol: row.symbol.clone(),
                    instrument_name: row.instrument_name.clone(),
                    currency_code: row
                        .market_price_currency_code
                        .clone()
                        .unwrap_or_else(|| row.trading_currency_code.clone()),
                    latest_price: row.market_price,
                    latest_price_date: row.market_price_date.clone(),
                    holding_account_count: 0,
                },
                account_ids: HashSet::new(),
            });

        entry.account_ids.insert(row.account_id);

        if row.market_price_date > entry.row.latest_price_date {
            entry.row.latest_price = row.market_price;
            entry.row.latest_price_date = row.market_price_date.clone();
            entry.row.currency_code = row
                .market_price_currency_code
                .clone()
                .unwrap_or_else(|| row.trading_currency_code.clone());
        }
    }

    let mut output = deduped
        .into_values()
        .map(|mut entry| {
            entry.row.holding_account_count = entry.account_ids.len();
            entry.row
        })
        .collect::<Vec<_>>();

    output.sort_by(|left, right| {
        left.symbol
            .cmp(&right.symbol)
            .then(left.instrument_name.cmp(&right.instrument_name))
    });
    output
}

fn compare_optional_desc(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    right
        .unwrap_or(f64::NEG_INFINITY)
        .partial_cmp(&left.unwrap_or(f64::NEG_INFINITY))
        .unwrap_or(std::cmp::Ordering::Equal)
}

const HOLDING_COLUMNS: &[(&str, &str)] = &[
    ("owner", "所有權人"),
    ("account", "證券帳戶"),
    ("account_number", "帳戶號碼"),
    ("symbol", "代號"),
    ("instrument", "商品名稱"),
    ("instrument_type", "類型"),
    ("asset_class", "資產類別"),
    ("region", "區域"),
    ("quantity", "數量"),
    ("average_cost", "平均成本（含買入手續費）"),
    ("market_price", "市價"),
    ("total_cost", "總成本"),
    ("market_value", "市值（毛額）"),
    ("liquidation_value", "預估清算淨值"),
    ("profit", "未實現損益"),
    ("return_rate", "損益率"),
    ("estimated_dividend", "預估年配息"),
    ("estimated_yield", "預估殖利率"),
    ("updated_at", "更新日"),
];

#[cfg(test)]
fn default_visible_holding_columns() -> HashSet<String> {
    HOLDING_COLUMNS
        .iter()
        .map(|(column_id, _)| (*column_id).to_string())
        .collect()
}

fn holding_column_ids() -> Vec<&'static str> {
    HOLDING_COLUMNS
        .iter()
        .map(|(column_id, _)| *column_id)
        .collect()
}

fn is_holding_column_visible(visible_columns: &HashSet<String>, column_id: &str) -> bool {
    visible_columns.contains(column_id)
}

#[derive(Default)]
struct HoldingReportTotals {
    total_cost_by_currency: BTreeMap<String, f64>,
    market_value_by_currency: BTreeMap<String, f64>,
    liquidation_value_by_currency: BTreeMap<String, f64>,
    unrealized_profit_by_currency: BTreeMap<String, f64>,
}

fn build_holding_report_totals(rows: &[HoldingMetric]) -> HoldingReportTotals {
    let mut totals = HoldingReportTotals::default();
    for row in rows {
        let market_currency_code = row
            .market_price_currency_code
            .as_deref()
            .unwrap_or(&row.trading_currency_code)
            .to_string();
        if let Some(total_cost) = row.total_cost {
            *totals
                .total_cost_by_currency
                .entry(row.cost_currency_code.clone())
                .or_default() += total_cost;
        }
        if let Some(market_value) = row.market_value {
            *totals
                .market_value_by_currency
                .entry(market_currency_code.clone())
                .or_default() += market_value;
        }
        if let Some(liquidation_value) = row.liquidation_value {
            *totals
                .liquidation_value_by_currency
                .entry(market_currency_code.clone())
                .or_default() += liquidation_value;
        }
        if row.cost_currency_code == market_currency_code {
            let Some(unrealized_profit) = row.unrealized_profit else {
                continue;
            };
            *totals
                .unrealized_profit_by_currency
                .entry(market_currency_code)
                .or_default() += unrealized_profit;
        }
    }
    totals
}

fn set_holding_column_visibility(
    visible_columns: &mut HashSet<String>,
    column_id: String,
    is_visible: bool,
) {
    if is_visible {
        visible_columns.insert(column_id);
    } else {
        visible_columns.remove(&column_id);
    }
}

#[cfg(test)]
mod holding_column_tests {
    use super::*;

    #[test]
    fn default_columns_include_all_selectable_holding_data() {
        let visible_columns = default_visible_holding_columns();

        assert_eq!(visible_columns.len(), HOLDING_COLUMNS.len());
        assert!(is_holding_column_visible(&visible_columns, "owner"));
        assert!(is_holding_column_visible(
            &visible_columns,
            "estimated_yield"
        ));
        assert!(!is_holding_column_visible(&visible_columns, "actions"));
    }

    #[test]
    fn updates_selected_column_visibility() {
        let mut visible_columns = default_visible_holding_columns();

        set_holding_column_visibility(&mut visible_columns, "market_price".to_string(), false);
        assert!(!is_holding_column_visible(&visible_columns, "market_price"));

        set_holding_column_visibility(&mut visible_columns, "market_price".to_string(), true);
        assert!(is_holding_column_visible(&visible_columns, "market_price"));
    }

    #[test]
    fn groups_holding_totals_by_market_currency() {
        let rows = vec![
            sample_holding_metric("USD", Some(2.0), Some(100.0)),
            sample_holding_metric("USD", Some(3.0), Some(150.0)),
            sample_holding_metric("NTD", Some(1.0), Some(50.0)),
        ];

        let totals = build_holding_report_totals(&rows);

        assert_eq!(totals.market_value_by_currency["USD"], 250.0);
        assert_eq!(totals.market_value_by_currency["NTD"], 50.0);
    }

    #[test]
    fn restores_exact_fee_exclusive_cost_from_snapshot_text() {
        let row = sample_holding_metric("NTD", Some(1.0), Some(45.5648375));

        assert_eq!(fee_exclusive_average_cost(&row), "45.5");
    }

    fn sample_holding_metric(
        currency_code: &str,
        quantity: Option<f64>,
        market_value: Option<f64>,
    ) -> HoldingMetric {
        HoldingMetric {
            holding_snapshot_id: 1,
            account_id: 1,
            instrument_id: 1,
            owner_name: "Owner".to_string(),
            account_name: "Account".to_string(),
            account_number: Some("001234567890".to_string()),
            symbol: "ABC".to_string(),
            instrument_name: "Example".to_string(),
            instrument_type: "ETF".to_string(),
            asset_class: "EQUITY".to_string(),
            region_type: "DOMESTIC".to_string(),
            trading_currency_code: currency_code.to_string(),
            cost_currency_code: currency_code.to_string(),
            snapshot_date: "2026-07-12".to_string(),
            quantity,
            average_cost: Some(45.5648375),
            average_cost_text: "45.5648375".to_string(),
            buy_fee_rate: Some(0.001425),
            applied_buy_fee_rate_text: "0.001425".to_string(),
            sell_fee_rate: Some(0.001425),
            sell_transaction_tax_rate: Some(0.003),
            note: String::new(),
            market_price_date: Some("2026-07-12".to_string()),
            market_price_currency_code: Some(currency_code.to_string()),
            market_price: None,
            total_cost: market_value,
            market_value,
            liquidation_value: market_value,
            unrealized_profit: market_value,
            unrealized_return_rate: None,
            dividend_effective_date: None,
            dividend_currency_code: None,
            estimated_annual_dividend_per_unit: None,
            payments_per_year: None,
            latest_dividend_per_unit: None,
            estimated_annual_dividend: None,
            estimated_yield_on_cost: None,
        }
    }
}

#[component]
fn HoldingsTable(rows: Vec<HoldingMetric>) -> Element {
    let mut editing_row = use_signal(|| None::<HoldingMetric>);
    let mut editing_dividend_row = use_signal(|| None::<HoldingMetric>);
    let mut status_message = use_signal(String::new);
    let preferences = use_context::<UiPreferences>();
    let mut owner_filter = use_signal(move || preference_value(&preferences(), HOLDINGS_OWNER));
    let mut type_filter = use_signal(move || preference_value(&preferences(), HOLDINGS_TYPE));
    let mut asset_class_filter =
        use_signal(move || preference_value(&preferences(), HOLDINGS_ASSET_CLASS));
    let mut region_filter = use_signal(move || preference_value(&preferences(), HOLDINGS_REGION));
    let mut search = use_signal(move || preference_value(&preferences(), HOLDINGS_SEARCH));
    let mut sort_by = use_signal(move || {
        valid_sort(
            &preference_value(&preferences(), HOLDINGS_SORT),
            &["market_value", "profit", "return"],
            "market_value",
        )
    });
    let mut visible_columns = use_signal(move || {
        parse_visible_columns(
            &preference_value(&preferences(), HOLDINGS_VISIBLE_COLUMNS),
            &holding_column_ids(),
        )
    });
    let mut is_column_picker_open = use_signal(|| false);
    let mut show_closed =
        use_signal(move || preference_value(&preferences(), HOLDINGS_SHOW_CLOSED) == "true");

    let owner_options = unique_strings(rows.iter().map(|row| row.owner_name.as_str()));
    let type_options = unique_strings(rows.iter().map(|row| row.instrument_type.as_str()));
    let asset_class_options = unique_strings(rows.iter().map(|row| row.asset_class.as_str()));
    let region_options = unique_strings(rows.iter().map(|row| row.region_type.as_str()));
    let owner_options_for_validation = owner_options.clone();
    let type_options_for_validation = type_options.clone();
    let asset_class_options_for_validation = asset_class_options.clone();
    let region_options_for_validation = region_options.clone();
    let owner_options_for_persistence = owner_options.clone();
    let type_options_for_persistence = type_options.clone();
    let asset_class_options_for_persistence = asset_class_options.clone();
    let region_options_for_persistence = region_options.clone();

    use_effect(move || {
        persist_preference(
            preferences,
            HOLDINGS_OWNER,
            valid_option(&owner_filter(), &owner_options_for_persistence, ""),
        )
    });
    use_effect(move || {
        persist_preference(preferences, HOLDINGS_SHOW_CLOSED, show_closed().to_string())
    });
    use_effect(move || {
        persist_preference(
            preferences,
            HOLDINGS_TYPE,
            valid_option(&type_filter(), &type_options_for_persistence, ""),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            HOLDINGS_ASSET_CLASS,
            valid_option(
                &asset_class_filter(),
                &asset_class_options_for_persistence,
                "",
            ),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            HOLDINGS_REGION,
            valid_option(&region_filter(), &region_options_for_persistence, ""),
        )
    });
    use_effect(move || persist_preference(preferences, HOLDINGS_SEARCH, search()));
    use_effect(move || persist_preference(preferences, HOLDINGS_SORT, sort_by()));
    use_effect(move || {
        persist_preference(
            preferences,
            HOLDINGS_VISIBLE_COLUMNS,
            serialize_visible_columns(&visible_columns(), &holding_column_ids()),
        )
    });
    use_effect(move || {
        let valid = valid_option(&owner_filter(), &owner_options_for_validation, "");
        if owner_filter() != valid {
            owner_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&type_filter(), &type_options_for_validation, "");
        if type_filter() != valid {
            type_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(
            &asset_class_filter(),
            &asset_class_options_for_validation,
            "",
        );
        if asset_class_filter() != valid {
            asset_class_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&region_filter(), &region_options_for_validation, "");
        if region_filter() != valid {
            region_filter.set(valid);
        }
    });

    let owner_value = owner_filter();
    let type_value = type_filter();
    let asset_class_value = asset_class_filter();
    let region_value = region_filter();
    let search_value = search().to_lowercase();
    let sort_value = sort_by();
    let visible_columns_value = visible_columns();
    let column_picker_options = HOLDING_COLUMNS
        .iter()
        .map(|(column_id, label)| {
            (
                (*column_id).to_string(),
                *label,
                is_holding_column_visible(&visible_columns_value, column_id),
            )
        })
        .collect::<Vec<_>>();
    let mut filtered_rows = rows
        .iter()
        .filter(|row| owner_value.is_empty() || row.owner_name == owner_value)
        .filter(|row| type_value.is_empty() || row.instrument_type == type_value)
        .filter(|row| asset_class_value.is_empty() || row.asset_class == asset_class_value)
        .filter(|row| region_value.is_empty() || row.region_type == region_value)
        .filter(|row| {
            search_value.is_empty()
                || row.symbol.to_lowercase().contains(&search_value)
                || row.instrument_name.to_lowercase().contains(&search_value)
        })
        .filter(|row| show_closed() || row.quantity.unwrap_or(0.0) > 0.0)
        .cloned()
        .collect::<Vec<_>>();
    let filtered_totals = build_holding_report_totals(&filtered_rows);

    filtered_rows.sort_by(|left, right| match sort_value.as_str() {
        "profit" => compare_optional_desc(left.unrealized_profit, right.unrealized_profit),
        "return" => {
            compare_optional_desc(left.unrealized_return_rate, right.unrealized_return_rate)
        }
        _ => compare_optional_desc(left.market_value, right.market_value),
    });

    rsx! {
        section { class: "card table-card",
            if !status_message().is_empty() {
                div { class: "status-message success", "{status_message}" }
            }
            div { class: "table-summary",
                strong { "{filtered_rows.len()} / {rows.len()} 筆持股" }
                span { "已依幣別彙總篩選結果" }
            }
            div { class: "filters",
                input {
                    placeholder: "搜尋商品名稱或代號",
                    value: "{search}",
                    oninput: move |event| search.set(event.value()),
                }
                SelectFilter { label: "所有權人".to_string(), value: owner_filter(), options: owner_options, translate_options: false, on_change: move |value| owner_filter.set(value) }
                SelectFilter { label: "商品類型".to_string(), value: type_filter(), options: type_options, translate_options: true, on_change: move |value| type_filter.set(value) }
                SelectFilter { label: "資產類別".to_string(), value: asset_class_filter(), options: asset_class_options, translate_options: true, on_change: move |value| asset_class_filter.set(value) }
                SelectFilter { label: "國內／國外".to_string(), value: region_filter(), options: region_options, translate_options: true, on_change: move |value| region_filter.set(value) }
                select {
                    value: "{sort_by}",
                    oninput: move |event| sort_by.set(event.value()),
                    option { value: "market_value", "依市值排序" }
                    option { value: "profit", "依損益排序" }
                    option { value: "return", "依報酬率排序" }
                }
                label { class: "column-toggle",
                    input {
                        r#type: "checkbox",
                        checked: show_closed(),
                        onchange: move |event| show_closed.set(event.checked()),
                    }
                    span { "顯示已清倉商品" }
                }
                button {
                    r#type: "button",
                    onclick: move |_| {
                        owner_filter.set(String::new());
                        type_filter.set(String::new());
                        asset_class_filter.set(String::new());
                        region_filter.set(String::new());
                        search.set(String::new());
                        sort_by.set("market_value".to_string());
                        show_closed.set(false);
                    },
                    "清除篩選"
                }
                button {
                    r#type: "button",
                    class: "ghost-button",
                    aria_expanded: is_column_picker_open(),
                    aria_controls: "holding-column-picker",
                    onclick: move |_| is_column_picker_open.toggle(),
                    "顯示欄位"
                }
            }
            div {
                id: "holding-column-picker",
                class: "column-picker",
                hidden: !is_column_picker_open(),
                for (column_id, label, is_visible) in column_picker_options {
                    label { class: "column-toggle",
                        input {
                            r#type: "checkbox",
                            checked: is_visible,
                            onchange: move |event| {
                                visible_columns.with_mut(|columns| {
                                    set_holding_column_visibility(
                                        columns,
                                        column_id.clone(),
                                        event.checked(),
                                    );
                                });
                            },
                        }
                        span { "{label}" }
                    }
                }
            }
            if filtered_rows.is_empty() {
                div { class: "empty-state", h3 { "目前沒有符合條件的持股資料" } }
            } else {
                div { class: "table-wrap",
                    table { class: "holdings-table",
                        thead {
                            tr {
                                if is_holding_column_visible(&visible_columns_value, "owner") { th { "所有權人" } }
                                if is_holding_column_visible(&visible_columns_value, "account") { th { "證券帳戶" } }
                                if is_holding_column_visible(&visible_columns_value, "account_number") { th { "帳戶號碼" } }
                                if is_holding_column_visible(&visible_columns_value, "symbol") { th { "代號" } }
                                if is_holding_column_visible(&visible_columns_value, "instrument") { th { "商品名稱" } }
                                if is_holding_column_visible(&visible_columns_value, "instrument_type") { th { "類型" } }
                                if is_holding_column_visible(&visible_columns_value, "asset_class") { th { "資產類別" } }
                                if is_holding_column_visible(&visible_columns_value, "region") { th { "區域" } }
                                if is_holding_column_visible(&visible_columns_value, "quantity") { th { "數量" } }
                                if is_holding_column_visible(&visible_columns_value, "average_cost") { th { "平均成本（含買入手續費）" } }
                                if is_holding_column_visible(&visible_columns_value, "market_price") { th { "市價" } }
                                if is_holding_column_visible(&visible_columns_value, "total_cost") { th { "總成本" } }
                                if is_holding_column_visible(&visible_columns_value, "market_value") { th { "市值（毛額）" } }
                                if is_holding_column_visible(&visible_columns_value, "liquidation_value") { th { "預估清算淨值" } }
                                if is_holding_column_visible(&visible_columns_value, "profit") { th { "未實現損益" } }
                                if is_holding_column_visible(&visible_columns_value, "return_rate") { th { "損益率" } }
                                if is_holding_column_visible(&visible_columns_value, "estimated_dividend") { th { "預估年配息" } }
                                if is_holding_column_visible(&visible_columns_value, "estimated_yield") { th { "預估殖利率" } }
                                if is_holding_column_visible(&visible_columns_value, "updated_at") { th { "更新日" } }
                                th { "操作" }
                            }
                        }
                        tbody {
                            for row in filtered_rows {
                                HoldingRow {
                                    row: row.clone(),
                                    visible_columns: visible_columns_value.clone(),
                                    on_edit: move |holding| {
                                        status_message.set(String::new());
                                        editing_row.set(Some(holding));
                                    },
                                    on_dividend_edit: move |holding| {
                                        status_message.set(String::new());
                                        editing_dividend_row.set(Some(holding));
                                    },
                                }
                            }
                        }
                    }
                }
                div { class: "holding-totals",
                    strong { "篩選結果合計" }
                    for (currency_code, total) in filtered_totals.total_cost_by_currency {
                        div { class: "holding-total-row",
                            span { class: "mono", "{currency_code}" }
                            span { "總成本：{money(Some(total))}" }
                        }
                    }
                    for (currency_code, total) in filtered_totals.market_value_by_currency {
                        div { class: "holding-total-row",
                            span { class: "mono", "{currency_code}" }
                            span { "市值（毛額）：{money(Some(total))}" }
                        }
                    }
                    for (currency_code, total) in filtered_totals.liquidation_value_by_currency {
                        div { class: "holding-total-row",
                            span { class: "mono", "{currency_code}" }
                            span { "預估清算淨值：{money(Some(total))}" }
                        }
                    }
                    for (currency_code, total) in filtered_totals.unrealized_profit_by_currency {
                        div { class: "holding-total-row",
                            span { class: "mono", "{currency_code}" }
                            span { "未實現損益：{money(Some(total))}" }
                        }
                    }
                }
            }
            if let Some(row) = editing_row() {
                HoldingEditModal {
                    row,
                    on_close: move |_| editing_row.set(None),
                    on_saved: move |message| {
                        status_message.set(message);
                        editing_row.set(None);
                    },
                }
            }
            if let Some(row) = editing_dividend_row() {
                HoldingDividendAssumptionModal {
                    row,
                    on_close: move |_| editing_dividend_row.set(None),
                    on_saved: move |message| {
                        status_message.set(message);
                        editing_dividend_row.set(None);
                    },
                }
            }
        }
    }
}

#[component]
fn AccountAssetsTable(rows: Vec<AccountAsset>) -> Element {
    let mut editing_row = use_signal(|| None::<AccountAsset>);
    let mut status_message = use_signal(String::new);
    let preferences = use_context::<UiPreferences>();
    let mut owner_filter = use_signal(move || preference_value(&preferences(), ACCOUNTS_OWNER));
    let mut institution_filter =
        use_signal(move || preference_value(&preferences(), ACCOUNTS_INSTITUTION));
    let mut asset_type_filter =
        use_signal(move || preference_value(&preferences(), ACCOUNTS_ASSET_TYPE));
    let mut currency_filter =
        use_signal(move || preference_value(&preferences(), ACCOUNTS_CURRENCY));
    let mut search = use_signal(move || preference_value(&preferences(), ACCOUNTS_SEARCH));
    let mut sort_by = use_signal(move || {
        valid_sort(
            &preference_value(&preferences(), ACCOUNTS_SORT),
            &["value", "owner", "institution", "asset_type"],
            "value",
        )
    });

    let owner_options = unique_strings(rows.iter().map(|row| row.owner_name.as_str()));
    let institution_options = unique_strings(rows.iter().map(|row| row.institution_name.as_str()));
    let asset_type_options = unique_strings(rows.iter().map(|row| row.asset_type.as_str()));
    let currency_options = unique_strings(rows.iter().map(|row| row.currency_code.as_str()));
    let owner_options_for_validation = owner_options.clone();
    let institution_options_for_validation = institution_options.clone();
    let asset_type_options_for_validation = asset_type_options.clone();
    let currency_options_for_validation = currency_options.clone();
    let owner_options_for_persistence = owner_options.clone();
    let institution_options_for_persistence = institution_options.clone();
    let asset_type_options_for_persistence = asset_type_options.clone();
    let currency_options_for_persistence = currency_options.clone();

    use_effect(move || {
        persist_preference(
            preferences,
            ACCOUNTS_OWNER,
            valid_option(&owner_filter(), &owner_options_for_persistence, ""),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            ACCOUNTS_INSTITUTION,
            valid_option(
                &institution_filter(),
                &institution_options_for_persistence,
                "",
            ),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            ACCOUNTS_ASSET_TYPE,
            valid_option(
                &asset_type_filter(),
                &asset_type_options_for_persistence,
                "",
            ),
        )
    });
    use_effect(move || {
        persist_preference(
            preferences,
            ACCOUNTS_CURRENCY,
            valid_option(&currency_filter(), &currency_options_for_persistence, ""),
        )
    });
    use_effect(move || persist_preference(preferences, ACCOUNTS_SEARCH, search()));
    use_effect(move || persist_preference(preferences, ACCOUNTS_SORT, sort_by()));
    use_effect(move || {
        let valid = valid_option(&owner_filter(), &owner_options_for_validation, "");
        if owner_filter() != valid {
            owner_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(
            &institution_filter(),
            &institution_options_for_validation,
            "",
        );
        if institution_filter() != valid {
            institution_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&asset_type_filter(), &asset_type_options_for_validation, "");
        if asset_type_filter() != valid {
            asset_type_filter.set(valid);
        }
    });
    use_effect(move || {
        let valid = valid_option(&currency_filter(), &currency_options_for_validation, "");
        if currency_filter() != valid {
            currency_filter.set(valid);
        }
    });

    let owner_value = owner_filter();
    let institution_value = institution_filter();
    let asset_type_value = asset_type_filter();
    let currency_value = currency_filter();
    let search_value = search().to_lowercase();
    let sort_value = sort_by();
    let mut filtered_rows = rows
        .iter()
        .filter(|row| owner_value.is_empty() || row.owner_name == owner_value)
        .filter(|row| institution_value.is_empty() || row.institution_name == institution_value)
        .filter(|row| asset_type_value.is_empty() || row.asset_type == asset_type_value)
        .filter(|row| currency_value.is_empty() || row.currency_code == currency_value)
        .filter(|row| {
            search_value.is_empty() || row.account_name.to_lowercase().contains(&search_value)
        })
        .cloned()
        .collect::<Vec<_>>();
    filtered_rows.sort_by(|left, right| match sort_value.as_str() {
        "owner" => left.owner_name.cmp(&right.owner_name),
        "institution" => left.institution_name.cmp(&right.institution_name),
        "asset_type" => left.asset_type.cmp(&right.asset_type),
        _ => compare_optional_desc(left.current_value_ntd, right.current_value_ntd),
    });
    let filtered_total = filtered_rows
        .iter()
        .filter_map(|row| row.current_value_ntd)
        .sum::<f64>();

    rsx! {
        section { class: "card table-card",
            if !status_message().is_empty() {
                div { class: "status-message success", "{status_message}" }
            }
            div { class: "table-summary",
                strong { "{filtered_rows.len()} / {rows.len()} 筆帳戶資產" }
                span { "篩選後總額：{money(Some(filtered_total))}" }
            }
            div { class: "filters account-filters",
                label { class: "filter-field account-search-filter",
                    span { "帳戶名稱" }
                    input {
                        placeholder: "搜尋帳戶名稱",
                        value: "{search}",
                        oninput: move |event| search.set(event.value()),
                    }
                }
                SelectFilter { label: "所有權人".to_string(), value: owner_filter(), options: owner_options, translate_options: false, on_change: move |value| owner_filter.set(value) }
                SelectFilter { label: "金融機構".to_string(), value: institution_filter(), options: institution_options, translate_options: false, on_change: move |value| institution_filter.set(value) }
                SelectFilter { label: "資產類型".to_string(), value: asset_type_filter(), options: asset_type_options, translate_options: true, on_change: move |value| asset_type_filter.set(value) }
                SelectFilter { label: "幣別".to_string(), value: currency_filter(), options: currency_options, translate_options: false, on_change: move |value| currency_filter.set(value) }
                label { class: "filter-field account-sort-filter",
                    span { "排序" }
                    select {
                        value: "{sort_by}",
                        oninput: move |event| sort_by.set(event.value()),
                        option { value: "value", "依台幣價值排序" }
                        option { value: "owner", "依所有權人排序" }
                        option { value: "institution", "依金融機構排序" }
                        option { value: "asset_type", "依資產類型排序" }
                    }
                }
                button {
                    r#type: "button",
                    class: "filter-clear",
                    onclick: move |_| {
                        owner_filter.set(String::new());
                        institution_filter.set(String::new());
                        asset_type_filter.set(String::new());
                        currency_filter.set(String::new());
                        search.set(String::new());
                        sort_by.set("value".to_string());
                    },
                    "清除篩選"
                }
            }
            if filtered_rows.is_empty() {
                div { class: "empty-state", h3 { "目前沒有符合條件的帳戶資產資料" } }
            } else {
                div { class: "table-wrap",
                    table { class: "account-assets-table",
                        thead {
                            tr {
                                th { "所有權人" }
                                th { "金融機構" }
                                th { "帳戶名稱" }
                                th { "帳戶號碼" }
                                th { "帳戶類型" }
                                th { "資產類型" }
                                th { "幣別" }
                                th { "投入金額" }
                                th { "數量" }
                                th { "台幣價值" }
                                th { "更新日" }
                                th { "操作" }
                            }
                        }
                        tbody {
                            for row in filtered_rows {
                                AccountAssetRow {
                                    row: row.clone(),
                                    on_edit: move |asset| {
                                        status_message.set(String::new());
                                        editing_row.set(Some(asset));
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(row) = editing_row() {
            AccountAssetEditModal {
                asset: row,
                on_close: move |_| editing_row.set(None),
                on_saved: move |message| {
                    status_message.set(message);
                    editing_row.set(None);
                },
            }
        }
    }
}

#[component]
fn AccountAssetRow(row: AccountAsset, on_edit: EventHandler<AccountAsset>) -> Element {
    rsx! {
        tr {
            td { "{row.owner_name}" }
            td { "{row.institution_name}" }
            td { class: "name-cell", "{row.account_name}" }
            td { class: "mono", "{row.account_number.as_deref().unwrap_or(\"—\")}" }
            td { "{account_type_label(&row.account_type)}" }
            td { "{select_option_label(&row.asset_type)}" }
            td { class: "mono", "{row.currency_code}" }
            td { class: "number", "{decimal(row.invested_amount, 2)}" }
            td { class: "number", "{decimal(row.quantity, 2)}" }
            td { class: "number strong", "{money(row.current_value_ntd)}" }
            td { class: "mono", "{row.snapshot_date}" }
            td {
                button {
                    r#type: "button",
                    class: "inline-action",
                    onclick: move |_| on_edit.call(row.clone()),
                    "編輯"
                }
            }
        }
    }
}

fn account_type_label(account_type: &str) -> &str {
    match account_type {
        "BANK" => "銀行帳戶",
        "BROKERAGE" => "證券帳戶",
        _ => account_type,
    }
}

#[cfg(test)]
mod account_type_label_tests {
    use super::account_type_label;

    #[test]
    fn translates_known_account_types_without_hiding_unknown_values() {
        assert_eq!(account_type_label("BANK"), "銀行帳戶");
        assert_eq!(account_type_label("BROKERAGE"), "證券帳戶");
        assert_eq!(account_type_label("CUSTOM"), "CUSTOM");
    }
}

#[component]
fn AccountAssetEditModal(
    asset: AccountAsset,
    on_close: EventHandler<()>,
    on_saved: EventHandler<String>,
) -> Element {
    let mut data_version = use_context::<Signal<u64>>();
    let is_foreign = is_foreign_currency_asset(&asset.currency_code);

    let today = chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();

    let initial_form = AccountAssetEditForm {
        snapshot_date: if asset.snapshot_date == "-" {
            today
        } else {
            asset.snapshot_date.clone()
        },
        quantity: if is_foreign {
            asset.quantity_text.clone().unwrap_or_default()
        } else {
            String::new()
        },
        current_value_override: if is_foreign {
            String::new()
        } else {
            asset
                .current_value_override_text
                .clone()
                .unwrap_or_default()
        },
        invested_amount: asset.invested_amount_text.clone().unwrap_or_default(),
        note: asset.note.clone(),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());
    let mut snapshot_date = use_signal(|| initial_form.snapshot_date.clone());
    let mut quantity = use_signal(|| {
        if is_foreign {
            asset.quantity_text.clone().unwrap_or_default()
        } else {
            String::new()
        }
    });
    let mut current_value_override = use_signal(|| {
        if is_foreign {
            String::new()
        } else {
            asset
                .current_value_override_text
                .clone()
                .unwrap_or_default()
        }
    });
    let mut invested_amount = use_signal(|| asset.invested_amount_text.clone().unwrap_or_default());
    let mut note = use_signal(|| asset.note.clone());
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_close = use_signal(|| false);
    let currency_code_for_rate = asset.currency_code.clone();
    let display_currency_code = asset.currency_code.clone();
    let interaction_locked = is_saving();
    let is_dirty = AccountAssetEditForm {
        snapshot_date: snapshot_date(),
        quantity: quantity(),
        current_value_override: current_value_override(),
        invested_amount: invested_amount(),
        note: note(),
    } != initial_form_snapshot();

    let exchange_rate = use_resource(move || {
        let _ = data_version();
        let date = snapshot_date();
        let currency = currency_code_for_rate.clone();
        async move { load_applicable_exchange_rate(&currency, &date) }
    });

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card",
                div { class: "modal-header",
                    div {
                        h3 { "編輯帳戶資產" }
                        p { class: "modal-subtitle", "{asset.account_name} / {asset_type_label(&asset.asset_type)} {asset.currency_code}" }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            let reset_form = initial_form_snapshot();
                            snapshot_date.set(reset_form.snapshot_date);
                            quantity.set(reset_form.quantity);
                            current_value_override.set(reset_form.current_value_override);
                            invested_amount.set(reset_form.invested_amount);
                            note.set(reset_form.note);
                        },
                        disabled: interaction_locked,
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                div { class: "form-grid two-column",
                    div { class: "form-field",
                        span { "帳戶" }
                        div { class: "readonly-field", "{asset.account_name}" }
                    }
                    div { class: "form-field",
                        span { "帳戶號碼" }
                        div { class: "readonly-field mono", "{asset.account_number.as_deref().unwrap_or(\"—\")}" }
                    }
                    div { class: "form-field",
                        span { "資產類型" }
                        div { class: "readonly-field", "{asset_type_label(&asset.asset_type)}" }
                    }
                    div { class: "form-field",
                        span { "幣別" }
                        div { class: "readonly-field", "{asset.currency_code}" }
                    }
                    label { class: "form-field",
                        span { "資料日期" }
                        input {
                            r#type: "date",
                            value: "{snapshot_date}",
                            required: true,
                            oninput: move |event| snapshot_date.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    if is_foreign {
                        label { class: "form-field",
                            span { "外幣數量" }
                            input {
                                value: "{quantity}",
                                oninput: move |event| quantity.set(event.value()),
                                disabled: interaction_locked,
                                placeholder: "0",
                            }
                        }
                    } else {
                        label { class: "form-field",
                            span { "目前餘額" }
                            input {
                                value: "{current_value_override}",
                                oninput: move |event| current_value_override.set(event.value()),
                                disabled: interaction_locked,
                                placeholder: "0",
                            }
                        }
                    }
                    label { class: "form-field",
                        span { "投入金額" }
                        input {
                            value: "{invested_amount}",
                            oninput: move |event| invested_amount.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "選填",
                        }
                    }
                    if is_foreign {
                        div { class: "exchange-preview",
                            match exchange_rate() {
                                None => rsx! { span { class: "form-warning", "查詢匯率中..." } },
                                Some(Err(_)) => rsx! {
                                    span { class: "form-warning",
                                        "找不到 {display_currency_code}/NTD 適用匯率，外幣數量仍可儲存但台幣價值暫時無法計算。"
                                    }
                                },
                                Some(Ok(None)) => rsx! {
                                    span { class: "form-warning",
                                        "找不到 {display_currency_code}/NTD 適用匯率，外幣數量仍可儲存但台幣價值暫時無法計算。"
                                    }
                                },
                                Some(Ok(Some(preview))) => {
                                    let parsed_quantity = quantity()
                                        .trim()
                                        .parse::<rust_decimal::Decimal>()
                                        .ok();
                                    let parsed_rate = preview.rate_text
                                        .parse::<rust_decimal::Decimal>()
                                        .ok();
                                    let ntd_display = parsed_quantity
                                        .zip(parsed_rate)
                                        .map(|(q, r)| crate::format::money(Some(
                                            (q * r).to_string().parse::<f64>().unwrap_or(0.0)
                                        )))
                                        .unwrap_or_else(|| "-".to_string());

                                    rsx! {
                                        span { "適用匯率：{preview.rate_text}" }
                                        span { "匯率日期：{preview.rate_date}" }
                                        span { strong { "換算台幣：{ntd_display}" } }
                                    }
                                }
                            }
                        }
                    }
                    label { class: "form-field",
                        span { "備註" }
                        input {
                            value: "{note}",
                            oninput: move |event| note.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "選填",
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        disabled: interaction_locked,
                        "取消"
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            is_saving.set(true);
                            error_message.set(String::new());

                            let input = AccountAssetInput {
                                source_snapshot_id: Some(asset.snapshot_id),
                                account_id: asset.account_id,
                                snapshot_date: snapshot_date(),
                                asset_type: asset.asset_type.clone(),
                                currency_code: asset.currency_code.clone(),
                                quantity: quantity(),
                                invested_amount: invested_amount(),
                                current_value_override: current_value_override(),
                                note: note(),
                            };

                            match validate_account_asset_input(&input) {
                                Err(error) => {
                                    error_message.set(error.to_string());
                                    is_saving.set(false);
                                }
                                Ok(validated) => {
                                    match upsert_manual_account_asset(validated) {
                                        Ok(_snapshot_id) => {
                                            data_version.with_mut(|v| *v += 1);
                                            is_saving.set(false);
                                            confirm_close.set(false);
                                            on_saved.call(format!("{} {} 已更新", asset_type_label(&asset.asset_type), asset.account_name));
                                        }
                                        Err(error) => {
                                            error_message.set(format!("儲存失敗：{error}"));
                                            is_saving.set(false);
                                        }
                                    }
                                }
                            }
                        },
                        if is_saving() { "儲存中..." } else { "儲存" }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn HoldingRow(
    row: HoldingMetric,
    visible_columns: HashSet<String>,
    on_edit: EventHandler<HoldingMetric>,
    on_dividend_edit: EventHandler<HoldingMetric>,
) -> Element {
    let edit_row = row.clone();
    let dividend_row = row.clone();
    let profit_class = match row.unrealized_profit {
        Some(value) if value > 0.0 => "number positive",
        Some(value) if value < 0.0 => "number negative",
        _ => "number muted",
    };

    rsx! {
        tr {
            if is_holding_column_visible(&visible_columns, "owner") { td { "{row.owner_name}" } }
            if is_holding_column_visible(&visible_columns, "account") { td { "{row.account_name}" } }
            if is_holding_column_visible(&visible_columns, "account_number") { td { class: "mono", "{row.account_number.as_deref().unwrap_or(\"—\")}" } }
            if is_holding_column_visible(&visible_columns, "symbol") { td { class: "mono", "{row.symbol}" } }
            if is_holding_column_visible(&visible_columns, "instrument") { td { class: "name-cell", "{row.instrument_name}" } }
            if is_holding_column_visible(&visible_columns, "instrument_type") { td { "{select_option_label(&row.instrument_type)}" } }
            if is_holding_column_visible(&visible_columns, "asset_class") { td { "{select_option_label(&row.asset_class)}" } }
            if is_holding_column_visible(&visible_columns, "region") { td { "{select_option_label(&row.region_type)}" } }
            if is_holding_column_visible(&visible_columns, "quantity") { td { class: "number", "{decimal(row.quantity, 2)}" } }
            if is_holding_column_visible(&visible_columns, "average_cost") { td { class: "number", "{decimal(row.average_cost, 2)}" } }
            if is_holding_column_visible(&visible_columns, "market_price") { td { class: "number", "{decimal(row.market_price, 2)}" } }
            if is_holding_column_visible(&visible_columns, "total_cost") { td { class: "number", "{money(row.total_cost)}" } }
            if is_holding_column_visible(&visible_columns, "market_value") { td { class: "number strong", "{money(row.market_value)}" } }
            if is_holding_column_visible(&visible_columns, "liquidation_value") { td { class: "number strong", "{money(row.liquidation_value)}" } }
            if is_holding_column_visible(&visible_columns, "profit") { td { class: profit_class, "{money(row.unrealized_profit)}" } }
            if is_holding_column_visible(&visible_columns, "return_rate") { td { class: profit_class, "{percent(row.unrealized_return_rate)}" } }
            if is_holding_column_visible(&visible_columns, "estimated_dividend") { td { class: "number", "{money(row.estimated_annual_dividend)}" } }
            if is_holding_column_visible(&visible_columns, "estimated_yield") { td { class: "number", "{percent(row.estimated_yield_on_cost)}" } }
            if is_holding_column_visible(&visible_columns, "updated_at") { td { class: "mono", "{row.snapshot_date}" } }
            td {
                button {
                    r#type: "button",
                    class: "inline-action",
                    onclick: move |_| on_edit.call(edit_row.clone()),
                    "更新目前資料"
                }
                button {
                    r#type: "button",
                    class: "inline-action",
                    onclick: move |_| on_dividend_edit.call(dividend_row.clone()),
                    "編輯配息估計"
                }
            }
        }
    }
}

#[component]
fn HoldingEditModal(
    row: HoldingMetric,
    on_close: EventHandler<()>,
    on_saved: EventHandler<String>,
) -> Element {
    let mut data_version = use_context::<Signal<u64>>();
    let initial_form = HoldingEditForm {
        as_of_date: Some(row.snapshot_date.clone())
            .filter(|date| date != "-")
            .unwrap_or_default(),
        quantity_text: editable_number(row.quantity),
        average_cost_text: fee_exclusive_average_cost(&row),
        note: row.note.clone(),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());
    let mut as_of_date = use_signal(|| {
        Some(row.snapshot_date.clone())
            .filter(|date| date != "-")
            .unwrap_or_default()
    });
    let mut quantity_text = use_signal(|| editable_number(row.quantity));
    let mut average_cost_text = use_signal(|| fee_exclusive_average_cost(&row));
    let cost_currency_code = row.cost_currency_code.clone();
    let mut note = use_signal(|| row.note.clone());
    let mut saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_close = use_signal(|| false);
    let interaction_locked = saving();
    let is_dirty = HoldingEditForm {
        as_of_date: as_of_date(),
        quantity_text: quantity_text(),
        average_cost_text: average_cost_text(),
        note: note(),
    } != initial_form_snapshot();

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card holding-edit-modal",
                div { class: "modal-header",
                    div {
                        h3 { "更新持股狀態" }
                        p { class: "modal-subtitle", "{row.symbol} {row.instrument_name}" }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            let reset_form = initial_form_snapshot();
                            as_of_date.set(reset_form.as_of_date);
                            quantity_text.set(reset_form.quantity_text);
                            average_cost_text.set(reset_form.average_cost_text);
                            note.set(reset_form.note);
                        },
                        disabled: interaction_locked,
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                p { class: "modal-subtitle", "此設定會套用至所有持有此商品的帳戶。" }
                div { class: "form-grid two-column",
                    label {
                        span { "所有權人" }
                        div { class: "readonly-field", "{row.owner_name}" }
                    }
                    label {
                        span { "證券帳戶" }
                        div { class: "readonly-field", "{row.account_name}" }
                    }
                    label {
                        span { "帳戶號碼" }
                        div { class: "readonly-field mono", "{row.account_number.as_deref().unwrap_or(\"—\")}" }
                    }
                    label {
                        span { "商品代號" }
                        div { class: "readonly-field", "{row.symbol}" }
                    }
                    label {
                        span { "商品名稱" }
                        div { class: "readonly-field", "{row.instrument_name}" }
                    }
                    label {
                        span { "成本幣別" }
                        div { class: "readonly-field", "{cost_currency_code}" }
                    }
                    label {
                        span { "資料日期" }
                        input {
                            r#type: "date",
                            value: "{as_of_date}",
                            oninput: move |event| as_of_date.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    label {
                        span { "持有數量" }
                        input {
                            value: "{quantity_text}",
                            oninput: move |event| quantity_text.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    label {
                        span { "平均成本（未含買入手續費）" }
                        input {
                            value: "{average_cost_text}",
                            oninput: move |event| average_cost_text.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    label { class: "full-width",
                        span { "備註" }
                        textarea {
                            value: "{note}",
                            oninput: move |event| note.set(event.value()),
                            disabled: interaction_locked,
                            rows: "3",
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        disabled: interaction_locked,
                        "取消"
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            saving.set(true);
                            error_message.set(String::new());

                            let result = save_current_holding_state(CurrentHoldingStateInput {
                                account_id: row.account_id,
                                instrument_id: row.instrument_id,
                                as_of_date: as_of_date(),
                                quantity_text: quantity_text(),
                                average_cost_text: average_cost_text(),
                                currency_code: cost_currency_code.clone(),
                                note: note(),
                            });

                            saving.set(false);

                            match result {
                                Ok(()) => {
                                    data_version.with_mut(|value| *value += 1);
                                    confirm_close.set(false);
                                    on_saved.call(format!("{} 已更新", row.instrument_name));
                                }
                                Err(error) => {
                                    error_message.set(error.to_string());
                                }
                            }
                        },
                        if saving() { "儲存中..." } else { "儲存" }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn HoldingDividendAssumptionModal(
    row: HoldingMetric,
    on_close: EventHandler<()>,
    on_saved: EventHandler<String>,
) -> Element {
    let mut data_version = use_context::<Signal<u64>>();
    let dividend_currency_code = row
        .dividend_currency_code
        .clone()
        .unwrap_or_else(|| row.cost_currency_code.clone());
    let original_effective_date = row
        .dividend_effective_date
        .clone()
        .unwrap_or_else(|| row.snapshot_date.clone());
    let initial_form = HoldingDividendAssumptionForm {
        effective_date: row
            .dividend_effective_date
            .clone()
            .filter(|date| date != "-")
            .unwrap_or_else(|| row.snapshot_date.clone()),
        payments_per_year: row
            .payments_per_year
            .map(|value| value.to_string())
            .unwrap_or_default(),
        latest_dividend_per_unit: editable_number(row.latest_dividend_per_unit),
        estimated_annual_dividend_per_unit: editable_number(row.estimated_annual_dividend_per_unit),
    };
    let initial_form_snapshot = use_signal(|| initial_form.clone());
    let mut effective_date = use_signal(|| {
        row.dividend_effective_date
            .clone()
            .filter(|date| date != "-")
            .unwrap_or_else(|| row.snapshot_date.clone())
    });
    let mut payments_per_year = use_signal(|| {
        row.payments_per_year
            .map(|value| value.to_string())
            .unwrap_or_default()
    });
    let mut latest_dividend_per_unit = use_signal(|| editable_number(row.latest_dividend_per_unit));
    let mut estimated_annual_dividend_per_unit =
        use_signal(|| editable_number(row.estimated_annual_dividend_per_unit));
    let mut is_saving = use_signal(|| false);
    let mut error_message = use_signal(String::new);
    let mut confirm_close = use_signal(|| false);
    let interaction_locked = is_saving();
    let is_dirty = HoldingDividendAssumptionForm {
        effective_date: effective_date(),
        payments_per_year: payments_per_year(),
        latest_dividend_per_unit: latest_dividend_per_unit(),
        estimated_annual_dividend_per_unit: estimated_annual_dividend_per_unit(),
    } != initial_form_snapshot();

    rsx! {
        div { class: "modal-backdrop",
            div { class: "modal-card",
                div { class: "modal-header",
                    div {
                        h3 { "編輯配息估計" }
                        p { class: "modal-subtitle", "{row.account_name} / {row.symbol} {row.instrument_name}" }
                    }
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            error_message.set(String::new());
                            confirm_close.set(false);
                            let reset_form = initial_form_snapshot();
                            effective_date.set(reset_form.effective_date);
                            payments_per_year.set(reset_form.payments_per_year);
                            latest_dividend_per_unit.set(reset_form.latest_dividend_per_unit);
                            estimated_annual_dividend_per_unit
                                .set(reset_form.estimated_annual_dividend_per_unit);
                        },
                        disabled: interaction_locked,
                        "還原"
                    }
                }
                if !error_message().is_empty() {
                    div { class: "status-message error", "{error_message}" }
                }
                div { class: "form-grid two-column",
                    label { class: "form-field",
                        span { "生效日期" }
                        input {
                            r#type: "date",
                            value: "{effective_date}",
                            oninput: move |event| effective_date.set(event.value()),
                            disabled: interaction_locked,
                        }
                    }
                    label { class: "form-field",
                        span { "配息頻率" }
                        input {
                            value: "{payments_per_year}",
                            oninput: move |event| payments_per_year.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "4",
                        }
                    }
                    label { class: "form-field",
                        span { "最新每單位配息" }
                        input {
                            value: "{latest_dividend_per_unit}",
                            oninput: move |event| latest_dividend_per_unit.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "0",
                        }
                    }
                    label { class: "form-field",
                        span { "預估每單位年配息" }
                        input {
                            value: "{estimated_annual_dividend_per_unit}",
                            oninput: move |event| estimated_annual_dividend_per_unit.set(event.value()),
                            disabled: interaction_locked,
                            placeholder: "0",
                        }
                    }
                }
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "ghost-button",
                        onclick: move |_| {
                            if interaction_locked {
                                return;
                            }
                            if is_dirty {
                                confirm_close.set(true);
                                return;
                            }
                            on_close.call(());
                        },
                        disabled: interaction_locked,
                        "取消"
                    }
                    button {
                        r#type: "button",
                        class: "primary-button",
                        disabled: interaction_locked,
                        onclick: move |_| {
                            is_saving.set(true);
                            error_message.set(String::new());

                            let has_values = !payments_per_year().trim().is_empty()
                                || !latest_dividend_per_unit().trim().is_empty()
                                || !estimated_annual_dividend_per_unit().trim().is_empty();
                            if row.dividend_effective_date.is_none() && !has_values {
                                error_message.set("請至少輸入一項配息估計資料".to_string());
                                is_saving.set(false);
                                return;
                            }
                            if !has_values && effective_date() != original_effective_date {
                                error_message.set("請至少輸入一項配息估計資料".to_string());
                                is_saving.set(false);
                                return;
                            }

                            let result = save_dividend_assumption(DividendAssumptionInput {
                                instrument_id: row.instrument_id,
                                effective_date: effective_date(),
                                payments_per_year_text: payments_per_year(),
                                latest_dividend_per_unit_text: latest_dividend_per_unit(),
                                estimated_annual_dividend_per_unit_text: estimated_annual_dividend_per_unit(),
                                currency_code: dividend_currency_code.clone(),
                            });

                            is_saving.set(false);

                            match result {
                                Ok(()) => {
                                    data_version.with_mut(|value| *value += 1);
                                    confirm_close.set(false);
                                    on_saved.call(format!("{} 配息估計已更新", row.instrument_name));
                                }
                                Err(error) => {
                                    error_message.set(format!("儲存失敗：{error}"));
                                }
                            }
                        },
                        if is_saving() { "儲存中..." } else { "儲存" }
                    }
                }
                if confirm_close() {
                    div { class: "delete-confirmation",
                        p { "尚未儲存變更，確定要關閉嗎？" }
                        div { class: "modal-actions",
                            button {
                                r#type: "button",
                                class: "ghost-button",
                                disabled: interaction_locked,
                                onclick: move |_| confirm_close.set(false),
                                "繼續編輯"
                            }
                            button {
                                r#type: "button",
                                class: "danger-button",
                                disabled: interaction_locked,
                                onclick: move |_| {
                                    confirm_close.set(false);
                                    on_close.call(());
                                },
                                "確認關閉"
                            }
                        }
                    }
                }
            }
        }
    }
}

fn editable_number(value: Option<f64>) -> String {
    value
        .map(|number| {
            let text = format!("{number:.6}");
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        })
        .unwrap_or_default()
}

fn fee_exclusive_average_cost(row: &HoldingMetric) -> String {
    let fee_inclusive_cost =
        crate::decimal::parse_decimal_field("average_cost", &row.average_cost_text);
    let buy_fee_rate =
        crate::decimal::parse_decimal_field("buy_fee_rate", &row.applied_buy_fee_rate_text);
    match (fee_inclusive_cost, buy_fee_rate) {
        (Ok(cost), Ok(rate)) => {
            crate::decimal::normalize_decimal_text(cost / (rust_decimal::Decimal::ONE + rate))
        }
        _ => String::new(),
    }
}

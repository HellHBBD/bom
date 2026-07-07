#[derive(Clone, Debug, PartialEq)]
pub struct HoldingMetric {
    pub owner_name: String,
    pub account_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub region_type: String,
    pub snapshot_date: String,
    pub quantity: Option<f64>,
    pub average_cost: Option<f64>,
    pub market_price: Option<f64>,
    pub total_cost: Option<f64>,
    pub market_value: Option<f64>,
    pub unrealized_profit: Option<f64>,
    pub unrealized_return_rate: Option<f64>,
    pub estimated_annual_dividend: Option<f64>,
    pub estimated_yield_on_cost: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountAsset {
    pub owner_name: String,
    pub institution_name: String,
    pub account_name: String,
    pub account_type: String,
    pub asset_type: String,
    pub currency_code: String,
    pub invested_amount: Option<f64>,
    pub quantity: Option<f64>,
    pub current_value_ntd: Option<f64>,
    pub snapshot_date: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DashboardSummary {
    pub total_assets: Option<f64>,
    pub account_assets: Option<f64>,
    pub investment_assets: Option<f64>,
    pub account_asset_count: i64,
    pub holding_count: i64,
    pub account_asset_missing_value_count: i64,
    pub holding_missing_market_value_count: i64,
    pub holding_missing_dividend_count: i64,
    pub estimated_annual_dividend: Option<f64>,
    pub estimated_monthly_dividend: Option<f64>,
    pub latest_account_asset_date: Option<String>,
    pub latest_holding_date: Option<String>,
    pub owner_totals: Vec<OwnerAssetTotal>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OwnerAssetTotal {
    pub owner_name: String,
    pub value_ntd: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DividendReceiptRow {
    pub owner_name: String,
    pub account_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub received_on: String,
    pub gross_amount: Option<f64>,
    pub tax_amount: Option<f64>,
    pub fee_amount: Option<f64>,
    pub net_amount: Option<f64>,
    pub currency_code: String,
    pub note: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendSummaryRow {
    pub owner_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub period_label: String,
    pub amount: Option<f64>,
    pub source_cell: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendMonthlyRow {
    pub owner_name: String,
    pub symbol: String,
    pub instrument_name: String,
    pub series_type: String,
    pub month_num: i64,
    pub amount: Option<f64>,
    pub source_cell: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyDividendData {
    pub summaries: Vec<LegacyDividendSummaryRow>,
    pub monthly: Vec<LegacyDividendMonthlyRow>,
}

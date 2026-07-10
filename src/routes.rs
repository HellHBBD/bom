use dioxus::prelude::*;

use crate::pages::{
    AccountsPage, DashboardPage, DividendIncomePage, DividendsLegacyPage, ExchangeRatePage,
    HoldingsPage, QuickPriceUpdatePage,
};

#[derive(Clone, Routable, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum Route {
    #[layout(crate::layout::AppLayout)]
    #[route("/")]
    DashboardPage {},
    #[route("/accounts")]
    AccountsPage {},
    #[route("/holdings")]
    HoldingsPage {},
    #[route("/market/prices")]
    QuickPriceUpdatePage {},
    #[route("/market/exchange-rates")]
    ExchangeRatePage {},
    #[route("/dividends")]
    DividendIncomePage {},
    #[route("/dividends/legacy")]
    DividendsLegacyPage {},
}

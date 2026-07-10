#![cfg_attr(
    all(target_os = "windows", feature = "bundle"),
    windows_subsystem = "windows"
)]

use dioxus::prelude::*;

mod account_asset;
mod db;
mod decimal;
mod dividend_receipt;
mod error;
mod exchange_rate;
mod format;
mod holding;
mod layout;
mod master_data;
mod models;
mod pages;
mod price;
mod routes;

use routes::Route;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let data_version = use_signal(|| 0_u64);
    use_context_provider(|| data_version);

    rsx! {
        document::Stylesheet { href: asset!("/assets/main.css") }
        Router::<Route> {}
    }
}

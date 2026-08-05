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
mod modal;
mod models;
mod pages;
mod price;
mod routes;
mod ui_preference;

use routes::Route;

#[cfg(all(not(target_arch = "wasm32"), feature = "desktop"))]
fn main() {
    use dioxus::desktop::{Config, LogicalSize, WindowBuilder};

    dioxus::LaunchBuilder::new()
        .with_cfg(
            Config::new().with_window(
                WindowBuilder::new()
                    .with_title("BOM")
                    .with_inner_size(LogicalSize::new(1440.0, 900.0))
                    .with_min_inner_size(LogicalSize::new(1024.0, 700.0)),
            ),
        )
        .launch(App);
}

#[cfg(any(target_arch = "wasm32", not(feature = "desktop")))]
fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let data_version = use_signal(|| 0_u64);
    use_context_provider(|| data_version);

    rsx! {
        document::Stylesheet { href: asset!("/assets/main.css") }
        modal::ModalFocusManager {}
        Router::<Route> {}
    }
}

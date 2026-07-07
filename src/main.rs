use dioxus::prelude::*;

mod db;
mod format;
mod layout;
mod models;
mod pages;
mod routes;

use routes::Route;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! {
        document::Stylesheet { href: asset!("/assets/main.css") }
        Router::<Route> {}
    }
}

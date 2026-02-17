# BOM Desktop

Desktop-only Dioxus application for CSV/XLSX import and local SQLite storage.

## Structure

```
src/
  main.rs          # Entry point + shared helpers
  app.rs           # Dioxus App root
  ui/
    state/app_state.rs
  domain/
  usecase/
  infra/
  platform/
  tests.rs         # Unit/integration tests
```

## Development

Run the desktop app:

```bash
dx serve --platform desktop
```

Run checks:

```bash
cargo clippy
cargo test
```

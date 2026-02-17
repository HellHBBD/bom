#[allow(dead_code)]
pub fn run_blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    f()
}

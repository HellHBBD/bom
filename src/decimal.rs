use std::str::FromStr;

use rust_decimal::Decimal;

use crate::error::{AppError, AppResult};

#[allow(dead_code)]
pub fn parse_decimal_field(field: &'static str, input: &str) -> AppResult<Decimal> {
    let normalized = input.trim().replace(',', "");
    if normalized.is_empty() {
        return Err(AppError::InvalidDecimal { field });
    }

    Decimal::from_str(&normalized).map_err(|_| AppError::InvalidDecimal { field })
}

#[allow(dead_code)]
pub fn normalize_decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use crate::error::AppError;

    use super::{normalize_decimal_text, parse_decimal_field};

    #[test]
    fn normalizes_decimal_text_for_storage() {
        let value = parse_decimal_field("price", "001000.5000").expect("valid decimal");

        assert_eq!(normalize_decimal_text(value), "1000.5");
    }

    #[test]
    fn accepts_grouped_decimal_input() {
        let value = parse_decimal_field("price", "1,234,567.8900").expect("valid decimal");

        assert_eq!(normalize_decimal_text(value), "1234567.89");
    }

    #[test]
    fn rejects_invalid_decimal_input() {
        let error = parse_decimal_field("price", "abc").expect_err("invalid decimal");

        assert!(matches!(error, AppError::InvalidDecimal { field: "price" }));
    }
}

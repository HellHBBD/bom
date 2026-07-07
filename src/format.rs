pub fn money(value: Option<f64>) -> String {
    match value {
        Some(value) => format_number(value, 0),
        None => "-".to_string(),
    }
}

pub fn decimal(value: Option<f64>, digits: usize) -> String {
    match value {
        Some(value) => format_number(value, digits),
        None => "-".to_string(),
    }
}

pub fn percent(value: Option<f64>) -> String {
    match value {
        Some(value) => format!("{}%", format_number(value * 100.0, 2)),
        None => "-".to_string(),
    }
}

fn format_number(value: f64, digits: usize) -> String {
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let formatted = format!("{:.*}", digits, value.abs());
    let (integer, fraction) = formatted
        .split_once('.')
        .map_or((formatted.as_str(), ""), |(integer, fraction)| {
            (integer, fraction)
        });

    let mut grouped = String::new();
    for (index, character) in integer.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(character);
    }

    let integer = grouped.chars().rev().collect::<String>();
    if digits == 0 {
        format!("{sign}{integer}")
    } else {
        format!("{sign}{integer}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_money_with_grouping() {
        assert_eq!(money(Some(1234567.4)), "1,234,567");
    }

    #[test]
    fn formats_negative_percent() {
        assert_eq!(percent(Some(-0.03219)), "-3.22%");
    }

    #[test]
    fn formats_missing_values_as_dash() {
        assert_eq!(money(None), "-");
        assert_eq!(percent(None), "-");
    }
}

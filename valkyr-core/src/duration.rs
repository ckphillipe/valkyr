//! Strict, unit-bearing durations used by adapter configuration.

use serde::{Deserialize, Deserializer, Serializer, de::Error};
use std::time::Duration;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse(&value).map_err(D::Error::custom)
}

pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format(value))
}

pub mod option {
    use super::{Duration, format, parse};
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| parse(&value).map_err(D::Error::custom))
            .transpose()
    }

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.as_ref().map(format).serialize(serializer)
    }
}

pub fn parse(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("duration must not be empty".into());
    }
    if input.starts_with('-') {
        return Err("negative durations are not supported".into());
    }

    let split = input
        .find(|character: char| character.is_ascii_alphabetic() || character == 'µ')
        .ok_or_else(|| "duration must include an explicit unit".to_owned())?;
    let (number, unit) = input.split_at(split);
    if number.is_empty() || number.matches('.').count() > 1 {
        return Err(format!("invalid duration number '{number}'"));
    }
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return Err(format!("invalid duration number '{number}'"));
    }
    let nanos_per_unit = match unit {
        "ns" => 1,
        "us" | "µs" => 1_000,
        "ms" => 1_000_000,
        "s" => NANOS_PER_SECOND,
        "m" => 60 * NANOS_PER_SECOND,
        "h" => 3_600 * NANOS_PER_SECOND,
        _ => return Err(format!("unsupported duration unit '{unit}'")),
    };
    let whole = whole
        .parse::<u128>()
        .map_err(|_| format!("duration number '{number}' overflows u128"))?;
    let whole_nanos = whole
        .checked_mul(nanos_per_unit)
        .ok_or_else(|| format!("duration '{input}' overflows"))?;
    let fraction_nanos = if fraction.is_empty() {
        0
    } else {
        let denominator = 10u128
            .checked_pow(fraction.len() as u32)
            .ok_or_else(|| format!("duration '{input}' has too many fractional digits"))?;
        let numerator = fraction
            .parse::<u128>()
            .map_err(|_| format!("duration number '{number}' overflows u128"))?
            .checked_mul(nanos_per_unit)
            .ok_or_else(|| format!("duration '{input}' overflows"))?;
        if numerator % denominator != 0 {
            return Err(format!(
                "duration '{input}' is not representable in nanoseconds"
            ));
        }
        numerator / denominator
    };
    let total_nanos = whole_nanos
        .checked_add(fraction_nanos)
        .ok_or_else(|| format!("duration '{input}' overflows"))?;
    let seconds = total_nanos / NANOS_PER_SECOND;
    let nanoseconds = (total_nanos % NANOS_PER_SECOND) as u32;
    let seconds = u64::try_from(seconds).map_err(|_| format!("duration '{input}' overflows"))?;
    Ok(Duration::new(seconds, nanoseconds))
}

fn format(value: &Duration) -> String {
    let total_nanos = value.as_nanos();
    if total_nanos == 0 {
        return "0s".into();
    }
    for (unit, nanos_per_unit) in [
        ("h", 3_600 * NANOS_PER_SECOND),
        ("m", 60 * NANOS_PER_SECOND),
        ("s", NANOS_PER_SECOND),
        ("ms", 1_000_000),
        ("us", 1_000),
        ("ns", 1),
    ] {
        if total_nanos % nanos_per_unit == 0 {
            return format!("{}{}", total_nanos / nanos_per_unit, unit);
        }
    }
    unreachable!("nanoseconds always divide evenly")
}

pub fn to_millis(value: Duration) -> Result<u64, String> {
    to_wire_unit(value, 1_000_000, "milliseconds")
}

pub fn to_seconds(value: Duration) -> Result<u64, String> {
    to_wire_unit(value, NANOS_PER_SECOND, "seconds")
}

fn to_wire_unit(value: Duration, nanos_per_unit: u128, unit: &str) -> Result<u64, String> {
    let nanos = value.as_nanos();
    if nanos % nanos_per_unit != 0 {
        return Err(format!("duration is not an exact whole {unit}"));
    }
    u64::try_from(nanos / nanos_per_unit).map_err(|_| format!("duration overflows {unit}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn parses_explicit_units_and_fractional_values() {
        assert_eq!(parse("15ms").unwrap(), Duration::from_millis(15));
        assert_eq!(parse("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse("1.5s").unwrap(), Duration::from_millis(1_500));
    }

    #[test]
    fn rejects_ambiguous_negative_and_unrepresentable_values() {
        for value in ["15", "-1s", "1.5ns", "1x"] {
            assert!(parse(value).is_err(), "{value} should be rejected");
        }
    }

    #[test]
    fn rejects_overflow_and_lossy_wire_conversions() {
        assert!(parse("18446744073709551616s").is_err());
        assert!(to_millis(Duration::from_secs(u64::MAX)).is_err());
        assert!(to_millis(Duration::from_nanos(1)).is_err());
        assert!(to_seconds(Duration::from_millis(1)).is_err());
        assert_eq!(to_millis(Duration::from_millis(1_500)).unwrap(), 1_500);
        assert_eq!(to_seconds(Duration::from_secs(90)).unwrap(), 90);
        assert_eq!(to_millis(Duration::from_millis(15)).unwrap(), 15);
        assert_eq!(to_seconds(Duration::from_secs(5)).unwrap(), 5);
    }

    #[test]
    fn serializes_with_an_explicit_unit() {
        let json = serde_json::to_string(&DurationValue(Duration::from_millis(15))).unwrap();
        assert_eq!(json, "\"15ms\"");
        let zero = serde_json::to_string(&DurationValue(Duration::ZERO)).unwrap();
        assert_eq!(zero, "\"0s\"");
    }

    #[derive(Deserialize, Serialize)]
    struct DurationValue(#[serde(with = "super")] Duration);
}

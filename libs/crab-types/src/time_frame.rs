use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeFrame {
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H2,
    H4,
    H6,
    H8,
    H12,
    D1,
    D3,
    W1,
    M1L,
}

impl TimeFrame {
    // 获取对应的时间毫秒数
    pub fn to_millis(&self) -> i64 {
        match self {
            TimeFrame::M1 => Duration::minutes(1).num_milliseconds(),
            TimeFrame::M3 => Duration::minutes(3).num_milliseconds(),
            TimeFrame::M5 => Duration::minutes(5).num_milliseconds(),
            TimeFrame::M15 => Duration::minutes(15).num_milliseconds(),
            TimeFrame::M30 => Duration::minutes(30).num_milliseconds(),
            TimeFrame::H1 => Duration::hours(1).num_milliseconds(),
            TimeFrame::H2 => Duration::hours(2).num_milliseconds(),
            TimeFrame::H4 => Duration::hours(4).num_milliseconds(),
            TimeFrame::H6 => Duration::hours(6).num_milliseconds(),
            TimeFrame::H8 => Duration::hours(8).num_milliseconds(),
            TimeFrame::H12 => Duration::hours(12).num_milliseconds(),
            TimeFrame::D1 => Duration::days(1).num_milliseconds(),
            TimeFrame::D3 => Duration::days(3).num_milliseconds(),
            TimeFrame::W1 => Duration::days(7).num_milliseconds(),
            TimeFrame::M1L => Duration::days(30).num_milliseconds(),
        }
    }

    // 获取字符串表示
    pub fn to_str(&self) -> &str {
        match self {
            TimeFrame::M1 => "1m",
            TimeFrame::M3 => "3m",
            TimeFrame::M5 => "5m",
            TimeFrame::M15 => "15m",
            TimeFrame::M30 => "30m",
            TimeFrame::H1 => "1h",
            TimeFrame::H2 => "2h",
            TimeFrame::H4 => "4h",
            TimeFrame::H6 => "6h",
            TimeFrame::H8 => "8h",
            TimeFrame::H12 => "12h",
            TimeFrame::D1 => "1d",
            TimeFrame::D3 => "3d",
            TimeFrame::W1 => "1w",
            TimeFrame::M1L => "1M",
        }
    }

    // Convenience function to parse a string and get the corresponding time frame and its millisecond value
    pub fn from_str_and_get_millis(s: &str) -> Option<(Self, u64)> {
        match s.parse::<TimeFrame>() {
            Ok(time_frame) => Some((time_frame.clone(), time_frame.to_millis() as u64)),
            Err(_) => None, // Return None for invalid TimeFrame strings
        }
    }
}

impl FromStr for TimeFrame {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1m" => Ok(TimeFrame::M1),
            "3m" => Ok(TimeFrame::M3),
            "5m" => Ok(TimeFrame::M5),
            "15m" => Ok(TimeFrame::M15),
            "30m" => Ok(TimeFrame::M30),
            "1h" => Ok(TimeFrame::H1),
            "2h" => Ok(TimeFrame::H2),
            "4h" => Ok(TimeFrame::H4),
            "6h" => Ok(TimeFrame::H6),
            "8h" => Ok(TimeFrame::H8),
            "12h" => Ok(TimeFrame::H12),
            "1d" => Ok(TimeFrame::D1),
            "3d" => Ok(TimeFrame::D3),
            "1w" => Ok(TimeFrame::W1),
            "1M" => Ok(TimeFrame::M1L),
            _ => Err(format!("Invalid TimeFrame string: {}", s)),
        }
    }
}

use barter_integration::Transformer;
use barter_integration::error::SocketError;
use serde::Deserialize;
use std::collections::HashSet;

/// Binance WSS 公告消息模型
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "UPPERCASE")]
pub enum BinanceAnnouncement {
    COMMAND {
        code: Option<u32>,
        msg: Option<String>,
    },
    DATA {
        #[serde(deserialize_with = "de_inner_json")]
        data: BinanceAnnouncementData,
    },
}

/// 公告的内部数据结构
#[derive(Debug, Deserialize, Clone)]
pub struct BinanceAnnouncementData {
    #[serde(rename = "publishDate")]
    pub publish_date: u64,
    pub title: String,
    #[serde(default)]
    pub catalog_id: u64,
    #[serde(default)]
    pub topic: String,
}

/// 统一的公告事件
#[derive(Debug, Clone)]
pub struct AnnouncementEvent {
    pub exchange: String,
    pub topic: String,
    pub title: String,
    pub publish_date: u64,
    pub catalog_id: u64,
}

/// Transformer：Binance 公告
pub struct BinanceAnnouncementTransformer {
    /// 最近处理过的 (publish_date, title)，避免重复
    seen: HashSet<(u64, String)>,
}

impl BinanceAnnouncementTransformer {
    pub fn new() -> Self {
        Self { seen: HashSet::new() }
    }
}

impl Default for BinanceAnnouncementTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformer for BinanceAnnouncementTransformer {
    type Error = SocketError;
    type Input = BinanceAnnouncement;
    type Output = AnnouncementEvent;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        match input {
            BinanceAnnouncement::DATA { data } => {
                let key = (data.publish_date, data.title.clone());
                if !self.seen.contains(&key) {
                    self.seen.insert(key.clone());
                    return vec![Ok(AnnouncementEvent {
                        exchange: "binance".to_string(),
                        topic: data.topic,
                        title: data.title,
                        publish_date: data.publish_date,
                        catalog_id: data.catalog_id,
                    })];
                }
                Vec::new()
            }

            BinanceAnnouncement::COMMAND { code, msg } => {
                // 可选：处理订阅确认或错误
                if let Some(c) = code {
                    if c != 0 {
                        return vec![Err(SocketError::Exchange(format!(
                            "Binance COMMAND error: code={:?}, msg={:?}",
                            code, msg
                        )))];
                    }
                }
                Vec::new()
            }
        }
    }
}

/// 将字符串形式的 JSON 解为目标类型
fn de_inner_json<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

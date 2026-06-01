use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventItem {
    pub source: String,
    pub external_id: String,
    pub event_type: String,
    pub title: String,
    pub publish_time: String,
    pub symbols: Vec<String>,
    pub importance: i32,
    pub url: String,
}

use crate::models::RekeningResponse;
use anyhow::Result;
use log::{info, error};
use serde_json;

pub struct ApiClient {
    client: reqwest::Client,
    gateway_url: String,
    token: String,
}

impl ApiClient {
    pub fn new(gateway_url: String, token: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            gateway_url,
            token,
        }
    }

    pub async fn fetch_rekening_data(&self, kd_satker: &str) -> Result<RekeningResponse> {
        let url = format!(
            "{}/api/v1/sprint/rekening/satker?kdsatker={}",
            self.gateway_url, kd_satker
        );

        info!("Fetching data for satker: {} from URL: {}", kd_satker, url);
        
        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;

        let status = response.status();
        info!("Response status for satker {}: {}", kd_satker, status);

        let text = response.text().await?;
        info!("Raw response length for satker {}: {} chars", kd_satker, text.len());
        info!("Raw response for satker {}: {}", kd_satker, text);
        
        match serde_json::from_str::<RekeningResponse>(&text) {
            Ok(parsed) => {
                info!("Successfully parsed response for satker {}. Data count: {}", 
                      kd_satker, parsed.data.len());
                Ok(parsed)
            }
            Err(e) => {
                error!("Failed to parse response for satker {}: {:?}", kd_satker, e);
                Err(e.into())
            }
        }
    }
} 
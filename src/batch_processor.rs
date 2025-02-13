use crate::api_client::ApiClient;
use crate::db::DatabaseHandler;
use anyhow::Result;
use futures::StreamExt;
use log::{error, info};
use tokio::time::{sleep, Duration};

pub struct BatchProcessor {
    api_client: ApiClient,
    db_handler: DatabaseHandler,
}

impl BatchProcessor {
    pub fn new(api_client: ApiClient, db_handler: DatabaseHandler) -> Self {
        Self {
            api_client,
            db_handler,
        }
    }

    pub async fn process_all_satkers(&self) -> Result<()> {
        let mut processed = 0;
        let chunk_size = 100;
        
        loop {
            let satkers = self.db_handler.get_active_satkers(chunk_size)?;
            if satkers.is_empty() {
                break;
            }

            info!("Processing batch of {} satkers", satkers.len());
            
            let futures = satkers.iter().cloned().map(|kd_satker| {
                async move {
                    self.process_single_satker(&kd_satker).await
                }
            });

            let results = futures::stream::iter(futures)
                .buffer_unordered(10)
                .collect::<Vec<_>>()
                .await;

            processed += results.len();
            info!("Processed {} satkers so far", processed);

            sleep(Duration::from_millis(500)).await;
        }

        info!("Completed processing all satkers. Total processed: {}", processed);
        Ok(())
    }

    async fn process_single_satker(&self, kd_satker: &str) -> Result<()> {
        info!("Starting to process satker: {}", kd_satker);
        
        // Get current count
        let initial_count: i64 = self.db_handler.get_rekening_count()?;
        info!("Initial rekening count: {}", initial_count);

        match self.api_client.fetch_rekening_data(kd_satker).await {
            Ok(response) => {
                info!("Response received for satker {}: success={}, message={}, data_count={}", 
                      kd_satker, response.success, response.message, response.data.len());
                
                if !response.success {
                    info!("No success response for satker: {}", kd_satker);
                    return Ok(());
                }

                let mut success_count = 0;
                let mut error_count = 0;
                let mut skip_count = 0;

                for (index, data) in response.data.iter().enumerate() {
                    info!("Processing record {}/{} for satker {}", 
                          index + 1, response.data.len(), kd_satker);
                    
                    if let Some(rekening) = data.to_rekening() {
                        info!("Attempting to insert rekening: {:?}", rekening);
                        match self.db_handler.insert_rekening(&rekening) {
                            Ok(_) => {
                                // Verify the insert
                                if self.db_handler.verify_rekening(&rekening.no_rekening)? {
                                    success_count += 1;
                                    info!("Successfully verified rekening {} for satker {}", 
                                          rekening.no_rekening, kd_satker);
                                } else {
                                    error_count += 1;
                                    error!("Failed to verify rekening {} for satker {}", 
                                          rekening.no_rekening, kd_satker);
                                }
                            }
                            Err(e) => {
                                error_count += 1;
                                error!("Failed to insert rekening for satker {}: {:?}", kd_satker, e);
                            }
                        }
                    } else {
                        skip_count += 1;
                        info!("Skipping record {}/{} with missing required fields for satker: {}", 
                              index + 1, response.data.len(), kd_satker);
                    }
                }

                let final_count: i64 = self.db_handler.get_rekening_count()?;
                info!("Final rekening count: {}. Delta: {}", 
                      final_count, final_count - initial_count);

                info!("Completed processing satker {}. Success: {}, Errors: {}, Skipped: {}", 
                      kd_satker, success_count, error_count, skip_count);

                self.db_handler.update_last_fetch_date(kd_satker)?;
                Ok(())
            }
            Err(e) => {
                error!("Error processing satker {}: {:?}", kd_satker, e);
                Err(e)
            }
        }
    }
} 
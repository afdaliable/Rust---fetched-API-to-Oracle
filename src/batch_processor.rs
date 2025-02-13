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
        let mut processed_satkers = 0;
        let mut total_records = 0;
        let chunk_size = 50;
        
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
                .buffer_unordered(5)
                .collect::<Vec<_>>()
                .await;

            let mut batch_success = 0;
            let mut batch_records = 0;

            for result in results {
                if let Ok((success, records)) = result {
                    if success {
                        batch_success += 1;
                        batch_records += records;
                    }
                }
            }

            processed_satkers += batch_success;
            total_records += batch_records;
            
            info!("Batch completed - Successful satkers: {}, Total records inserted: {}", 
                  batch_success, batch_records);
            info!("Progress - Processed satkers: {}, Total records: {}", 
                  processed_satkers, total_records);

            sleep(Duration::from_secs(1)).await;
        }

        info!("Completed processing. Total successful satkers: {}, Total records: {}", 
              processed_satkers, total_records);
        Ok(())
    }

    async fn process_single_satker(&self, kd_satker: &str) -> Result<(bool, i32)> {
        info!("Starting to process satker: {}", kd_satker);
        
        match self.api_client.fetch_rekening_data(kd_satker).await {
            Ok(response) => {
                if !response.success || response.data.is_empty() {
                    info!("No data for satker: {}", kd_satker);
                    return Ok((false, 0));
                }

                let conn = match self.db_handler.begin_transaction() {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to begin transaction for satker {}: {:?}", kd_satker, e);
                        return Ok((false, 0));
                    }
                };

                let mut success_count = 0;
                let mut error_count = 0;

                info!("Starting transaction for satker {} with {} records", 
                      kd_satker, response.data.len());

                for (idx, data) in response.data.iter().enumerate() {
                    if let Some(rekening) = data.to_rekening() {
                        info!("Processing record {}/{} for satker {}: {}", 
                              idx + 1, response.data.len(), kd_satker, rekening.no_rekening);
                        
                        match self.db_handler.insert_rekening_batch(&conn, &rekening) {
                            Ok(_) => {
                                info!("Successfully inserted record {}/{}: {}", 
                                      idx + 1, response.data.len(), rekening.no_rekening);
                                success_count += 1;
                            },
                            Err(e) => {
                                error!("Failed to insert record {}/{} - {}: {:?}", 
                                       idx + 1, response.data.len(), rekening.no_rekening, e);
                                error_count += 1;
                                let _ = DatabaseHandler::rollback_transaction(&conn);
                                return Ok((false, 0));
                            }
                        }
                    }
                }

                if error_count == 0 && success_count > 0 {
                    match DatabaseHandler::commit_transaction(&conn) {
                        Ok(_) => {
                            info!("Successfully committed {} records for satker {}", 
                                  success_count, kd_satker);
                            self.db_handler.update_last_fetch_date(kd_satker)?;
                            Ok((true, success_count))
                        },
                        Err(e) => {
                            error!("Failed to commit transaction for satker {}: {:?}", 
                                   kd_satker, e);
                            let _ = DatabaseHandler::rollback_transaction(&conn);
                            Ok((false, 0))
                        }
                    }
                } else {
                    error!("No successful inserts for satker {}", kd_satker);
                    let _ = DatabaseHandler::rollback_transaction(&conn);
                    Ok((false, 0))
                }
            },
            Err(e) => {
                error!("Failed to fetch data for satker {}: {:?}", kd_satker, e);
                Ok((false, 0))
            }
        }
    }
} 
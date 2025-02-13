mod api_client;
mod db;
mod models;
mod batch_processor;

use anyhow::Result;
use dotenv::dotenv;
use log::{error, info};
use std::env;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::api_client::ApiClient;
use crate::db::DatabaseHandler;
use crate::batch_processor::BatchProcessor;

async fn process_data() -> Result<()> {
    let gateway_url = env::var("GATEWAY_URL")?;
    let token = env::var("API_TOKEN")?;
    let connection_string = env::var("ORACLE_CONNECTION_STRING")?;

    let api_client = ApiClient::new(gateway_url, token);
    let db_handler = DatabaseHandler::new(&connection_string)?;
    let batch_processor = BatchProcessor::new(api_client, db_handler);

    info!("Starting batch processing for all satkers");
    batch_processor.process_all_satkers().await?;
    info!("Completed batch processing");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    let scheduler_enabled = env::var("SCHEDULER_ENABLED")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    if scheduler_enabled {
        let schedule = env::var("SCHEDULE").unwrap_or_else(|_| "0 0 * * *".to_string());
        let mut scheduler = JobScheduler::new().await?;
        
        scheduler
            .add(Job::new_async(schedule.as_str(), |_uuid, _l| {
                Box::pin(async {
                    if let Err(e) = process_data().await {
                        error!("Error processing data: {:?}", e);
                    }
                })
            })?)
            .await?;

        scheduler.start().await?;
        tokio::signal::ctrl_c().await?;
        scheduler.shutdown().await?;
    } else {
        if let Err(e) = process_data().await {
            error!("Error processing data: {:?}", e);
        }
    }

    Ok(())
}

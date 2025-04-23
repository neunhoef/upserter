use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, info};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
// use tokio::sync::Mutex;
use tokio::time::sleep;

/// ArangoDB parallel UPSERT tester
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// ArangoDB endpoint URLs
    #[clap(long, env = "ARANGODB_URL", value_delimiter = ',')]
    endpoints: Vec<String>,

    /// ArangoDB username
    #[clap(long, env = "ARANGODB_USER")]
    username: String,

    /// ArangoDB password
    #[clap(long, env = "ARANGODB_PASSWORD")]
    password: String,

    /// Prefix for database names
    #[clap(long, default_value = "test_db")]
    prefix: String,

    /// Number of parallel worker threads for UPSERT operations
    #[clap(long, default_value = "10")]
    worker_threads: usize,
}

struct ArangoClient {
    endpoints: Vec<String>,
    username: String,
    password: String,
    prefix: String,
    client: reqwest::Client,
}

impl ArangoClient {
    fn new(endpoints: Vec<String>, username: String, password: String, prefix: String) -> Self {
        let client = reqwest::Client::new();
        Self {
            endpoints,
            username,
            password,
            prefix,
            client,
        }
    }

    fn get_random_endpoint(&self) -> &str {
        let idx = rand::thread_rng().gen_range(0..self.endpoints.len());
        &self.endpoints[idx]
    }

    async fn upsert_operation(&self, worker_id: usize) -> Result<()> {
        // This is a placeholder for the actual UPSERT operation
        let endpoint = self.get_random_endpoint();
        info!(
            "Worker {}: Performing UPSERT operation on endpoint {}",
            worker_id, endpoint
        );
        // Simulate work
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    async fn create_database(&self) -> Result<String> {
        // This is a placeholder for the actual database creation
        let endpoint = self.get_random_endpoint();
        let db_name = format!("{}_{}", self.prefix, rand::thread_rng().gen::<u32>());
        info!(
            "DB Manager: Creating database '{}' on endpoint {}",
            db_name, endpoint
        );
        // Simulate work
        sleep(Duration::from_secs(1)).await;
        Ok(db_name)
    }

    async fn drop_database(&self, db_name: &str) -> Result<()> {
        // This is a placeholder for the actual database dropping
        let endpoint = self.get_random_endpoint();
        info!(
            "DB Manager: Dropping database '{}' on endpoint {}",
            db_name, endpoint
        );
        // Simulate work
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

async fn run_worker(client: Arc<ArangoClient>, worker_id: usize) -> Result<()> {
    loop {
        client.upsert_operation(worker_id).await?;
        debug!("Worker {}: Operation completed", worker_id);
    }
}

async fn run_db_manager(client: Arc<ArangoClient>) -> Result<()> {
    let max_dbs = 10;
    let mut active_dbs = Vec::new();

    loop {
        // Randomly create or drop databases
        let rand_val = rand::thread_rng().gen_range(0..=100);

        if rand_val < 60 && active_dbs.len() < max_dbs {
            // Create a new database
            let db_name = client.create_database().await?;
            active_dbs.push(db_name);
        } else if !active_dbs.is_empty() {
            // Drop a random database
            let idx = rand::thread_rng().gen_range(0..active_dbs.len());
            let db_name = active_dbs.remove(idx);
            client.drop_database(&db_name).await?;
        }

        // Sleep for a bit before next operation
        sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!("Starting ArangoDB UPSERT tester");
    info!("ArangoDB Endpoints: {}", args.endpoints.join(", "));
    info!("Worker Threads: {}", args.worker_threads);
    info!("Database Prefix: {}", args.prefix);

    let client = Arc::new(ArangoClient::new(
        args.endpoints,
        args.username,
        args.password,
        args.prefix,
    ));

    // Spawn worker tasks
    let mut worker_handles = Vec::new();
    for i in 0..args.worker_threads {
        let client_clone = Arc::clone(&client);
        let handle = tokio::spawn(async move {
            if let Err(e) = run_worker(client_clone, i).await {
                eprintln!("Worker {} error: {}", i, e);
            }
        });
        worker_handles.push(handle);
    }

    // Spawn database manager task
    let db_manager_client = Arc::clone(&client);
    let db_manager_handle = tokio::spawn(async move {
        if let Err(e) = run_db_manager(db_manager_client).await {
            eprintln!("Database manager error: {}", e);
        }
    });

    // Wait for tasks to complete (they're infinite loops, so they won't unless there's an error)
    for (i, handle) in worker_handles.into_iter().enumerate() {
        handle.await.context(format!("Worker {} task failed", i))?;
    }
    db_manager_handle
        .await
        .context("Database manager task failed")?;

    Ok(())
}

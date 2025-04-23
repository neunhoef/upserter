use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, info};
use rand::Rng;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
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
    
    /// Sleep time for database manager task in milliseconds
    #[clap(long, default_value = "100")]
    db_sleep_ms: u64,
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

    async fn upsert_operation(
        &self,
        worker_id: usize,
        active_dbs: &Arc<RwLock<Vec<String>>>,
    ) -> Result<()> {
        // Get a random database from the list
        let db_name = {
            let db_list = active_dbs.read().unwrap();
            if db_list.is_empty() {
                // No databases available yet
                debug!(
                    "Worker {}: No databases available, skipping operation",
                    worker_id
                );
                return Ok(());
            }
            let idx = rand::thread_rng().gen_range(0..db_list.len());
            db_list[idx].clone()
        };

        let endpoint = self.get_random_endpoint();
        
        // Choose a random collection (c1-c5)
        let coll_num = rand::thread_rng().gen_range(1..=5);
        let collection_name = format!("c{}", coll_num);
        
        info!(
            "Worker {}: Performing UPSERT operation on database '{}', collection '{}' at endpoint {}",
            worker_id, db_name, collection_name, endpoint
        );
        
        // Build URL for cursor API
        let url = format!("{}/_db/{}/_api/cursor", endpoint, db_name);
        
        // Build the AQL query with bindVars
        let query_body = serde_json::json!({
            "query": "FOR i IN 1..10 LET key = TO_STRING(FLOOR(RAND()*1000)) UPSERT {_key:key} INSERT {_key:key, value: FLOOR(RAND()*1000000), Hallo: i, date: DATE_NOW()} UPDATE {_value: FLOOR(RAND() * 1000000), Hallo: i, date: DATE_NOW()} IN @@col",
            "bindVars": {
                "@col": collection_name
            }
        });
        
        // Make the POST request with timeout
        match self.client
            .post(&url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&query_body)
            .timeout(Duration::from_secs(60))
            .send()
            .await
        {
            Ok(response) => {
                if response.status() != 201 {
                    // Request completed but with wrong status code
                    info!(
                        "Worker {}: UPSERT operation failed on database '{}', collection '{}': HTTP status {}",
                        worker_id, db_name, collection_name, response.status()
                    );
                } else {
                    debug!(
                        "Worker {}: UPSERT operation succeeded on database '{}', collection '{}'",
                        worker_id, db_name, collection_name
                    );
                }
            },
            Err(e) => {
                // Request failed (e.g., timeout, connection error)
                info!(
                    "Worker {}: UPSERT operation failed on database '{}', collection '{}': {}",
                    worker_id, db_name, collection_name, e
                );
            }
        }
        
        Ok(())
    }

    async fn create_database(&self) -> Result<String> {
        let endpoint = self.get_random_endpoint();
        let db_name = format!("{}_{}", self.prefix, rand::thread_rng().gen::<u32>());
        info!(
            "DB Manager: Creating database '{}' on endpoint {}",
            db_name, endpoint
        );
        
        // Create database with single shard
        let url = format!("{}{}", endpoint, "/_api/database");
        let json_body = serde_json::json!({
            "name": db_name,
            "sharding": "single"
        });
        
        // Make the POST request with 60 second timeout
        let result = self.client
            .post(&url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&json_body)
            .timeout(Duration::from_secs(60))
            .send()
            .await;
            
        match result {
            Ok(response) => {
                if response.status() != 201 {
                    // Request completed but with wrong status code
                    info!(
                        "DB Manager: Failed to create database '{}': HTTP status {}",
                        db_name, response.status()
                    );
                    return Err(anyhow::anyhow!("Failed to create database: HTTP status {}", response.status()));
                }
                
                // Create collections c1 through c5
                for i in 1..=5 {
                    let collection_name = format!("c{}", i);
                    let coll_url = format!("{}{}{}{}", endpoint, "/_db/", db_name, "/_api/collection");
                    let coll_body = serde_json::json!({
                        "name": collection_name
                    });
                    
                    match self.client
                        .post(&coll_url)
                        .basic_auth(&self.username, Some(&self.password))
                        .json(&coll_body)
                        .timeout(Duration::from_secs(60))
                        .send()
                        .await 
                    {
                        Ok(coll_response) => {
                            if coll_response.status() != 200 && coll_response.status() != 201 {
                                info!(
                                    "DB Manager: Failed to create collection '{}' in database '{}': HTTP status {}",
                                    collection_name, db_name, coll_response.status()
                                );
                            } else {
                                debug!(
                                    "DB Manager: Created collection '{}' in database '{}'",
                                    collection_name, db_name
                                );
                            }
                        },
                        Err(e) => {
                            info!(
                                "DB Manager: Failed to create collection '{}' in database '{}': {}",
                                collection_name, db_name, e
                            );
                        }
                    }
                }
                
                // Database created successfully
                Ok(db_name)
            },
            Err(e) => {
                // Request failed (e.g., timeout, connection error)
                info!(
                    "DB Manager: Failed to create database '{}': {}",
                    db_name, e
                );
                Err(anyhow::anyhow!("Failed to create database: {}", e))
            }
        }
    }

    async fn drop_database(&self, db_name: &str) -> Result<()> {
        let endpoint = self.get_random_endpoint();
        info!(
            "DB Manager: Dropping database '{}' on endpoint {}",
            db_name, endpoint
        );
        
        // Build URL for database deletion
        let url = format!("{}/_api/database/{}", endpoint, db_name);
        
        // Make the DELETE request with 60 second timeout
        match self.client
            .delete(&url)
            .basic_auth(&self.username, Some(&self.password))
            .timeout(Duration::from_secs(60))
            .send()
            .await
        {
            Ok(response) => {
                if response.status() != 200 {
                    // Request completed but with wrong status code
                    info!(
                        "DB Manager: Failed to drop database '{}': HTTP status {}",
                        db_name, response.status()
                    );
                    // Assume database is still there and continue
                    return Ok(());
                }
                
                info!("DB Manager: Successfully dropped database '{}'", db_name);
                Ok(())
            },
            Err(e) => {
                // Request failed (e.g., timeout, connection error)
                info!(
                    "DB Manager: Failed to drop database '{}': {}",
                    db_name, e
                );
                // Assume database is still there and continue
                Ok(())
            }
        }
    }
}

async fn run_worker(
    client: Arc<ArangoClient>,
    worker_id: usize,
    active_dbs: Arc<RwLock<Vec<String>>>,
) -> Result<()> {
    loop {
        client.upsert_operation(worker_id, &active_dbs).await?;
        debug!("Worker {}: Operation completed", worker_id);
        // Short sleep to avoid constant lock contention
        sleep(Duration::from_millis(100)).await;
    }
}

async fn run_db_manager(
    client: Arc<ArangoClient>,
    active_dbs: Arc<RwLock<Vec<String>>>,
    db_sleep_ms: u64,
) -> Result<()> {
    let min_dbs = 3;
    let max_dbs = 10;

    // First, ensure we have at least min_dbs databases
    let mut created_count = 0;
    let mut attempts = 0;
    let max_attempts = min_dbs * 2; // Allow some failures
    
    while created_count < min_dbs && attempts < max_attempts {
        match client.create_database().await {
            Ok(db_name) => {
                let mut dbs = active_dbs.write().unwrap();
                dbs.push(db_name);
                created_count += 1;
                info!("DB Manager: Created initial database, total: {}", dbs.len());
            },
            Err(e) => {
                info!("DB Manager: Failed to create initial database: {}", e);
            }
        }
        attempts += 1;
    }
    
    if created_count < min_dbs {
        return Err(anyhow::anyhow!("Failed to create the minimum required databases"));
    }

    loop {
        // Randomly create or drop databases
        let rand_val = rand::thread_rng().gen_range(0..=100);
        let should_create;
        let db_to_drop = {
            let dbs = active_dbs.read().unwrap();
            should_create = rand_val < 60 && dbs.len() < max_dbs;

            // Only drop if we have more than min_dbs
            if !should_create && dbs.len() > min_dbs {
                // Choose a database to drop
                let idx = rand::thread_rng().gen_range(0..dbs.len());
                Some(dbs[idx].clone())
            } else {
                None
            }
        };

        if should_create {
            // Create a new database
            match client.create_database().await {
                Ok(db_name) => {
                    // Add it to our shared list
                    {
                        let mut dbs = active_dbs.write().unwrap();
                        dbs.push(db_name);
                        info!("DB Manager: Active databases: {}", dbs.len());
                    }
                },
                Err(e) => {
                    // Just log the error but continue operation
                    info!("DB Manager: Database creation failed: {}", e);
                }
            }
        } else if let Some(db_name) = db_to_drop {
            // Drop the selected database
            client.drop_database(&db_name).await?;
            // Remove it from our shared list
            {
                let mut dbs = active_dbs.write().unwrap();
                dbs.retain(|name| name != &db_name);
                info!("DB Manager: Active databases: {}", dbs.len());
            }
        }

        // Sleep for a configurable amount of time before next operation
        sleep(Duration::from_millis(db_sleep_ms)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    let args = Args::parse();

    info!("Starting ArangoDB UPSERT tester");
    info!("ArangoDB Endpoints: {}", args.endpoints.join(", "));
    info!("Worker Threads: {}", args.worker_threads);
    info!("Database Prefix: {}", args.prefix);
    info!("DB Manager Sleep: {}ms", args.db_sleep_ms);

    let client = Arc::new(ArangoClient::new(
        args.endpoints,
        args.username,
        args.password,
        args.prefix,
    ));

    // Create shared database list
    let active_dbs = Arc::new(RwLock::new(Vec::<String>::new()));

    // Spawn worker tasks
    let mut worker_handles = Vec::new();
    for i in 0..args.worker_threads {
        let client_clone = Arc::clone(&client);
        let dbs_clone = Arc::clone(&active_dbs);
        let handle = tokio::spawn(async move {
            if let Err(e) = run_worker(client_clone, i, dbs_clone).await {
                eprintln!("Worker {} error: {}", i, e);
            }
        });
        worker_handles.push(handle);
    }

    // Spawn database manager task
    let db_manager_client = Arc::clone(&client);
    let db_manager_dbs = Arc::clone(&active_dbs);
    let db_sleep_ms = args.db_sleep_ms;
    let db_manager_handle = tokio::spawn(async move {
        if let Err(e) = run_db_manager(db_manager_client, db_manager_dbs, db_sleep_ms).await {
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

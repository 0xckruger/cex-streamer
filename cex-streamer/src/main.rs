mod library;
mod logger;
mod redis;

use crate::library::{BinanceClient, BinanceEvent, ExchangeClient, SubscriptionConfig};
use crate::logger::init_logger;
use crate::redis::test_and_get_redis_client;
use clap::Parser;
use futures_util::future::join_all;
use std::path::PathBuf;
use std::{env, fs};
use tokio::signal;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const REDIS_WRITE_BATCH_SIZE: usize = 100;
const REDIS_WRITE_BUFFER_MS: u64 = 200;

#[derive(Parser)]
#[command(
    author,
    version,
    about,
    long_about = "A simple Binance WebSocket feed consumer"
)]
struct Args {
    #[arg(short = 'c', long, required = true)]
    subscription_config_path: PathBuf,
    #[clap(short = 'v', long)]
    verbose: bool,
}

fn load_config(path: PathBuf) -> anyhow::Result<Vec<SubscriptionConfig>> {
    let config_file = fs::read_to_string(path)?;
    let configs: Vec<SubscriptionConfig> = serde_json::from_str(&config_file)?;
    Ok(configs)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let binary_name = env!("CARGO_PKG_NAME");
    let logging_directory = match env::var_os("LOG_DIR") {
        Some(os_string) => Some(PathBuf::from(os_string)),
        None => None,
    };
    init_logger(logging_directory, binary_name, args.verbose);

    let redis_client = test_and_get_redis_client().await?;

    let config = load_config(args.subscription_config_path)?;

    let mut handles = Vec::new();
    let mut senders = Vec::new();

    let cancellation_token = CancellationToken::new();

    for exchange_config in config {
        if !exchange_config.enabled {
            continue;
        }

        // TODO: Refactor out common logic (ws start, bbo publisher start) per exchange
        match exchange_config.exchange.as_str() {
            "binance" => {
                let client = BinanceClient::new(exchange_config);
                // TODO: Convert channel to Arc<BinanceEvent> to save resources
                let (tx, _rx) = broadcast::channel::<BinanceEvent>(16_384);
                let ws_cancellation_token = cancellation_token.child_token();
                let ws_tx = tx.clone();
                let ws_handle = tokio::spawn(async move {
                    client.run(ws_tx, ws_cancellation_token).await;
                });

                // DB writer for persisting universal BboMessages
                let bbo_cancellation_token = cancellation_token.child_token();
                let bbo_rx = tx.subscribe();
                handles.push(BinanceClient::start_bbo_publisher(
                    redis_client.clone(),
                    bbo_rx,
                    bbo_cancellation_token,
                    REDIS_WRITE_BATCH_SIZE,
                    REDIS_WRITE_BUFFER_MS,
                ));
                info!("Started publishing Binance BBOs");

                // Example trading consumer for a monorepo style setup
                // Consumer could generate valuations, ETL data further, collect metrics, etc.
                let val_cancellation_token = cancellation_token.child_token();
                let mut valuation_rx = tx.subscribe();
                let valuation_handle = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = val_cancellation_token.cancelled() => {
                                info!("Exiting valuation task");
                                break;
                            }
                            next_val = valuation_rx.recv() => {
                                match next_val {
                                    Ok(msg) => match msg {
                                        BinanceEvent::BookTicker(bbo) => {
                                        debug!("[Trading] Got book ticker event: {:?}", bbo);
                                    },
                                    BinanceEvent::Unknown(v) => {
                                        debug!("[Trading] Unknown Binance event: {:?}", v);
                                    }
                                },
                                Err(_) => break,
                                }
                            }
                        }
                    }
                });

                handles.push(ws_handle);
                handles.push(valuation_handle);
                senders.push(tx);
            }
            unsupported_exchange => {
                error!("Provided unsupported exchange: {}", unsupported_exchange);
            }
        }
    }

    signal::ctrl_c().await?;
    info!("Interrupt received, exiting program");
    cancellation_token.cancel();
    let _ = join_all(handles).await;

    Ok(())
}

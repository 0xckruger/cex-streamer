use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Local, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client, Value};
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};

use clap::Parser;
use crossterm::{
    cursor::MoveTo,
    execute,
    terminal::{Clear, ClearType},
};
use std::io::{self};

const REFRESH_RATE: u64 = 1;

#[derive(Clone, Debug)]
struct BboMessage {
    exchange: String,
    symbol: String,
    bid: f32,
    ask: f32,
    bid_qty: f32,
    ask_qty: f32,
    timestamp: u64,
}

impl Default for BboMessage {
    fn default() -> Self {
        Self {
            exchange: "unknown".to_string(),
            symbol: "unknown".to_string(),
            bid: 0.0,
            ask: 0.0,
            bid_qty: 0.0,
            ask_qty: 0.0,
            timestamp: 0,
        }
    }
}

#[derive(Clone)]
struct App {
    symbol_to_bbo: Arc<RwLock<HashMap<String, BboMessage>>>,
    // track most recent redis id for stream
    stream_to_last_id: Arc<RwLock<HashMap<String, String>>>,
}

impl App {
    fn new(symbols: Vec<String>) -> Self {
        let mut symbol_to_bbo = HashMap::new();

        let mut stream_to_last_id = HashMap::new();

        for sym in symbols {
            symbol_to_bbo.insert(sym.clone(), BboMessage::default());
            stream_to_last_id.insert(format!("bbo:{}", sym), "$".to_string()); // most recent
        }

        Self {
            symbol_to_bbo: Arc::new(RwLock::new(symbol_to_bbo)),
            stream_to_last_id: Arc::new(RwLock::new(stream_to_last_id)),
        }
    }

    /// Pull in streams for BBO Data from Redis
    async fn read_streams(&self, mut conn: ConnectionManager) {
        loop {
            // Copy bbo streams to most recent message IDs
            let (keys, ids): (Vec<String>, Vec<String>) = {
                let stream_to_last_id = self.stream_to_last_id.read().await;
                (
                    stream_to_last_id.keys().cloned().collect(),
                    stream_to_last_id.values().cloned().collect(),
                )
            };

            // Convert to Redis-friendly type
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();

            let opts = StreamReadOptions::default().block(100).count(10);

            // XREAD from stream(s)
            match conn
                .xread_options::<&str, &str, StreamReadReply>(&key_refs, &id_refs, &opts)
                .await
            {
                Ok(stream_read_reply) => {
                    for stream in stream_read_reply.keys {
                        let stream_name = stream.key;
                        let symbol = stream_name.trim_start_matches("bbo:").to_string();

                        for msg in stream.ids {
                            self.stream_to_last_id
                                .write()
                                .await
                                .insert(stream_name.clone(), msg.id.clone());

                            let bbo = BboMessage {
                                exchange: get_string_from_map(&msg.map, "exchange"),
                                symbol: symbol.clone(),
                                bid: get_f32_from_map(&msg.map, "best_bid_price"),
                                ask: get_f32_from_map(&msg.map, "best_ask_price"),
                                bid_qty: get_f32_from_map(&msg.map, "best_bid_qty"),
                                ask_qty: get_f32_from_map(&msg.map, "best_ask_qty"),
                                timestamp: get_u64_from_map(&msg.map, "timestamp"),
                            };

                            self.symbol_to_bbo.write().await.insert(symbol.clone(), bbo);
                        }
                    }
                }
                Err(_) => {
                    // Retry
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn print_loop(&self) {
        loop {
            {
                let bbo_display_data = self.symbol_to_bbo.read().await;

                execute!(io::stdout(), Clear(ClearType::All), MoveTo(0, 0)).unwrap();

                println!("---- BBO Streaming from Redis ----");
                println!(
                    "{:<10} {:<10} {:>12} {:>12} {:>12} {:>12} {:>15}",
                    "Exchange", "Symbol", "Bid", "BidQty", "Ask", "AskQty", "Timestamp"
                );

                for (_sym, bbo) in bbo_display_data.iter() {
                    let ts_str = if bbo.timestamp > 0 {
                        DateTime::<Utc>::from_timestamp_millis(bbo.timestamp as i64)
                            .map(|dt_utc| {
                                let dt_local = dt_utc.with_timezone(&Local);
                                dt_local.format("%H:%M:%S").to_string()
                            })
                            .unwrap_or_else(|| "-".into())
                    } else {
                        "n/a".into()
                    };

                    println!(
                        "{:<10} {:<10} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>15}",
                        bbo.exchange,
                        bbo.symbol,
                        bbo.bid,
                        bbo.bid_qty,
                        bbo.ask,
                        bbo.ask_qty,
                        ts_str
                    );
                }
            }
            sleep(Duration::from_secs(REFRESH_RATE)).await;
        }
    }
}

fn get_f32_from_map(map: &HashMap<String, Value>, field: &str) -> f32 {
    map.get(field)
        .and_then(|val| redis::from_redis_value::<String>(val).ok())
        .and_then(|s| s.parse::<f32>().ok())
        .unwrap_or_default()
}

fn get_u64_from_map(map: &HashMap<String, Value>, field: &str) -> u64 {
    map.get(field)
        .and_then(|val| redis::from_redis_value::<String>(val).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_default()
}

fn get_string_from_map(map: &HashMap<String, Value>, field: &str) -> String {
    map.get(field)
        .and_then(|val| redis::from_redis_value::<String>(val).ok())
        .unwrap_or_default()
}

pub fn test_and_get_redis_client() -> Result<Client> {
    let redis_host = match env::var("REDIS_HOSTNAME") {
        Ok(hostname) => hostname,
        Err(e) => {
            return Err(anyhow!(
                "REDIS_HOSTNAME env var not set, required for Redis DB! {}",
                e
            ));
        }
    };

    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();

    let uri_scheme = match env::var("IS_TLS") {
        Ok(_) => "rediss",
        Err(_) => "redis",
    };

    let redis_url = format!("{}://:{}@{}", uri_scheme, redis_password, redis_host);
    match Client::open(redis_url) {
        Ok(client) => Ok(client),
        Err(e) => Err(anyhow!("Error creating Redis client: {}", e)),
    }
}

#[derive(Parser, Debug)]
#[command(name = "bbomonitor", about = "Simple Redis BBO monitor")]
struct Args {
    /// Subscribe to these symbol streams
    #[arg(short, long, value_name = "SYMBOL", num_args = 1.., default_values_t = default_symbols())]
    symbols: Vec<String>,
}

fn default_symbols() -> Vec<String> {
    vec!["BTCUSDT".into(), "SOLUSDT".into(), "ETHUSDT".into()]
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let redis_client = test_and_get_redis_client()?;

    println!("Monitoring symbols: {:?}", args.symbols);

    let app = App::new(args.symbols);
    let conn = ConnectionManager::new(redis_client).await?;

    let app_clone = app.clone();
    tokio::spawn(async move {
        app_clone.read_streams(conn).await;
    });

    app.print_loop().await;

    Ok(())
}

use crate::library::BboMessage;
use anyhow::anyhow;
use redis::Client;
use redis::aio::ConnectionManager;
use std::env;
use tokio::time::{Duration, timeout};
use tracing::info;

const MAX_STREAM_LEN: u64 = 1_000_000;
const REDIS_CONNECT_TIMEOUT_SECS: u64 = 5;
const REDIS_PING_TIMEOUT_SECS: u64 = 5;

/// Create Redis Client and test it. If this fails, we shouldn't continue.
pub async fn test_and_get_redis_client() -> anyhow::Result<Client> {
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
    let client = Client::open(redis_url.clone())
        .map_err(|e| anyhow!("Error creating Redis client: {}", e))?;

    let mut manager = timeout(
        Duration::from_secs(REDIS_CONNECT_TIMEOUT_SECS),
        ConnectionManager::new(client.clone()),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "Timed out connecting to Redis at {}. Is the server online?",
            redis_url
        )
    })?
    .map_err(|e| anyhow!("Failed to connect to Redis at {}: {}", redis_url, e))?;

    timeout(
        Duration::from_secs(REDIS_PING_TIMEOUT_SECS),
        redis::cmd("PING").query_async::<String>(&mut manager),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "Timed out pinging Redis at {}. Check server health.",
            redis_url
        )
    })?
    .map_err(|e| anyhow!("Failed to ping Redis at {}: {}", redis_url, e))?;

    info!("Successfully connected to Redis at {}", redis_url);

    Ok(client)
}

/// Transform a BboMessage into a Redis struct per-symbol and send it to the symbol's BBO stream
/// Caps stream length at `MAX_STREAM_LEN`
pub async fn bbo_to_redis(
    bbos: Vec<BboMessage>,
    conn: &mut ConnectionManager,
) -> anyhow::Result<()> {
    if bbos.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    for bbo in bbos {
        let entry: Vec<(&str, String)> = vec![
            ("exchange", bbo.exchange.clone()),
            ("symbol", bbo.symbol.clone()),
            ("timestamp", bbo.timestamp.to_string()),
            ("best_bid_price", bbo.best_bid_price.to_string()),
            ("best_bid_qty", bbo.best_bid_qty.to_string()),
            ("best_ask_price", bbo.best_ask_price.to_string()),
            ("best_ask_qty", bbo.best_ask_qty.to_string()),
        ];

        let symbol_key = format!("bbo:{}", bbo.symbol);
        pipe.cmd("XADD")
            .arg(symbol_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAX_STREAM_LEN)
            .arg("*")
            .arg(entry)
            .ignore();
    }

    pipe.query_async::<()>(conn).await?;
    Ok(())
}

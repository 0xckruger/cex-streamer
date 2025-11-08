use crate::redis::bbo_to_redis;
use anyhow::anyhow;
use async_trait::async_trait;
use derive_more::Display;
use futures::{SinkExt, StreamExt};
use redis::Client;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::task;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, info, warn};
use url::Url;

#[derive(Debug, Deserialize)]
pub struct SubscriptionConfig {
    pub(crate) exchange: String,
    pub(crate) enabled: bool,
    wss_uri: String,
    streams: Vec<String>,
    symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SymbolConfig {
    symbol: String,
    enabled: bool,
    base: String,
    quote: String,
}

/// Universal BBO format
#[derive(Debug, Clone, serde::Deserialize, Display)]
#[display(
    "{exchange}/{symbol}: {best_bid_price} @ {best_ask_price} [#{best_ask_qty} x #{best_bid_qty}]"
)]
pub struct BboMessage {
    pub(crate) exchange: String,
    pub(crate) timestamp: u64,
    pub(crate) symbol: String,
    pub(crate) best_bid_price: f32,
    pub(crate) best_bid_qty: f32,
    pub(crate) best_ask_price: f32,
    pub(crate) best_ask_qty: f32,
}

/// Binance BBO to Universal BBO
impl TryFrom<BinanceBookTickerMessage> for BboMessage {
    type Error = anyhow::Error;

    fn try_from(value: BinanceBookTickerMessage) -> Result<Self, Self::Error> {
        Ok(BboMessage {
            exchange: "binance".to_owned(),
            timestamp: value.event_time,
            symbol: value.symbol,
            best_bid_price: value.best_bid_price.parse::<f32>()?,
            best_bid_qty: value.best_bid_qty.parse::<f32>()?,
            best_ask_price: value.best_ask_price.parse::<f32>()?,
            best_ask_qty: value.best_ask_qty.parse::<f32>()?,
        })
    }
}

/// Exchange Client trait
#[async_trait]
pub trait ExchangeClient: Send + Sync {
    type MessageType: Clone + Send + 'static;
    fn new(config: SubscriptionConfig) -> Self;
    fn config(&self) -> &SubscriptionConfig;

    /// Creates JSON parameters to subscribe to stream with
    fn build_subscription(&self, config: &SubscriptionConfig, id: u32) -> anyhow::Result<String>;

    /// Opposite of build_subscription, creates unsubscribe message
    fn build_unsubscription(
        &self,
        subscription_config: &SubscriptionConfig,
    ) -> anyhow::Result<String>;

    /// Decodes exchange specific messages
    fn decode_message(&self, raw: &str) -> anyhow::Result<Self::MessageType>;

    /// Public-facing client start method that manages dropped WS connections
    async fn run(
        &self,
        tx: broadcast::Sender<Self::MessageType>,
        cancellation_token: CancellationToken,
    ) {
        let mut retry = 1;
        let max_retries = 5;
        while !cancellation_token.is_cancelled() {
            match self._run_loop(tx.clone(), cancellation_token.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    warn!("Disconnected from WebSocket with error: {e}");
                    if retry < max_retries {
                        warn!(
                            "Attempting reconnection (attempt {}/{})...",
                            retry, max_retries
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(retry * 5)).await;
                        retry += 1;
                    } else {
                        error!("Could not reconnect to WebSocket! Exiting");
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    /// Actual run loop for connecting to and dealing with WebSocket messages -- should only be
    /// called by run(), not called by itself
    async fn _run_loop(
        &self,
        tx: broadcast::Sender<Self::MessageType>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let url = Url::parse(&*self.config().wss_uri)?;
        let subscription_msg = self.build_subscription(&self.config(), 1)?;

        let (ws_stream, _) = connect_async(url.as_str()).await?;
        info!("Connected to {}", self.config().exchange);

        let (mut writer, mut reader) = ws_stream.split();

        match writer
            .send(Message::Text(subscription_msg.to_string().into()))
            .await
        {
            Ok(_) => info!("Successfully sent subscription"),
            Err(e) => {
                return Err(anyhow!(
                    "Failed to send subscription message {} due to error: {}",
                    subscription_msg,
                    e
                ));
            }
        }

        // TODO: Add watchdog timeout/pings
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Unsubscribing from streams and exiting");
                    match self.build_unsubscription(&self.config()) {
                        Ok(unsubscription_msg) => {
                            if let Err(e) = writer.send(Message::Text(unsubscription_msg.into())).await {
                                warn!("Unable to send unsubscribe message due to error {}, exiting without unsubscribing", e)
                            }
                        }
                        Err(e) => warn!("Unable to build unsubscribe message due to error {}, exiting without unsubscribing", e)
                    }
                    if let Err(e) = writer.send(Message::Close(None)).await {
                        warn!("Unable to send close message due to error {}, exiting anyway", e);
                    }
                    return Ok(())
                }
                next = reader.next() => {
                    if let Some(msg) = next {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if tracing::enabled!(Level::DEBUG) {
                                    debug!("{:?}", text);
                                }
                                match self.decode_message(&*text.to_string()) {
                                    Ok(message) => {
                                        if let Err(e) = tx.send(message) {
                                            error!(
                                                "Failed to forward message to receivers: {} (possibly no active receivers or receivers lagging)",
                                                e
                                            );
                                        }
                                    }
                                    Err(e) => error!("Failed to decode message {} due to error: {}", text, e),
                                }
                            }
                            Ok(Message::Ping(p)) => {
                                writer.send(Message::Pong(p)).await.ok();
                            }
                            Ok(Message::Pong(_)) => {
                                info!("Server pong");
                            }
                            Ok(Message::Binary(b)) => {
                                warn!("Received unexpected binary message: {:?}", b);
                            }
                            Ok(Message::Frame(f)) => {
                                debug!("Received frame message: {:?}", f);
                            }
                            Ok(Message::Close(_)) => {
                                info!("Server closed connection");
                                return Ok(())
                            }
                            Err(e) => {
                                return Err(anyhow!("WebSocket error: {e}"));
                            }
                        }
                    } else {
                        return Err(anyhow!("WebSocket stream ended unexpectedly"))
                    }
                }
            }
        }
    }
}

/// Binance implementation

/// Wrapper type for Binance Messages
#[derive(Debug, serde::Deserialize)]
struct JsonWrapper {
    stream: String,
    data: Value,
}
/// Generic Binance Event
#[derive(Debug, Clone)]
pub enum BinanceEvent {
    BookTicker(BinanceBookTickerMessage),
    Unknown(Value),
}

/// Binance bookTicker message (aka BBO)
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BinanceBookTickerMessage {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T")]
    transaction_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
}

/// Binance Client definition (same across all clients)
pub struct BinanceClient {
    config: SubscriptionConfig,
}

impl BinanceClient {
    pub(crate) fn start_bbo_publisher(
        redis_client: Client,
        mut bbo_rx: Receiver<BinanceEvent>,
        cancellation_token: CancellationToken,
        batch_size: usize,
        batch_timeout_ms: u64,
    ) -> task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut conn = match ConnectionManager::new(redis_client).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to Redis: {}", e);
                    return;
                }
            };

            let mut buffer = Vec::with_capacity(batch_size);

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        flush_bbo_buffer(&mut buffer, &mut conn).await;
                        info!("Exiting BBO publisher");
                        break;
                    }

                    msg = bbo_rx.recv() => {
                        if let Ok(BinanceEvent::BookTicker(book_ticker)) = msg {
                            if let Ok(bbo) = BboMessage::try_from(book_ticker) {
                                buffer.push(bbo);
                                if buffer.len() >= batch_size {
                                    flush_bbo_buffer(&mut buffer, &mut conn).await;
                                }
                            } else {
                                warn!("Failed to convert to BboMessage");
                            }
                        } else if msg.is_err() {
                            break;
                        }
                    }

                    // Periodically flush buffer
                    _ = sleep(Duration::from_millis(batch_timeout_ms)) => {
                        flush_bbo_buffer(&mut buffer, &mut conn).await;
                    }
                }
            }
        })
    }
}

async fn flush_bbo_buffer(buffer: &mut Vec<BboMessage>, conn: &mut ConnectionManager) {
    if buffer.is_empty() {
        return;
    }
    if let Err(e) = bbo_to_redis(buffer.drain(..).collect(), conn).await {
        error!("Failed to flush BBO buffer: {}", e);
    }
}

#[async_trait]
impl ExchangeClient for BinanceClient {
    type MessageType = BinanceEvent;

    fn new(config: SubscriptionConfig) -> Self {
        BinanceClient { config }
    }

    fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    fn build_subscription(&self, config: &SubscriptionConfig, id: u32) -> anyhow::Result<String> {
        if config.streams.is_empty() {
            Err(anyhow::anyhow!(
                "No streams provided, stream type(s) must be specified"
            ))
        } else if config.symbols.is_empty() {
            Err(anyhow::anyhow!(
                "No symbols provided, symbol list may not be empty"
            ))
        } else {
            let symbol_params: Vec<String> = config
                .symbols
                .iter()
                .filter(|s| s.enabled)
                .flat_map(|s| {
                    config
                        .streams
                        .iter()
                        .map(move |stream| format!("{}@{}", s.symbol.to_lowercase(), stream))
                })
                .collect();
            Ok(json!({
                "method": "SUBSCRIBE",
                "params": symbol_params,
                "id": 1
            })
            .to_string())
        }
    }

    fn build_unsubscription(
        &self,
        subscription_config: &SubscriptionConfig,
    ) -> anyhow::Result<String> {
        let unsubscribe_params = self.build_subscription(subscription_config, 999)?;
        Ok(unsubscribe_params.replace("\"SUBSCRIBE\"", "UNSUBSCRIBE"))
    }

    fn decode_message(&self, raw: &str) -> anyhow::Result<Self::MessageType> {
        let value: Value = serde_json::from_str(raw)?;
        if value.get("result").is_some() {
            debug!("Ignoring Binance control message: {}", value);
            return Ok(BinanceEvent::Unknown(value));
        }

        let wrapper: JsonWrapper = serde_json::from_str(raw)?;
        let event_type = wrapper
            .data
            .get("e")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match event_type {
            "bookTicker" => {
                let msg: BinanceBookTickerMessage = serde_json::from_value(wrapper.data)?;
                Ok(BinanceEvent::BookTicker(msg))
            }
            _ => Ok(BinanceEvent::Unknown(wrapper.data)),
        }
    }
}

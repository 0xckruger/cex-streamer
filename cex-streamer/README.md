# cex-streamer

`cex-streamer` subscribes to a websocket feed from Binance (with the option of adding other exchanges easily in the future)
for a given stream type and exports it to Redis. The repo also includes a sibling example program, `bbo-viewer`, which
demonstrates how to visualize those Redis streams from the terminal.

Currently, the only supported stream type is [bookTicker](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams),
which is the best bid/ask price/quantity for a given symbol. Like exchanges, stream types may be easily added in the future.

A few things are required:
- Rust
- A JSON config file (provided is usable example)
- Redis (through the provided docker-compose.yml file or a local installation)
- Environment vars `REDIS_HOSTNAME` set, e.g. `REDIS_HOSTNAME=localhost:6379` (can be set in a new .env file)

The JSON config file contains, per exchange:
- Exchange name
- "Enabled" boolean
- The WSS url for the exchange
- A list of streams to subscribe to 
- A list of symbols to subscribe to per stream

To run:

`
docker-compose up -d
` 
to start a Redis instance through Docker (make sure you have Docker installed).

`
cargo run -- -c ./config.json
`
to start the program.

Enable verbose mode with `-v` or set in env `RUST_LOG=debug` (warning: spammy)



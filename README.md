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

To run cex-streamer

`
docker-compose up -d
`
in cex-streamer to start a Redis instance through Docker (make sure you have Docker installed).

`
cargo run -p cex-streamer -- -c ./config.json
`
to start the program.

Enable verbose mode with `-v` or set in env `RUST_LOG=debug` (warning: spammy)

## bbo-viewer

Read from the `cex-streamer` Redis BBO stream(s) in the terminal

Make sure `REDIS_HOSTNAME` is set in .env

Usage: `-s` to pass in symbols to display (`./bbo-viewer -s BTCUSDT ETHUSDT` for example)

As a default it will display BTC/USDT, ETH/USDT, and SOL/USDT.

`
cargo run -p bbo-viewer
`

Looks something like this (very simple)

<img width="824" height="232" alt="Screenshot 2025-11-07 at 9 55 49 PM" src="https://github.com/user-attachments/assets/53bb63e1-c4d2-4280-baf8-5ae9a2e3dcc2" />




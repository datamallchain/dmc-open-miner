#!/bin/bash
set -e
cargo build -p dmc-miner-account-server --features eos --release
cargo build -p dmc-miner-contracts --features eos --release
cargo build -p dmc-miner-journal --release
cargo build -p dmc-miner-sectors-node --release
cargo build -p dmc-miner-sectors-gateway --release
cargo build -p dmc-spv --features eos --release
cargo build -p dmc-miner-client --release
cargo build -p dmc-miner-lite-server --release

strip ./target/release/dmc-miner-account-server
strip ./target/release/dmc-miner-contracts
strip ./target/release/dmc-miner-journal
strip ./target/release/dmc-miner-sectors-node
strip ./target/release/dmc-miner-sectors-gateway
strip ./target/release/dmc-spv
strip ./target/release/dmc-miner-client
strip ./target/release/dmc-miner-lite-server

rm -rf miner-dist
mkdir miner-dist

cp ./target/release/dmc-miner-account-server ./miner-dist
cp ./target/release/dmc-miner-contracts ./miner-dist
cp ./target/release/dmc-miner-journal ./miner-dist
cp ./target/release/dmc-miner-sectors-node ./miner-dist
cp ./target/release/dmc-miner-sectors-gateway ./miner-dist
cp ./target/release/dmc-spv ./miner-dist
cp ./target/release/dmc-miner-client ./miner-dist
cp ./target/release/dmc-miner-lite-server ./miner-dist
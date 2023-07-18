#!/bin/bash
set -e
cargo build -p dmc-miner-client --release
cargo build -p dmc-miner-lite-server --release

strip ./target/release/dmc-miner-client
strip ./target/release/dmc-miner-lite-server

rm -rf ../bin/linux
mkdir ../bin/linux

cp ./target/release/dmc-miner-client ../bin/linux
cp ./target/release/dmc-miner-lite-server ../bin/linux
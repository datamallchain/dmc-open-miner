#!/bin/bash
set -e
cargo build -p dmc-user-lite-client -p dmc-user-lite-server --release

strip ./target/release/dmc-user-lite-client
strip ./target/release/dmc-user-lite-server

rm -rf ../bin/linux
mkdir ../bin/linux

cp ./target/release/dmc-user-lite-client ../bin/linux
cp ./target/release/dmc-user-lite-server ../bin/linux
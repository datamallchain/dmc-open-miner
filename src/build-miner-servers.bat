@echo off

cargo build -p dmc-miner-account-server --features eos --release
cargo build -p dmc-miner-contracts --features eos --release
cargo build -p dmc-miner-journal --release
cargo build -p dmc-miner-sectors-node --release
cargo build -p dmc-miner-sectors-gateway --release
cargo build -p dmc-spv --features eos --release
cargo build -p dmc-miner-client --release
cargo build -p dmc-miner-lite-server --release

rmdir /S /Q miner-dist
mkdir miner-dist

copy /Y .\target\release\dmc-miner-account-server.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-contracts.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-journal.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-sectors-node.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-sectors-gateway.exe .\miner-dist\
copy /Y .\target\release\dmc-spv.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-client.exe .\miner-dist\
copy /Y .\target\release\dmc-miner-lite-server.exe .\miner-dist\
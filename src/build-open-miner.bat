@echo off

cargo build -p dmc-miner-client --release
cargo build -p dmc-miner-lite-server --release

rmdir /S /Q ..\bin\win
mkdir ..\bin\win

copy /Y .\target\release\dmc-miner-client.exe ..\bin\win
copy /Y .\target\release\dmc-miner-lite-server.exe ..\bin\win
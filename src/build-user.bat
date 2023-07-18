@echo off
cargo build -p dmc-user-lite-client -p dmc-user-lite-server --release

rmdir /S /Q ..\bin\win
mkdir ..\bin\win

copy /Y target\release\dmc-user-lite-client.exe ..\bin\win
copy /Y target\release\dmc-user-lite-server.exe ..\bin\win
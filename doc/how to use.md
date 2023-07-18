There's a lite client for user to back up or restore a local directory with DMC network. To use dmc lite user client, follow next steps:
+ Download compatible dmc user lite tools package for your OS (windows/linux ubuntu/mac);
+ Unzip package to your install path;
+ Modify ${install path}/eos-config.toml, write your account name and secret registered on DMC chain (makesure you've got some DMC before) in `account` field;
```json
host = "explorer.dmctech.io"
chain_id = "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d"
account = ["account name", "account private key"]
piece_size = 1024

[retry_rpc_interval]
secs = 1
nanos = 0

[trans_expire]
secs = 600
nanos = 0
```
+ cd your ${install path}, execute dmc-user-lite-client; it will run dmc-user-lite-server on backend, do not kill it;
+ execute `dmc-user-lite-client resiter` with a local path you want to back up; then got a source_id returned;
```
PS F:\source\dmc-tools\src\target\debug> ./dmc-user-lite-client.exe register --file "C:\dmc_tools\tests\data\write_restore\user\order_source.data"
checking user server...
use url http://localhost:7236/account/address
user server started, account name testdmcdsg11
register file to source service C:\dmc_tools\tests\data\write_restore\user\order_source.data
source registered, source_id is 2
```
+ a zip file will be created in your OS tmp directory; do not remove it before back up finished;
```
C:\Users\tsukasa\AppData\Local\Temp\0985c591.zip
```
+ execute `dmc-user-lite-client backup` with source_id returned before and a duration in weeks;

```
PS F:\source\dmc-tools\src\target\debug> ./dmc-user-lite-client.exe backup --source-id 2 --duration 24
checking user server...
use url http://localhost:7236/account/address
user server started, account name testdmcdsg11
backup 2 to DMC network
wait prepare source ready
source ready
backup started, sector_id=1
watching sector_id=1 backup progress
ordered on DMC network order_id=197
source has backup to miner
```
+ back up process may last a period time (depends on how large the source data is), dmc-user-lite-service will order to a bill on DMC smart contract, prepare it, and transfer data to miner side in backend automaticaly; 
+ in this process, when sector_id returned, you can remember it, and exit dmc-user-lite-client if you dont want to wait until finished;
```
backup started, sector_id=1
```
+ when order_id returned, means that a order has created on DMC smart contract, you may check this order infortion will DMC block explorer on web;
```
ordered on DMC network order_id=197
``` 
+ when data transferred and merkle root checked on both side, back up process finished;
+ you can execute `dmc-user-lite-client list` to see all backup processes running in dmc-user-lite-server;
```
PS F:\source\dmc-tools\src\target\debug> ./dmc-user-lite-client.exe list
checking user server...
use url http://localhost:7236/account/address
user server started, account name testdmcdsg11
list sectors page=0
sector_id:1
```
+ you can execute `dmc-user-lite-client watch` to see backup progress of a sector_id;
```
PS F:\source\dmc-tools\src\target\debug> ./dmc-user-lite-client.exe watch --sector-id 1
checking user server...
use url http://localhost:7236/account/address
user server started, account name testdmcdsg11
watching sector_id=1 backup progress
ordered on DMC network order_id=197
source has backup to miner
```  
+ when backup finished; source local path can be removed;
+ you can execute `dmc-user-lite-client restore` to restore your data backing up on miner to local path;   
```
PS F:\source\dmc-tools\src\target\debug> ./dmc-user-lite-client.exe restore --sector-id 1 --path ./
checking user server...
use url http://localhost:7236/account/address
user server started, account name testdmcdsg11
restore sector_id=1 to ./
restored to local
``` 
+ restore process may last a period time (depends on how large it is), if you exit dmc-user-lite-client before it's finished, restore process will pause, and resume at next time you execute restore with same sector id and local path; 

+ You can use the command `dmc-user-lite-client journal --source-id <SOURCE_ID> --sector-id <SECTOR_ID>` to view the key journal corresponding to source or sector
	- --sector-id is valid only when backup generates order id. Pass in a sector id that has not yet generated an order, and this parameter will be ignored
	- If both --source-id and --sector-id are specified, both conditions will be matched
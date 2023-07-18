# Mico-services architecture
![alt architecture and flow](./how%20it%20works.jpg)
We developed serveral micro-services on both miner and user side; users intent to backup their local files to miners on DMC network with some cost of tokens, meanwhile miners intend to store datas from users for earning tokens.

On user side, the following micro-services should be deployed:
+ source service: this service manages sources to backup, user input a local path on OS file system to source service, service will create an unique source id for it; service then calculates necessary stubs(merkle root, serveral pieces on random range) for onchain and offchain challenge to miner who stores the source;
+ account service: this service manages process of ordering a miner's bill for sources to backup on DMC smart contract, when bound a order on source, service deliver this order to contract service to finish backup process;
+ contract service: this service interacts with miner side directly to finish data delivery, off-chain challenge.

On miner side, the following micro-services should be depolyed:
+ sector service: this service manages disk spaces to bill on DMC network, miner input local pathes mounted on OS file system, service will create an unique raw sector id for it; service also process IO operations to sectors.
+ account service: this service manages process of billing raw sectors to DMC smart contract with miner defined params such as price, pledge rate; and when user orders, service deliver this order to contract service to finish storage process;
+ contract service: this service interacts with user side directly to finish data delivery, off-chain challenge.

# Backup process
+ miner side
    + flow 0: Miner mounts local pathes to disk spaces to bill on DMC smart contract;
    + flow 1: Miner registers pathes to sector service, and then gets its raw sector id;
    + flow 2: Miner registers raw sector id to account service with defined bill params;
    + flow 3: Account service sends bill transaction to DMC smart contract with defined params, and then gets its bill id. Account service starts watching incoming orders on this bill.

+ user side
    + flow 4: User generates data and writes to files on OS file system;
    + flow 5: User registers local pathes to source serivce, gets its source id. Source service starts calculating necessary stubs.
    + flow 6: User registers source id to account service with defined order params;
    + flow 7: Account service explores bills on DMC smart contract, finds a bill id whose params matches defined order params, sends a order transaction on this bill id, and then gets its order id. Contract service also sends a prepare data transaction with source's merkle root(got with flow 12) on order id.
    + flow 15: Account service delivers this order to contract service, tells contract service which source id should be backed up to which miner.
    + flow 11: Contract service starts sending data(got with flow 13) to miner side.

+ miner side
    + flow 8: When incoming order event reached, account service deliver incoming order id to contract service to interact with user side.
    + flow 11: When data from user side incomes, contract service stores them to local pathes bound with raw sector id(flow 9);
    + flow 10: When all data writen, contract service calculates merkle root for data(got with flow 9), then sends a prepare data transaction on order id. If 2 merkle root from miner and user side matches, backup process finished.

+ user side
    + flow 11: Contract service will challenge miners side with source's stubs(got by flow 12) periodically. 
    + flow 14: If off-chain challenge fails, contract service may start a on-chain challenge on DMC smart contract.

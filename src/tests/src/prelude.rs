use std::{
    time::Duration, 
    ops::Range, 
    collections::BTreeMap, 
    path::{Path, PathBuf}
};
use async_std::task;
use tide::http::Url;
pub use dmc_tools_common::*;
pub use dmc_spv::*;
pub mod miner {
    pub use dmc_miner_journal as journal;
    pub mod sector {
        pub use dmc_miner_sectors_client::*;
        pub use dmc_miner_sectors_node::*;
    }
    pub use dmc_miner_contracts as contract;
    pub mod account {
        pub use dmc_miner_account_client::*;
        pub use dmc_miner_account_server::*;
    }
}
pub mod user {
    pub use dmc_user_journal as journal;
    pub use dmc_user_sources as source;
    pub mod account {
        pub use dmc_user_account_client::*;
        pub use dmc_user_account_server::*;
    }
    pub use dmc_user_contracts as contract;
}


pub struct DmcComponentsConfig {
    pub case_name: String, 
    pub start_port: u16, 
    pub use_spv: bool, 
    pub miners: Vec<String>, 
    pub users: Vec<String>
}


#[cfg(feature = "fake")]
mod chain {
    use crate::fake_chain::*;

    pub type Chain = FakeChain;

    pub fn init_chain() -> Chain {
        FakeChain::new()
    }

    pub type ChainClient = FakeChainClient;

    pub fn new_chain_client(chain: &Chain) -> ChainClient {
        FakeChainClient::none_account(chain.clone())
    }

    pub fn new_chain_account_client(chain: &Chain, account: &str) -> ChainClient {
        FakeChainClient::with_account(account.to_owned(), chain.clone())
    }
}


#[cfg(feature = "eos")]
mod chain {
    use dmc_eos::*;
    
    pub type Chain = ();

    pub fn init_chain() -> Chain {
        
    }

    pub type ChainClient = EosClient;

    pub fn new_chain_client(_: &Chain) -> ChainClient {
        async_std::task::block_on(EosClient::new(EosClientConfig { 
            host: "test.dmctech.io".to_owned(), 
            account: None, 
            chain_id: "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d".to_owned(), 
            retry_rpc_interval: std::time::Duration::from_secs(1), 
            trans_expire: std::time::Duration::from_secs(60),  
            piece_size: 1024
        })).unwrap()
    }

    pub fn new_chain_account_client(_: &Chain, account: &str) -> ChainClient {
        match account {
            "miner" => {
                async_std::task::block_on(EosClient::new(EosClientConfig { 
                    host: "test.dmctech.io".to_owned(), 
                    account: Some(("testdmcdsg11".to_owned(), "5JFWdMQtzyMwU5J2ZQdiBgkY1CgpqkFfPCLRidmSapSMU2aDw4n".to_owned())), 
                    chain_id: "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d".to_owned(), 
                    retry_rpc_interval: std::time::Duration::from_secs(1), 
                    trans_expire: std::time::Duration::from_secs(60),  
                    piece_size: 1024
                })).unwrap()
            },
            "user" => {
                async_std::task::block_on(EosClient::new(EosClientConfig { 
                    host: "test.dmctech.io".to_owned(), 
                    account: Some(("testdmcdsg12".to_owned(), "5HvcWxZAvAZ5D49WKfhodsxMPVJ7CdQSoNHbY95WJtk2kpWDsFQ".to_owned())), 
                    chain_id: "4d1fb981dd562d2827447dafa89645622bdcd4e29185d60eeb45539f25d2d85d".to_owned(), 
                    retry_rpc_interval: std::time::Duration::from_secs(1), 
                    trans_expire: std::time::Duration::from_secs(60),  
                    piece_size: 1024
                })).unwrap()
            },
            _ => unreachable!()
        }
    }
}


pub struct DmcMinerComponent {
    account_host: String, 
    contract_host: String, 
    sector_host: String, 
    sector_dir: PathBuf, 
    chain_client: chain::ChainClient
}

impl DmcMinerComponent {
    pub fn account_host(&self) -> &str {
        self.account_host.as_str()
    }

    pub fn contract_host(&self) -> &str {
        self.contract_host.as_str()
    }

    pub fn account_client(&self) -> miner::account::AccountClient {
        miner::account::AccountClient::new(miner::account::AccountClientConfig {
            endpoint: Url::parse(&format!("http://{}", &self.account_host)).unwrap()
        }).unwrap()
    }

    pub fn contract_client(&self) -> miner::contract::ContractClient {
        miner::contract::ContractClient::new(miner::contract::ContractClientConfig {
            endpoint: Url::parse(&format!("http://{}", self.contract_host.clone())).unwrap(),
        }).unwrap()
    }
    
    pub fn sector_host(&self) -> &str {
        &self.sector_host
    }

    pub fn sector_admin(&self) -> miner::sector::SectorAdmin {
        miner::sector::SectorAdmin::new(miner::sector::SectorAdminConfig {
            endpoint: Url::parse(&format!("http://{}", &self.sector_host)).unwrap()
        }).unwrap()
    }

    pub fn sector_dir(&self) -> &Path {
        self.sector_dir.as_path()
    }

    pub fn chain_client(&self) -> &chain::ChainClient {
        &self.chain_client
    }
}

pub struct DmcUserComponent {
    journal_host: String, 
    source_host: String, 
    account_host: String, 
    contract_host: String, 
    data_dir: PathBuf
}

impl DmcUserComponent {
    pub fn account_host(&self) -> &str {
        self.account_host.as_str()
    }

    pub fn contract_host(&self) -> &str {
        self.contract_host.as_str()
    }

    pub fn source_client(&self) -> user::source::SourceClient {
        user::source::SourceClient::new(user::source::SourceClientConfig {
            host: self.source_host.clone()
        }).unwrap()
    }

    pub fn account_client(&self) -> user::account::AccountClient {
        user::account::AccountClient::new(user::account::AccountClientConfig {
            host: self.account_host.clone()
        }).unwrap()
    }

    pub fn contract_client(&self) -> user::contract::ContractClient {
        user::contract::ContractClient::new(user::contract::ContractClientConfig {
            host: self.contract_host.clone()
        }).unwrap()
    }

    pub fn journal_client(&self) -> user::journal::JournalClient {
        user::journal::JournalClient::new(user::journal::JournalClientConfig {
            host: self.journal_host.clone()
        }).unwrap()
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir.as_path()
    }
}


pub struct DmcComponents {
    miners: BTreeMap<String, DmcMinerComponent>, 
    users: BTreeMap<String, DmcUserComponent>, 
    ports: Range<u16>, 
    chain: chain::Chain
}

impl DmcComponents {
    pub fn miner(&self, miner: &str) -> &DmcMinerComponent {
        self.miners.get(miner).unwrap()
    }

    pub fn user(&self, user: &str) -> &DmcUserComponent {
        self.users.get(user).unwrap()
    }

    pub fn ports(&self) -> Range<u16> {
        self.ports.clone()
    }
}

pub async fn init_components(config: DmcComponentsConfig) -> DmcComponents {
    let mut cur_port = config.start_port;

    let chain = chain::init_chain();
    
    let data_root; 
    #[cfg(target_os = "windows")]
    {
        data_root = Path::new("C:\\dmc_tools\\tests\\data");
    }
    #[cfg(not(target_os = "windows"))]
    {
        data_root = Path::new("/dmc_tools/tests/data");
    }

    std::fs::create_dir_all(data_root).unwrap();  

    let case_root = data_root.join(&config.case_name);
    if case_root.exists() {
        std::fs::remove_dir_all(case_root.as_path()).unwrap();
    } 
    std::fs::create_dir(case_root.as_path()).unwrap();
   
    let mysql_host = "mysql://root:123456@localhost:3306".to_owned();

    let spv_host = if config.use_spv {
        let spv_host = format!("http://127.0.0.1:{}", cur_port);
        cur_port += 1;

        let mysql_url = format!("{}/{}", mysql_host, "spv");

        let config = SpvServerConfig {
            host: spv_host.clone(),  
            sql_url: mysql_url,
        };

        let chain_client = chain::new_chain_client(&chain);

        let server = SpvServer::new(chain_client, config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });
        Some(spv_host)
    } else {
        None
    };

    let mut miners = BTreeMap::new();
    
    for miner in config.miners {        
        let miner_journal_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let miner_sector_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;


        let miner_account_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let miner_contract_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let config = miner::journal::JournalServerConfig {
            listen_address: miner_journal_host.clone(),
            sql_url: format!("{}/{}", mysql_host, &miner),
        };
        let server = miner::journal::JournalServer::new(config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        let mysql_url = format!("{}/{}", mysql_host, miner);

        let config = miner::sector::SectorsNodeConfig {
            listen_address: miner_sector_host.clone(),
            node_id: 1, 
            sql_url: format!("{}/{}", mysql_host, &miner),
            journal_client: miner::journal::JournalClientConfig {
                endpoint: Url::parse(&format!("http://{}", miner_journal_host)).unwrap()
            }
        };
        let server = miner::sector::SectorsNode::new(config).await.unwrap();
        server.reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        let config = miner::account::AccountServerConfig {
            listen_address: miner_account_host.clone(),
            sql_url: mysql_url.clone(), 
            retry_event_interval: Duration::from_secs(1), 
            bill_atomic_interval: Duration::from_secs(5), 
            bill_retry_interval: Duration::from_secs(5), 
            journal_client: miner::journal::JournalClientConfig {
                endpoint: Url::parse(&format!("http://{}", miner_journal_host)).unwrap()
            }, 
            sector_client: miner::sector::SectorClientConfig {
                endpoint: Url::parse(&format!("http://{}", &miner_sector_host)).unwrap(),
            },
            contract_client: miner::contract::ContractClientConfig {
                endpoint: Url::parse(&format!("http://{}", &miner_contract_host)).unwrap(),
            }
        };

        let chain_client = chain::new_chain_account_client(&chain, &miner);
        
        let server = miner::account::AccountServer::new(chain_client, config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });


        let config = miner::contract::ContractsServerConfig {
            prepare_atomic_interval: Duration::from_secs(1), 
            prepare_retry_interval: Duration::from_secs(1), 
            journal_client: miner::journal::JournalClientConfig {
                endpoint: Url::parse(&format!("http://{}", miner_journal_host)).unwrap()
            }, 
            sector_client: miner::sector::SectorClientConfig {
                endpoint: Url::parse(&format!("http://{}", &miner_sector_host)).unwrap(),
            }, 
            prefix_table_name: false
        };

        {
            let chain_client = chain::new_chain_account_client(&chain, &miner);
            let server = miner::contract::ContractsServer::with_sql_url(chain_client, mysql_url.clone(), config).await.unwrap().reset().await.unwrap();
            let miner_contract_host = miner_contract_host.clone();
            task::spawn(async move {
                let _ = server.listen(miner_contract_host).await.unwrap();
            });
        }
       

        let sector_dir = case_root.join(&miner);
        std::fs::create_dir(sector_dir.as_path()).unwrap();
        let chain_client = chain::new_chain_account_client(&chain, &miner);
        miners.insert(miner, DmcMinerComponent {
            chain_client, 
            account_host: miner_account_host, 
            contract_host: miner_contract_host, 
            sector_host: miner_sector_host, 
            sector_dir,
        });
    }


    let mut users = BTreeMap::new();
    for user in config.users {
        let data_dir = case_root.join(&user);
        std::fs::create_dir(data_dir.as_path()).unwrap();

        let user_journal_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let user_source_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let user_account_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;

        let user_contract_host = format!("127.0.0.1:{}", cur_port);
        cur_port += 1;


        let sql_url;
        #[cfg(feature="mysql")] 
        {
            sql_url = format!("{}/{}", mysql_host, &user);
        }
        #[cfg(feature="sqlite")] 
        {
            let db_path = data_dir.join("server.db");

            #[cfg(target_os = "windows")]
            {
                let parts: Vec<String> = db_path.components().enumerate().filter_map(|(i, p)| if i == 1 { None } else { Some(p.as_os_str().to_str().unwrap().to_owned()) }).collect();
                let db_path = parts.join("/");
                sql_url = format!("sqlite:///{}", db_path);
            }
            #[cfg(not(target_os = "windows"))]
            {   
                sql_url = format!("sqlite:///{}", db_path.display());
            }
        }
       

        let config = user::journal::JournalServerConfig {
            host: user_journal_host.clone(), 
            sql_url: sql_url.clone(), 
        };
        let server = user::journal::JournalServer::new(config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        let config = user::source::SourceServerConfig {
            host: user_source_host.clone(), 
            sql_url: sql_url.clone(), 
            journal_client: user::journal::JournalClientConfig {
                host: user_journal_host.clone()
            }, 
        };
        let server = user::source::SourceServer::new(config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        let config = user::account::AccountServerConfig {
            host: user_account_host.clone(), 
            sql_url: sql_url.clone(), 
            spv_client: spv_host.as_ref().map(|host| SpvClientConfig { host: Url::parse(host).unwrap() }),
            order_atomic_interval: Duration::from_secs(1), 
            order_retry_interval: Duration::from_secs(1), 
            retry_event_interval: Duration::from_secs(1),  
            journal_client: user::journal::JournalClientConfig {
                host: user_journal_host.clone()
            }, 
            journal_listener_interval: Duration::from_secs(1), 
            journal_listener_page_size: 1,  
            contract_client: user::contract::ContractClientConfig {
                host: user_contract_host.clone(), 
            }, 
            source_client: user::source::SourceClientConfig {
                host: user_source_host.clone(), 
            },
            auto_retry_order: None,
            auto_onchain_challenge: false
        };

        let chain_client = chain::new_chain_account_client(&chain, &user);
        let server = user::account::AccountServer::new(chain_client, config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        let config = user::contract::ContractsServerConfig {
            host: user_contract_host.clone(), 
            sql_url: sql_url.clone(), 
            source_client: user::source::SourceClientConfig {
                host: user_source_host.clone(), 
            }, 
            journal_client: user::journal::JournalClientConfig {
                host: user_journal_host.clone()
            }, 
            apply_atomic_interval: Duration::from_secs(1), 
            apply_retry_interval: Duration::from_secs(1),  
            challenge_atomic_interval: Duration::from_secs(10), 
            challenge_interval: Duration::from_secs(10)
        };

        let chain_client = chain::new_chain_account_client(&chain, &user);
        let server = user::contract::ContractsServer::new(chain_client, config).await.unwrap().reset().await.unwrap();
        task::spawn(async move {
            let _ = server.listen().await.unwrap();
        });

        users.insert(user, DmcUserComponent {
            journal_host: user_journal_host, 
            source_host: user_source_host, 
            account_host: user_account_host, 
            contract_host: user_contract_host, 
            data_dir
        });
    }

    task::sleep(Duration::from_secs(1)).await; 
   
    DmcComponents {
        miners, 
        users, 
        ports: config.start_port..cur_port, 
        chain
    }
}


pub fn random_mem(piece: usize, count: usize) -> (usize, Vec<u8>) {
    let mut buffer = vec![0u8; piece * count];
    for i in 0..count {
        let piece_ptr = &mut buffer[i * piece..(i + 1) * piece];
        let bytes = count.to_be_bytes();
        for j in 0..(piece >> 3) {
            piece_ptr[j * 8..(j + 1) * 8].copy_from_slice(&bytes[..]);
        }
    }
    (piece * count, buffer)
}

pub fn local_file_url(path: &Path) -> tide::http::Url {
    #[cfg(target_os = "windows")]
    {
        let parts: Vec<String> = path.components().enumerate().filter_map(|(i, p)| if i == 1 { None } else { Some(p.as_os_str().to_str().unwrap().to_owned()) }).collect();
        let file_path = parts.join("/");
        tide::http::Url::parse(&format!("file://{}", file_path)).unwrap()
    }
    #[cfg(not(target_os = "windows"))]
    {
        tide::http::Url::parse(&format!("file://{}", path.as_os_str().to_str().unwrap())).unwrap()
    }
}

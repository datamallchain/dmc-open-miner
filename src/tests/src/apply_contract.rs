use std::{time::Duration};
use async_std::task;
use dmc_miner_account_client::RemoveSectorOptions;
use crate::prelude::*;
use log::*;

pub async fn write_restore() {
    let config = DmcComponentsConfig {
        case_name: "write_restore".to_owned(),
        start_port: 20000,
        use_spv: false,
        miners: vec!["miner".to_owned()],
        users: vec!["user".to_owned()]
    };


    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();

    let sector_asset = 1;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None,
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(),
        capacity: sector_asset * 1024 * 1024 * 1024,
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(),
        bill: DmcBillOptions {
            asset: sector_asset,
            price: 1,
            pledge_rate: Some(1),
            min_asset: None,
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await
            .unwrap().next_page().await
            .unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let source_path = components.user("user").data_dir().join("order_source.data");
    {
        let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
        let (len, data) = random_mem(1024, 2 * 1024);
        assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);
    }
    let source_url = local_file_url(source_path.as_path());

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: source_url.to_string(),
        length: sector_asset * 1024 * 1024 * 1024,
        merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 },
        stub: user::source::SourceStubOptions { count: 100 }
    }).await.unwrap();

    let source_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if datasource.state > user::source::SourceState::PreparingMerkle {
            break datasource.source_id;
        }
    };

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id,
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    let order_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(order) = sector.info.state {
            break order.order_id;
        }
    };

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
        if let Some(contract) = contract {
            if let miner::contract::ContractState::Storing {..} = contract.state {
                break;
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Storing = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    let restore_reader = components.user("user").contract_client().restore(user::contract::RestoreContractOptions {
        navigator: user::contract::ContractDataNavigator {
            order_id,
            offset: 0,
            len: 2* 1024 * 1024
        }
    }).await.unwrap();

    let restore_path = components.user("user").data_dir().join("order_restore.data");
    {
        let restore_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(restore_path.as_path()).await.unwrap();
        async_std::io::copy(restore_reader, restore_file).await.unwrap();
    }

}

pub async fn write_two() {
    let config = DmcComponentsConfig { 
        case_name: "write_two".to_owned(),
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };

    
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None,
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 3 * 1024 * 1024 * 1024,
        chunk_size: 64 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset * 3,
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25,
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };
    {
        let source_path = components.user("user").data_dir().join("order_source_1.data");
        {
            let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
            let (len, data) = random_mem(1024, 1 * 1024);
            assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);

        }
        let source_url = local_file_url(source_path.as_path());

        let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
            source_url: source_url.to_string(),
            length: sector_asset * 1024 * 1024 * 1024,
            merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 },
            stub: user::source::SourceStubOptions { count: 100 }
        }).await.unwrap();

        let source_id = loop {
            task::sleep(Duration::from_secs(1)).await;
            let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
            if datasource.state > user::source::SourceState::PreparingMerkle {
                break datasource.source_id;
            }
        };

        let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
            source_id,
            order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
        }).await.unwrap();

        let sector_id = sector.sector_id;
        let order_id = loop {
            task::sleep(Duration::from_secs(1)).await;
            let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
            if let user::account::SectorState::PostOrder(order) = sector.info.state {
                break order.order_id;
            }
        };

        loop {
            let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
            if let Some(contract) = contract {
                if let miner::contract::ContractState::Storing {..} = contract.state {
                    break;
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }

        loop {
            let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
            if let user::contract::ContractState::Storing = contract.state {
                break;
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    {
        let source_path = components.user("user").data_dir().join("order_source_2.data");
        {
            let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
            let (len, data) = random_mem(1024, 2 * 1024);
            assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);
        }
        let source_url = local_file_url(source_path.as_path());

        let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
            source_url: source_url.to_string(),
            length: sector_asset * 1024 * 1024 * 1024,
            merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 },
            stub: user::source::SourceStubOptions { count: 100 }
        }).await.unwrap();

        let source_id = loop {
            task::sleep(Duration::from_secs(1)).await;
            let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
            if datasource.state > user::source::SourceState::PreparingMerkle {
                break datasource.source_id;
            }
        };

        let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
            source_id,
            order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
        }).await.unwrap();

        let sector_id = sector.sector_id;
        let order_id = loop {
            task::sleep(Duration::from_secs(1)).await;
            let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
            if let user::account::SectorState::PostOrder(order) = sector.info.state {
                break order.order_id;
            }
        };

        loop {
            let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
            if let Some(contract) = contract {
                if let miner::contract::ContractState::Storing {..} = contract.state {
                    break;
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }

        loop {
            let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
            if let user::contract::ContractState::Storing = contract.state {
                break;
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    info!("write complete");
}


pub async fn offchain_challenge() {
    let config = DmcComponentsConfig { 
        case_name: "offline_challenge".to_owned(), 
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };
     
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let source_path = components.user("user").data_dir().join("order_source.data");
    {
        let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
        let (len, data) = random_mem(1024, 2 * 1024);
        assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);   
    }
    let source_url = local_file_url(source_path.as_path());

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: source_url.to_string(), 
        length: sector_asset * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 }, 
        stub: user::source::SourceStubOptions { count: 100 }
    }).await.unwrap();

    let source_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if datasource.state > user::source::SourceState::PreparingMerkle {
            break datasource.source_id;
        }
    };

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    let order_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(order) = sector.info.state {
            break order.order_id;
        }
    };

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
        if let Some(contract) = contract {
            if let miner::contract::ContractState::Storing {..} = contract.state {
                break;
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }
 
    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Storing = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    let journal_filter = user::journal::JournalFilter {event_type: Some(vec![user::journal::JournalEventType::OffchainChallengeOk]), source_id: None, order_id: None };
    let mut journal_listener = components.user("user").journal_client().listener(journal_filter, None, Duration::from_secs(1), 1);
    journal_listener.next().await.unwrap();
    
}


pub async fn onchain_challenge() {
    let config = DmcComponentsConfig { 
        case_name: "offline_challenge".to_owned(), 
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };
     
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;
   
    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let source_path = components.user("user").data_dir().join("order_source.data");
    {
        let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
        let (len, data) = random_mem(1024, 2 * 1024);
        assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);   
    }
    let source_url = local_file_url(source_path.as_path());

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: source_url.to_string(), 
        length: sector_asset * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 }, 
        stub: user::source::SourceStubOptions { count: 100 }
    }).await.unwrap();

    let source_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if datasource.state > user::source::SourceState::PreparingMerkle {
            break datasource.source_id;
        }
    };

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    let order_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(order) = sector.info.state {
            break order.order_id;
        }
    };

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
        if let Some(contract) = contract {
            if let miner::contract::ContractState::Storing {..} = contract.state {
                break;
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }
 
    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Storing = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    let _ = components.user("user").contract_client().challenge(user::contract::OnChainChallengeOptions { order_id, challenge: None }).await.unwrap();

    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if contract.challenge.is_none() {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }
}


pub async fn malicious_challenge() {
    let config = DmcComponentsConfig { 
        case_name: "offline_challenge".to_owned(), 
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };
     
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 2;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let bill = components.miner("miner").chain_client().get_bill_by_id(bill_id).await.unwrap().unwrap();
    assert_eq!(bill.asset, sector_asset, "asset not match, expected: {}, got: {}", sector_asset, bill.asset);

    let source_path = components.user("user").data_dir().join("order_source.data");
    {
        let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
        let (len, data) = random_mem(1024, 2 * 1024);
        assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);   
    }
    let source_url = local_file_url(source_path.as_path());

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: source_url.to_string(), 
        length: 1 * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 }, 
        stub: user::source::SourceStubOptions { count: 100 }
    }).await.unwrap();

    let source_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if datasource.state > user::source::SourceState::PreparingMerkle {
            break datasource.source_id;
        }
    };

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    let order_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(order) = sector.info.state {
            break order.order_id;
        }
    };

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
        if let Some(contract) = contract {
            if let miner::contract::ContractState::Storing {..} = contract.state {
                break;
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }
 
    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Storing = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    let bill = components.miner("miner").chain_client().get_bill_by_id(bill_id).await.unwrap().unwrap();
    assert_eq!(bill.asset, sector_asset - 1, "asset not match, expected: {}, got: {}", sector_asset - 1, bill.asset);

    let _ = components.user("user").contract_client().challenge(user::contract::OnChainChallengeOptions { 
        order_id, 
        challenge: Some(DmcChallengeParams::SourceStub { 
            piece_index: 0, 
            nonce: "abcd".to_owned(), 
            hash: HashValue::default() 
        }) }).await.unwrap();

    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Canceled = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::contract::ContractState::Canceled = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

}

pub async fn cancel_bill() {
    let config = DmcComponentsConfig { 
        case_name: "cancel_bill".to_owned(), 
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };
     
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;
   
    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let _ = components.miner("miner").account_client().remove_sector(RemoveSectorOptions::from_bill(bill_id)).await;

    let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
    info!("sector state is {:?} after removed.", sector.info.state);
    if let miner::account::SectorState::Removed(bill) = sector.info.state {
        assert!(bill.is_some());
        let bill = bill.unwrap();
        let bill_on_chain = components.miner("miner").chain_client().get_bill_by_id(bill.bill_id).await.unwrap();
        assert!(bill_on_chain.is_none());
    } else {
        panic!("state of the sector is error after removed.");
    }
}




pub async fn rebill_order_asset() {
    let config = DmcComponentsConfig { 
        case_name: "offline_challenge".to_owned(), 
        start_port: 20000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()] 
    };
     
    let components = init_components(config).await;
    let apply_method = format!("http://{}", components.miner("miner").contract_host());
    components.miner("miner").chain_client().set_apply_method(apply_method).await.unwrap().unwrap();


    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 2;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let sector = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(1), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill) = sector.info.state {
            break bill.bill_id;
        }
    };

    let bill = components.miner("miner").chain_client().get_bill_by_id(bill_id).await.unwrap().unwrap();
    assert_eq!(bill.asset, sector_asset, "asset not match, expected: {}, got: {}", sector_asset, bill.asset);

    let source_path = components.user("user").data_dir().join("order_source.data");
    {
        let source_file = async_std::fs::OpenOptions::new().read(true).write(true).create(true).open(source_path.as_path()).await.unwrap();
        let (len, data) = random_mem(1024, 2 * 1024);
        assert_eq!(async_std::io::copy(async_std::io::Cursor::new(data), source_file).await.unwrap(), len as u64);   
    }
    let source_url = local_file_url(source_path.as_path());

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: source_url.to_string(), 
        length: 1 * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Prepare { piece_size: 1024 }, 
        stub: user::source::SourceStubOptions { count: 100 }
    }).await.unwrap();

    let source_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let datasource = components.user("user").source_client().sources(user::source::SourceFilter::from_source_id(datasource.source_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if datasource.state > user::source::SourceState::PreparingMerkle {
            break datasource.source_id;
        }
    };

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    let order_id = loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(order) = sector.info.state {
            break order.order_id;
        }
    };

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap().get(0).cloned();
        if let Some(contract) = contract {
            if let miner::contract::ContractState::Storing {..} = contract.state {
                break;
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }
 
    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Storing = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    let bill = components.miner("miner").chain_client().get_bill_by_id(bill_id).await.unwrap().unwrap();
    assert_eq!(bill.asset, sector_asset - 1, "asset not match, expected: {}, got: {}", sector_asset - 1, bill.asset);

    let _ = components.user("user").contract_client().challenge(user::contract::OnChainChallengeOptions { 
        order_id, 
        challenge: Some(DmcChallengeParams::SourceStub { 
            piece_index: 0, 
            nonce: "abcd".to_owned(), 
            hash: HashValue::default() 
        }) }).await.unwrap();

    loop {
        let contract = components.user("user").contract_client().get(user::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::contract::ContractState::Canceled = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    loop {
        let contract = components.miner("miner").contract_client().get(miner::contract::ContractFilter::from_order_id(order_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::contract::ContractState::Canceled = contract.state {
            break;
        }
        task::sleep(Duration::from_secs(1)).await;
    }

    // the asset will be updated in a few moments
    task::sleep(Duration::from_secs(2)).await;

    let bill = components.miner("miner").chain_client().get_bill_by_id(bill_id).await.unwrap().unwrap();
    assert_eq!(bill.asset, sector_asset, "asset not match, expected: {}, got: {}", sector_asset, bill.asset);

}
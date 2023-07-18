use log::*;
use std::time::Duration;
use async_std::task;
use crate::prelude::*;

pub async fn create_bill() {
    let config = DmcComponentsConfig { 
        case_name: "create_bill".to_owned(), 
        start_port: 10000, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec![]
    };

    let components = init_components(config).await;

    let sector_path = components.miner("miner").sector_dir().join("1");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;
    info!("will add sector");

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None,
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();
    let id: u64 = sector.sector_id.clone().into();
    info!("add sector id {}", id);
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
    info!("create sector id {}", sector.sector_id);

    let sector_id = sector.sector_id;

    loop {
        task::sleep(Duration::from_secs(10)).await;
        info!("try get sector info...");
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        info!("get sector {} state {:?}", sector_id, sector.info.state);
        if let miner::account::SectorState::PostBill(_) = sector.info.state {
            break;
        }
    }
    
}


pub async fn order_bill() {
    let config = DmcComponentsConfig { 
        case_name: "order_bill".to_owned(), 
        start_port: 10010, 
        use_spv: false, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()]
    };
    let components = init_components(config).await;

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

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: "source url".to_owned(), 
        length: sector_asset * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Ready(DmcMerkleStub { piece_size: 0, leaves: 0, root: HashValue::default() }), 
        stub: user::source::SourceStubOptions { count: 0 }
    }).await.unwrap();

    let source_id = datasource.source_id;

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Bill(DmcOrderOptions { bill_id, asset: sector_asset, duration: 24 })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(_) = sector.info.state {
            break;
        }
    };
    
}

pub async fn order_with_filter() {
    let config = DmcComponentsConfig { 
        case_name: "order_bill".to_owned(), 
        start_port: 10010, 
        use_spv: true, 
        miners: vec!["miner".to_owned()], 
        users: vec!["user".to_owned()]
    };
     
    let components = init_components(config).await;

    let sector_path = components.miner("miner").sector_dir().join("0");
    async_std::fs::create_dir(sector_path.as_path()).await.unwrap();
    
    let sector_asset = 1;

    let sector = components.miner("miner").sector_admin().add(miner::sector::AddSectorOptions {
        node_id: None, 
        local_path: sector_path.as_os_str().to_str().unwrap().to_owned(), 
        capacity: sector_asset * 1024 * 1024 * 1024, 
        chunk_size: 1 * 1024 * 1024
    }).await.unwrap();

    let _ = components.miner("miner").account_client().create_sector(miner::account::CreateSectorOptions {
        raw_sector_id: sector.sector_id.into(), 
        bill: DmcBillOptions {
            asset: sector_asset, 
            price: 1, 
            pledge_rate: Some(3), 
            min_asset: None, 
            duration: 25
        }
    }).await.unwrap();

    let datasource = components.user("user").source_client().create(user::source::CreateSourceOptions {
        source_url: "source url".to_owned(), 
        length: sector_asset * 1024 * 1024 * 1024, 
        merkle: user::source::SourceMerkleOptions::Ready(DmcMerkleStub { piece_size: 0, leaves: 0, root: HashValue::default() }), 
        stub: user::source::SourceStubOptions { count: 0 }
    }).await.unwrap();

    let source_id = datasource.source_id;

    let mut bill_filter = FindBillFilter::default();
    bill_filter.max_price = Some(2);
    bill_filter.min_pledge_rate = Some(2);
    bill_filter.min_asset = Some(sector_asset);

    let sector = components.user("user").account_client().create_sector(user::account::CreateSectorOptions {
        source_id, 
        order: user::account::SectorOrderOptions::Filter(user::account::SectorOrderFilter {
            bill_filter, 
            duration: 25
        })
    }).await.unwrap();

    let sector_id = sector.sector_id;
    loop {
        task::sleep(Duration::from_secs(1)).await;
        let sector = components.user("user").account_client().sectors(user::account::SectorFilter::from_sector_id(sector_id), user::account::SectorNavigator::default()).await.unwrap().next_page().await.unwrap()[0].clone();
        if let user::account::SectorState::PostOrder(_) = sector.info.state {
            break;
        }
    };
    
}
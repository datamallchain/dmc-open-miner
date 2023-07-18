use std::time::Duration;
use async_std::future::pending;
use log::info;
use dmc_tools_common::DmcBillOptions;
use dmc_tools_tests::prelude::*;
use std::io::Write;

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(log::LevelFilter::Info)
        .filter_module("tide", log::LevelFilter::Off)
        .filter_module("sqlx", log::LevelFilter::Off)
        .filter_module("fake", log::LevelFilter::Off)
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} {} - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.level(),
                record.args()
            )
        }).init();

    let config = DmcComponentsConfig {
        case_name: "mserver".to_owned(),
        start_port: 20000,
        use_spv: true,
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
            duration: 100,
        }
    }).await.unwrap();

    let sector_id = sector.sector_id;

    let bill_id = loop {
        async_std::task::sleep(Duration::from_secs(1)).await;
        let sector = components.miner("miner").account_client().sectors(miner::account::SectorFilter::from_sector_id(sector_id), 1).await.unwrap().next_page().await.unwrap()[0].clone();
        if let miner::account::SectorState::PostBill(bill_id) = sector.info.state {
            break bill_id;
        }
    };

    info!("create bill id {} success, use user client to test...", bill_id);

    async_std::task::block_on(pending())
}
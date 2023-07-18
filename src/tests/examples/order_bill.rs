use std::io::Write;
use dmc_tools_common::*;

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(log::LevelFilter::Debug)
        .filter_module("tide", log::LevelFilter::Off)
        .filter_module("sqlx", log::LevelFilter::Off)
        .filter_module("rustls", log::LevelFilter::Off)
        .filter_module("h2", log::LevelFilter::Warn)
        .filter_module("hyper", log::LevelFilter::Warn)
        .filter_module("reqwest", log::LevelFilter::Warn)
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

    // cyfs_debug::PanicBuilder::new("dmc-test", "order_bill").disable_bug_report().exit_on_panic(true).build().start();

    match std::env::args().nth(1).expect("expect a function").as_str() {
        "create_bill" => {
            dmc_tools_tests::order_bill::create_bill(true).await;
        },
        "order_bill" => {
            dmc_tools_tests::order_bill::order_bill(true).await;
        }, 
        "order_with_filter" => {
            dmc_tools_tests::order_bill::order_with_filter(true).await;
        }, 
        _ => unreachable!()
    }
}
use std::io::Write;

// cargo run -p dmc_tools_tests --no-default-features --features sqlite,fake --example apply_contract -- write_restore

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(log::LevelFilter::Info)
        .filter_module("tide", log::LevelFilter::Off)
        .filter_module("sqlx", log::LevelFilter::Off)
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

    match std::env::args().nth(1).expect("expect a function").as_str() {
        "write_restore" => {
            dmc_tools_tests::apply_contract::write_restore().await;
        },
        "write_two" => {
            dmc_tools_tests::apply_contract::write_two().await;
        },
        "offchain_challenge" => {
            dmc_tools_tests::apply_contract::offchain_challenge().await;
        },
        "onchain_challenge" => {
            dmc_tools_tests::apply_contract::onchain_challenge().await;
        },
        "malicious_challenge" => {
            dmc_tools_tests::apply_contract::malicious_challenge().await;
        },
        "cancel_bill" => {
            dmc_tools_tests::apply_contract::cancel_bill().await;
        },
        "rebill_order_asset" => {
            dmc_tools_tests::apply_contract::rebill_order_asset().await;
        },
        _ => unreachable!()
    }
}
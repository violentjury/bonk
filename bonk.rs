use clap::Parser;
use jito_protos::bundle::bundle_result::Result::{
    Accepted, Dropped, Finalized, Processed, Rejected,
};
use jito_protos::bundle::{Bundle, BundleResult};
use jito_protos::packet::{Meta as ProtoMeta, Packet as ProtoPacket};
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::searcher::{
    GetTipAccountsRequest, SendBundleRequest, SubscribeBundleResultsRequest,
};
use jito_searcher_client::get_searcher_client_auth;
use jito_searcher_client::token_authenticator::ClientInterceptor;
use rand::seq::SliceRandom;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::native_token::lamports_to_sol;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use std::collections::HashSet;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Streaming;

#[derive(Parser)]
#[command(name = "jito-searcher")]
struct Args {
    #[arg(long, env = "KEYPAIR_PATH")]
    keypair_path: String,

    #[arg(long, env = "BLOCK_ENGINE_URL")]
    block_engine_url: String,

    #[arg(long, env = "RPC_URL")]
    rpc_url: String,

    #[arg(long, env = "TIP_SIZE_LAMPORTS", default_value = "100000")]
    tip_size_lamports: u64,

    #[arg(long, env = "BUNDLES_TO_SEND", default_value = "10")]
    bundles_to_send: usize,

    #[arg(long, env = "WAIT_SECS", default_value = "25")]
    wait_secs: u64,

    #[arg(long, env = "GAP_MS", default_value = "750")]
    gap_ms: u64,
}

struct Stats {
    sent_count: usize,
    landed_count: usize,
    failed_count: usize,
    sent_ids: HashSet<String>,
}

impl Stats {
    fn new() -> Self {
        Stats {
            sent_count: 0,
            landed_count: 0,
            failed_count: 0,
            sent_ids: HashSet::new(),
        }
    }

    fn on_sent(&mut self, bundle_id: String) {
        self.sent_count += 1;
        self.sent_ids.insert(bundle_id);
    }

    fn record_landed(&mut self) {
        self.landed_count += 1;
    }

    fn record_failed(&mut self) {
        self.failed_count += 1;
    }

    fn land_rate_percent(&self) -> f64 {
        if self.sent_count == 0 {
            0.0
        } else {
            (self.landed_count as f64 / self.sent_count as f64) * 100.0
        }
    }

    fn report(&self) {
        println!(
            "Land Rate: {:.2}% | Sent: {} | Landed: {} | Failed: {}",
            self.land_rate_percent(),
            self.sent_count,
            self.landed_count,
            self.failed_count
        );
    }
}

async fn load_keypair_from_file(path: &str) -> Result<Keypair, Box<dyn std::error::Error>> {
    let keypair = read_keypair_file(path).expect("keypair");
    Ok(keypair)
}

async fn get_tip_accounts(
    mut client: SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
) -> Result<Vec<Pubkey>, Box<dyn std::error::Error>> {
    let request = GetTipAccountsRequest {};
    let response = client
        .get_tip_accounts(request)
        .await
        .expect("failed to get tip accounts");

    let tip_accounts: Vec<Pubkey> = response
        .into_inner()
        .accounts
        .iter()
        .filter_map(|account_str| account_str.parse::<Pubkey>().ok())
        .collect();

    println!("got {} tip accounts", tip_accounts.len());
    Ok(tip_accounts)
}

async fn build_bundle(
    payer: &Keypair,
    tip_account: Pubkey,
    tip_size_lamports: u64,
    rpc_client: &RpcClient,
) -> Result<Vec<Transaction>, Box<dyn std::error::Error>> {
    let recent_blockhash = rpc_client
        .get_latest_blockhash()
        .expect("failed to get latest blockhash");

    let tip_instruction =
        system_instruction::transfer(&payer.pubkey(), &tip_account, tip_size_lamports);
    let tip_transaction = Transaction::new_signed_with_payer(
        &[tip_instruction],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    );

    let dummy_ix = system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1);
    let dummy_tx = Transaction::new_signed_with_payer(
        &[dummy_ix],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    );

    Ok(vec![dummy_tx, tip_transaction])
}

async fn submit_bundle(
    mut client: SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    transactions: Vec<Transaction>,
) -> Result<String, Box<dyn std::error::Error>> {
    let packets: Vec<ProtoPacket> = transactions
        .iter()
        .map(|tx| {
            let data = bincode::serialize(tx).expect("failed to serialize transaction");
            let size = data.len() as u64;
            ProtoPacket {
                data,
                meta: Some(ProtoMeta {
                    size,
                    ..Default::default()
                }),
            }
        })
        .collect();

    let request = SendBundleRequest {
        bundle: Some(Bundle {
            header: None,
            packets,
        }),
    };

    let response = client
        .send_bundle(request)
        .await
        .expect("failed to send bundle");

    let bundle_id = response.into_inner().uuid;

    Ok(bundle_id)
}

async fn subscribe_to_bundle_results(
    mut client: SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
) -> Result<Streaming<BundleResult>, Box<dyn std::error::Error>> {
    let request = SubscribeBundleResultsRequest {};

    let stream = client
        .subscribe_bundle_results(request)
        .await
        .expect("failed to subscribe to bundle results")
        .into_inner();

    Ok(stream)
}

async fn drain_bundle_results(
    stream: &mut Streaming<BundleResult>,
    tracker: Arc<Mutex<Stats>>,
    wait_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // stops once nothing new shows up for wait_secs
    while let Ok(Ok(Some(next))) =
        tokio::time::timeout(Duration::from_secs(wait_secs), stream.message()).await
    {
        if !tracker.lock().unwrap().sent_ids.contains(&next.bundle_id) {
            continue;
        }

        if let Some(result_value) = next.result {
            match result_value {
                Accepted(_) => {
                    println!("bundle accepted");
                }
                Rejected(_) => {
                    let mut t = tracker.lock().unwrap();
                    t.record_failed();
                    println!(
                        "bundle rejected - land rate now: {:.2}%",
                        t.land_rate_percent()
                    );
                }
                Finalized(_) => {
                    println!("bundle finalized");
                }
                Processed(_) => {
                    let mut t = tracker.lock().unwrap();
                    t.record_landed();
                    println!(
                        "bundle landed - land rate now: {:.2}%",
                        t.land_rate_percent()
                    );
                }
                Dropped(_) => {
                    let mut t = tracker.lock().unwrap();
                    t.record_failed();
                    println!(
                        "bundle dropped - land rate now: {:.2}%",
                        t.land_rate_percent()
                    );
                }
            }
        }
    }

    Ok(())
}

async fn run_searcher(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let payer = Arc::new(load_keypair_from_file(&args.keypair_path).await?);
    let rpc_client = RpcClient::new_with_commitment(args.rpc_url, CommitmentConfig::confirmed());

    let mut searcher_client = get_searcher_client_auth(&args.block_engine_url, &payer)
        .await
        .expect("failed to connect to block engine");

    let tip_accounts = get_tip_accounts(searcher_client.clone()).await?;

    let tracker = Arc::new(Mutex::new(Stats::new()));

    let mut result_stream = subscribe_to_bundle_results(searcher_client.clone()).await?;

    sleep(Duration::from_millis(500)).await;

    for i in 0..args.bundles_to_send {
        let tip_account = *tip_accounts.choose(&mut rand::thread_rng()).unwrap();
        let bundle = build_bundle(&payer, tip_account, args.tip_size_lamports, &rpc_client).await?;

        let bundle_id = submit_bundle(searcher_client.clone(), bundle).await?;

        let mut t = tracker.lock().unwrap();
        t.on_sent(bundle_id.clone());
        println!("sent #{} -> {}", i + 1, bundle_id);
        t.report();
        drop(t);

        sleep(Duration::from_millis(args.gap_ms)).await;
    }

    drain_bundle_results(&mut result_stream, tracker.clone(), args.wait_secs).await?;

    let final_tracker = tracker.lock().unwrap();
    println!("final stats:");
    final_tracker.report();

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    if let Err(e) = run_searcher(args).await {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

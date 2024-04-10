#![allow(clippy::arithmetic_side_effects)]
use {
    log::*,
    solana_bench_tps::{
        bench::{do_bench_tps, max_lamports_for_prioritization},
        bench_tps_client::{
            high_tps_client::{HighTpsClient, HighTpsClientConfig},
            BenchTpsClient,
        },
        cli::{self, ExternalClientType},
        keypairs::get_keypairs,
        send_batch::{generate_durable_nonce_accounts, generate_keypairs},
    },
    solana_client::{connection_cache::ConnectionCache, thin_client::ThinClient},
    solana_genesis::Base64Account,
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        fee_calculator::FeeRateGovernor,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        system_program,
    },
    solana_streamer::streamer::StakedNodes,
    solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig},
    std::{
        collections::HashMap,
        fs::File,
        io::prelude::*,
        net::IpAddr,
        net::{SocketAddr, ToSocketAddrs},
        path::Path,
        process::exit,
        sync::{Arc, RwLock},
    },
};

/// Number of signatures for all transactions in ~1 week at ~100K TPS
pub const NUM_SIGNATURES_FOR_TXS: u64 = 100_000 * 60 * 60 * 24 * 7;

/// Request information about node's stake
/// If fail to get requested information, return error
/// Otherwise return stake of the node
/// along with total activated stake of the network
fn find_node_activated_stake(
    rpc_client: Arc<RpcClient>,
    node_id: Pubkey,
) -> Result<(u64, u64), ()> {
    let vote_accounts = rpc_client.get_vote_accounts();
    if let Err(error) = vote_accounts {
        error!("Failed to get vote accounts, error: {}", error);
        return Err(());
    }

    let vote_accounts = vote_accounts.unwrap();

    let total_active_stake: u64 = vote_accounts
        .current
        .iter()
        .map(|vote_account| vote_account.activated_stake)
        .sum();

    let node_id_as_str = node_id.to_string();
    let find_result = vote_accounts
        .current
        .iter()
        .find(|&vote_account| vote_account.node_pubkey == node_id_as_str);
    match find_result {
        Some(value) => Ok((value.activated_stake, total_active_stake)),
        None => {
            error!("Failed to find stake for requested node");
            Err(())
        }
    }
}

fn create_connection_cache(
    json_rpc_url: &str,
    tpu_connection_pool_size: usize,
    use_quic: bool,
    bind_address: IpAddr,
    client_node_ids: Option<&Vec<Keypair>>,
    commitment_config: CommitmentConfig,
) -> Vec<ConnectionCache> {
    if !use_quic {
        return vec![ConnectionCache::with_udp(
            "bench-tps-connection_cache_udp",
            tpu_connection_pool_size,
        )];
    }
    if client_node_ids.is_none() {
        return vec![ConnectionCache::new_quic(
            "bench-tps-connection_cache_quic",
            tpu_connection_pool_size,
        )];
    }

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        commitment_config,
    ));

    //TODO(klykov): can we use one HashMap for all the connections?
    let client_node_ids = client_node_ids.unwrap();
    let mut connection_caches = Vec::with_capacity(client_node_ids.len());
    for client_node_id in client_node_ids {
        let (stake, total_stake) =
            find_node_activated_stake(rpc_client.clone(), client_node_id.pubkey())
                .unwrap_or_default();
        info!("Stake for specified client_node_id: {stake}, total stake: {total_stake}");
        let stakes = HashMap::from([
            (client_node_id.pubkey(), stake),
            (Pubkey::new_unique(), total_stake - stake),
        ]);
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::new(
            Arc::new(stakes),
            HashMap::<Pubkey, u64>::default(), // overrides
        )));
        let cc = ConnectionCache::new_with_client_options(
            "bench-tps-connection_cache_quic",
            tpu_connection_pool_size,
            None,
            Some((client_node_id, bind_address)),
            Some((&staked_nodes, &client_node_id.pubkey())),
        );
        connection_caches.push(cc);
    }
    connection_caches
}

#[allow(clippy::too_many_arguments)]
fn create_client(
    external_client_type: &ExternalClientType,
    json_rpc_url: &str,
    websocket_url: &str,
    connection_caches: Vec<ConnectionCache>,
    commitment_config: CommitmentConfig,
    pinned_tpu_address: Option<&str>,
) -> Arc<dyn BenchTpsClient + Send + Sync> {
    match external_client_type {
        ExternalClientType::RpcClient => Arc::new(RpcClient::new_with_commitment(
            json_rpc_url.to_string(),
            commitment_config,
        )),
        ExternalClientType::ThinClient => {
            let Some(pinned_tpu_address) = pinned_tpu_address else {
                panic!("ThinClient requires pinned address");
            };
            let rpc: SocketAddr = (pinned_tpu_address.split(':').collect::<Vec<_>>()[0].to_owned()
                + ":8899")
                .parse()
                .unwrap();
            let tpu: SocketAddr = pinned_tpu_address
                .to_socket_addrs()
                .ok()
                .unwrap()
                .next()
                .unwrap();
            info!("RPC: {:?}, TPU: {:?}", rpc, tpu);
            let connection_cache = Arc::new(connection_caches[0].clone());
            Arc::new(ThinClient::new(rpc, tpu, connection_cache))
        }
        ExternalClientType::TpuClient => {
            let rpc_client = Arc::new(RpcClient::new_with_commitment(
                json_rpc_url.to_string(),
                commitment_config,
            ));
            match &connection_caches[0] {
                ConnectionCache::Udp(cache) => Arc::new(
                    TpuClient::new_with_connection_cache(
                        rpc_client,
                        websocket_url,
                        TpuClientConfig::default(),
                        cache.clone(),
                    )
                    .unwrap_or_else(|err| {
                        eprintln!("Could not create TpuClient {err:?}");
                        exit(1);
                    }),
                ),
                ConnectionCache::Quic(cache) => Arc::new(
                    TpuClient::new_with_connection_cache(
                        rpc_client,
                        websocket_url,
                        TpuClientConfig::default(),
                        cache.clone(),
                    )
                    .unwrap_or_else(|err| {
                        eprintln!("Could not create TpuClient {err:?}");
                        exit(1);
                    }),
                ),
            }
        }
        ExternalClientType::HighTpsClient => {
            let rpc_client = Arc::new(RpcClient::new_with_commitment(
                json_rpc_url.to_string(),
                commitment_config,
            ));
            let caches = connection_caches
                .iter()
                .map(|cache| match cache {
                    ConnectionCache::Udp(_) => {
                        unimplemented!("UDP protocol is not supported.");
                    }
                    ConnectionCache::Quic(cache) => cache.clone(),
                })
                .collect();
            Arc::new(
                HighTpsClient::new(
                    rpc_client,
                    websocket_url,
                    HighTpsClientConfig {
                        fanout_slots: 1,
                        send_batch_size: 64,
                        pinned_tpu_address: pinned_tpu_address
                            .and_then(|s| s.to_socket_addrs().ok()?.next()),
                    },
                    caches,
                )
                .unwrap_or_else(|err| {
                    eprintln!("Could not create HighTpsClient {err:?}");
                    exit(1);
                }),
            )
        }
    }
}

fn main() {
    solana_logger::setup_with_default_filter();
    solana_metrics::set_panic_hook("bench-tps", /*version:*/ None);

    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = match cli::parse_args(&matches) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            exit(1);
        }
    };

    let cli::Config {
        json_rpc_url,
        websocket_url,
        id,
        tx_count,
        keypair_multiplier,
        client_ids_and_stake_file,
        write_to_client_file,
        read_from_client_file,
        target_lamports_per_signature,
        num_lamports_per_account,
        external_client_type,
        use_quic,
        tpu_connection_pool_size,
        skip_tx_account_data_size,
        compute_unit_price,
        use_durable_nonce,
        instruction_padding_config,
        bind_address,
        client_node_ids,
        commitment_config,
        pinned_tpu_address,
        ..
    } = &cli_config;

    let keypair_count = *tx_count * keypair_multiplier;
    if *write_to_client_file {
        info!("Generating {} keypairs", keypair_count);
        let (keypairs, _) = generate_keypairs(id, keypair_count as u64);
        let num_accounts = keypairs.len() as u64;
        let max_fee = FeeRateGovernor::new(*target_lamports_per_signature, 0)
            .max_lamports_per_signature
            .saturating_add(max_lamports_for_prioritization(compute_unit_price));
        let num_lamports_per_account = (num_accounts - 1 + NUM_SIGNATURES_FOR_TXS * max_fee)
            / num_accounts
            + num_lamports_per_account;
        let mut accounts = HashMap::new();
        keypairs.iter().for_each(|keypair| {
            accounts.insert(
                serde_json::to_string(&keypair.to_bytes().to_vec()).unwrap(),
                Base64Account {
                    balance: num_lamports_per_account,
                    executable: false,
                    owner: system_program::id().to_string(),
                    data: String::new(),
                },
            );
        });

        info!("Writing {}", client_ids_and_stake_file);
        let serialized = serde_yaml::to_string(&accounts).unwrap();
        let path = Path::new(&client_ids_and_stake_file);
        let mut file = File::create(path).unwrap();
        file.write_all(b"---\n").unwrap();
        file.write_all(&serialized.into_bytes()).unwrap();
        return;
    }

    let connection_caches = create_connection_cache(
        json_rpc_url,
        *tpu_connection_pool_size,
        *use_quic,
        *bind_address,
        client_node_ids.as_ref(),
        *commitment_config,
    );
    let client = create_client(
        external_client_type,
        json_rpc_url,
        websocket_url,
        connection_caches,
        *commitment_config,
        pinned_tpu_address.as_deref(),
    );
    if let Some(instruction_padding_config) = instruction_padding_config {
        info!(
            "Checking for existence of instruction padding program: {}",
            instruction_padding_config.program_id
        );
        client
            .get_account(&instruction_padding_config.program_id)
            .expect("Instruction padding program must be deployed to this cluster. Deploy the program using `solana program deploy ./bench-tps/tests/fixtures/spl_instruction_padding.so` and pass the resulting program id with `--instruction-padding-program-id`");
    }
    let keypairs = get_keypairs(
        client.clone(),
        id,
        keypair_count,
        *num_lamports_per_account,
        client_ids_and_stake_file,
        *read_from_client_file,
        *skip_tx_account_data_size,
        instruction_padding_config.is_some(),
    );

    let nonce_keypairs = if *use_durable_nonce {
        Some(generate_durable_nonce_accounts(client.clone(), &keypairs))
    } else {
        None
    };
    do_bench_tps(client, cli_config, keypairs, nonce_keypairs);
}

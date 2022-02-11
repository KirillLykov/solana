#![allow(clippy::integer_arithmetic)]
use {
    clap::{crate_description, crate_name, value_t, value_t_or_exit, App, Arg},
    log::*,
    rand::{thread_rng, Rng},
    solana_client::rpc_client::RpcClient,
    solana_core::serve_repair::RepairProtocol,
    solana_gossip::{contact_info::ContactInfo, gossip_service::discover},
    solana_sdk::pubkey::Pubkey,
    solana_sdk::timing::timestamp,
    // TODO(klykov): maybe later will move to test_tx.rs
    solana_sdk::{
        hash::Hash, instruction::CompiledInstruction, signature::Keypair, stake,
        system_instruction::SystemInstruction, system_program, transaction::Transaction,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        net::{SocketAddr, UdpSocket},
        process::exit,
        str::FromStr,
        time::{Duration, Instant},
    },
};

fn get_repair_contact(nodes: &[ContactInfo]) -> ContactInfo {
    let source = thread_rng().gen_range(0, nodes.len());
    let mut contact = nodes[source].clone();
    contact.id = solana_sdk::pubkey::new_rand();
    contact
}

fn test_multisig_tx(
    transaction_params: &TransactionParams,
    rpc_client: &Option<RpcClient>,
    blockhash_time: &mut (Hash, Instant), // blockhash together with the time of generation
) -> Transaction {
    // generate a new blockhash every 0.5sec
    if blockhash_time.1.elapsed().as_millis() > 500 {
        *blockhash_time = if transaction_params.valid_block_hash {
            (
                rpc_client.as_ref().unwrap().get_latest_blockhash().unwrap(),
                Instant::now(),
            )
        } else {
            (Hash::default(), Instant::now())
        };
    }

    let lamports = 5;
    let transfer_instruction = SystemInstruction::Transfer { lamports };

    let program_ids = vec![system_program::id(), stake::program::id()];

    let instructions = vec![CompiledInstruction::new(
        0,
        &transfer_instruction,
        vec![0, 1],
    )];

    if transaction_params.valid_signatures {
        let kpvals: Vec<Keypair> = (0..transaction_params.num_sign)
            .map(|_| Keypair::new())
            .collect();
        let keypairs: Vec<&Keypair> = kpvals.iter().collect();

        Transaction::new_with_compiled_instructions(
            &keypairs,
            &[],
            blockhash_time.0,
            program_ids,
            instructions,
        )
    } else {
        let mut tx = Transaction::new_with_compiled_instructions(
            &[] as &[&Keypair; 0],
            &[],
            blockhash_time.0,
            program_ids,
            instructions,
        );
        tx.signatures = vec![Transaction::get_invalid_signature(); transaction_params.num_sign];
        tx
    }
}

/// Options for data_type=transaction
struct TransactionParams {
    unique_transactons: bool, // use unique transactions
    num_sign: usize,          // number of signatures in a transaction
    valid_block_hash: bool,   // use valid blockhash or random
    valid_signatures: bool,   // use valid signatures or not
}

fn run_dos(
    nodes: &[ContactInfo],
    iterations: usize,
    entrypoint_addr: SocketAddr,
    data_type: String,
    data_size: usize,
    mode: String,
    data_input: Option<String>,
    transaction_params: Option<TransactionParams>,
) {
    let mut target = None;
    let mut rpc_client = None;
    if nodes.is_empty() {
        if mode == "rpc" {
            rpc_client = Some(RpcClient::new_socket(entrypoint_addr));
        }
        target = Some(entrypoint_addr);
    } else {
        for node in nodes {
            if node.gossip == entrypoint_addr {
                target = match mode.as_str() {
                    "gossip" => Some(node.gossip),
                    "tvu" => Some(node.tvu),
                    "tvu_forwards" => Some(node.tvu_forwards),
                    "tpu" => {
                        rpc_client = Some(RpcClient::new_socket(node.rpc));
                        Some(node.tpu)
                    }
                    "tpu_forwards" => Some(node.tpu_forwards),
                    "repair" => Some(node.repair),
                    "serve_repair" => Some(node.serve_repair),
                    "rpc" => {
                        rpc_client = Some(RpcClient::new_socket(node.rpc));
                        None
                    }
                    &_ => panic!("Unknown mode"),
                };
                break;
            }
        }
    }
    let target = target.expect("should have target");

    info!("Targetting {}", target);
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let mut data = Vec::new();
    let mut blockhash_time = (Hash::default(), Instant::now() - Duration::from_secs(100));

    match data_type.as_str() {
        "repair_highest" => {
            let slot = 100;
            let req = RepairProtocol::WindowIndexWithNonce(get_repair_contact(nodes), slot, 0, 0);
            data = bincode::serialize(&req).unwrap();
        }
        "repair_shred" => {
            let slot = 100;
            let req =
                RepairProtocol::HighestWindowIndexWithNonce(get_repair_contact(nodes), slot, 0, 0);
            data = bincode::serialize(&req).unwrap();
        }
        "repair_orphan" => {
            let slot = 100;
            let req = RepairProtocol::OrphanWithNonce(get_repair_contact(nodes), slot, 0);
            data = bincode::serialize(&req).unwrap();
        }
        "random" => {
            data.resize(data_size, 0);
        }
        "transaction" => {
            if transaction_params.is_none() {
                panic!("transaction parameters are not specified");
            }
            let tp = transaction_params.as_ref().unwrap();
            if tp.unique_transactons {
                let tx = test_multisig_tx(tp, &rpc_client, &mut blockhash_time);
                info!("{:?}", tx);
                data = bincode::serialize(&tx).unwrap();
            }
        }
        "get_account_info" => {}
        "get_program_accounts" => {}
        &_ => {
            panic!("unknown data type");
        }
    }

    let mut last_log = Instant::now();
    let mut count = 0;
    let mut error_count = 0;
    loop {
        if mode == "rpc" {
            match data_type.as_str() {
                "get_account_info" => {
                    let res = rpc_client
                        .as_ref()
                        .unwrap()
                        .get_account(&Pubkey::from_str(data_input.as_ref().unwrap()).unwrap());
                    if res.is_err() {
                        error_count += 1;
                    }
                }
                "get_program_accounts" => {
                    let res = rpc_client.as_ref().unwrap().get_program_accounts(
                        &Pubkey::from_str(data_input.as_ref().unwrap()).unwrap(),
                    );
                    if res.is_err() {
                        error_count += 1;
                    }
                }
                &_ => {
                    panic!("unsupported data type");
                }
            }
        } else {
            if data_type == "random" {
                thread_rng().fill(&mut data[..]);
            }
            if let Some(tp) = transaction_params.as_ref() {
                if tp.unique_transactons {
                    let tx = test_multisig_tx(tp, &rpc_client, &mut blockhash_time);
                    info!("{:?}", tx);
                    data = bincode::serialize(&tx).unwrap();
                }
            }
            let res = socket.send_to(&data, target);
            if res.is_err() {
                error_count += 1;
            }
        }
        count += 1;
        if last_log.elapsed().as_millis() > 10_000 {
            info!("count: {} errors: {}", count, error_count);
            last_log = Instant::now();
            count = 0;
        }
        if iterations != 0 && count >= iterations {
            break;
        }
    }
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("entrypoint")
                .long("entrypoint")
                .takes_value(true)
                .value_name("HOST:PORT")
                .help("Gossip entrypoint address. Usually <ip>:8001"),
        )
        .arg(
            Arg::with_name("mode")
                .long("mode")
                .takes_value(true)
                .value_name("MODE")
                .possible_values(&[
                    "gossip",
                    "tvu",
                    "tvu_forwards",
                    "tpu",
                    "tpu_forwards",
                    "repair",
                    "serve_repair",
                    "rpc",
                ])
                .help("Interface to DoS"),
        )
        .arg(
            Arg::with_name("data_size")
                .long("data-size")
                .takes_value(true)
                .value_name("BYTES")
                .help("Size of packet to DoS with"),
        )
        .arg(
            Arg::with_name("data_type")
                .long("data-type")
                .takes_value(true)
                .value_name("TYPE")
                .possible_values(&[
                    "repair_highest",
                    "repair_shred",
                    "repair_orphan",
                    "random",
                    "get_account_info",
                    "get_program_accounts",
                    "transaction",
                ])
                .help("Type of data to send"),
        )
        .arg(
            Arg::with_name("data_input")
                .long("data-input")
                .takes_value(true)
                .value_name("TYPE")
                .help("Data to send"),
        )
        .arg(
            Arg::with_name("skip_gossip")
                .long("skip-gossip")
                .help("Just use entrypoint address directly"),
        )
        .arg(
            Arg::with_name("allow_private_addr")
                .long("allow-private-addr")
                .takes_value(false)
                .help("Allow contacting private ip addresses")
                .hidden(true),
        )
        .arg(
            Arg::with_name("num_sign")
                .long("number-of-signatures")
                .takes_value(true)
                .value_name("NSIGN")
                .help("Number of signatures in transaction"),
        )
        .arg(
            Arg::with_name("valid_blockhash")
                .long("generate-valid-blockhash")
                .takes_value(false)
                .help("Generate a valid blockhash for transaction")
                .hidden(true),
        )
        .arg(
            Arg::with_name("valid_sign")
                .long("generate-valid-signatures")
                .takes_value(false)
                .help("Generate valid signature(s) for transaction")
                .hidden(true),
        )
        .arg(
            Arg::with_name("unique_trans")
                .long("generate-unique-transactions")
                .takes_value(false)
                .help("Generate unique transaction")
                .hidden(true),
        )
        .get_matches();

    let mut entrypoint_addr = SocketAddr::from(([127, 0, 0, 1], 8001));
    if let Some(addr) = matches.value_of("entrypoint") {
        entrypoint_addr = solana_net_utils::parse_host_port(addr).unwrap_or_else(|e| {
            eprintln!("failed to parse entrypoint address: {}", e);
            exit(1)
        });
    }
    let data_size = value_t!(matches, "data_size", usize).unwrap_or(128);
    let skip_gossip = matches.is_present("skip_gossip");

    let mode = value_t_or_exit!(matches, "mode", String);
    let data_type = value_t_or_exit!(matches, "data_type", String);
    let data_input = value_t!(matches, "data_input", String).ok();

    let transaction_params = match data_type.as_str() {
        "transaction" => Some(TransactionParams {
            unique_transactons: matches.is_present("unique_trans"),
            num_sign: value_t!(matches, "num_sign", usize).unwrap_or(2),
            valid_block_hash: matches.is_present("valid_blockhash"),
            valid_signatures: matches.is_present("valid_sign"),
        }),
        _ => None,
    };

    let mut nodes = vec![];
    if !skip_gossip {
        info!("Finding cluster entry: {:?}", entrypoint_addr);
        let socket_addr_space = SocketAddrSpace::new(matches.is_present("allow_private_addr"));
        let (gossip_nodes, _validators) = discover(
            None, // keypair
            Some(&entrypoint_addr),
            None,                    // num_nodes
            Duration::from_secs(60), // timeout
            None,                    // find_node_by_pubkey
            Some(&entrypoint_addr),  // find_node_by_gossip_addr
            None,                    // my_gossip_addr
            0,                       // my_shred_version
            socket_addr_space,
        )
        .unwrap_or_else(|err| {
            eprintln!("Failed to discover {} node: {:?}", entrypoint_addr, err);
            exit(1);
        });
        nodes = gossip_nodes;
    }

    info!("done found {} nodes", nodes.len());

    run_dos(
        &nodes,
        0,
        entrypoint_addr,
        data_type,
        data_size,
        mode,
        data_input,
        transaction_params,
    );
}

#[cfg(test)]
pub mod test {
    use solana_local_cluster::{cluster::Cluster, local_cluster::LocalCluster};
    use {super::*, solana_sdk::timing::timestamp};

    #[test]
    fn test_dos() {
        let nodes = [ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            timestamp(),
        )];
        let entrypoint_addr = nodes[0].gossip;
        run_dos(
            &nodes,
            1,
            entrypoint_addr,
            "random".to_string(),
            10,
            "tvu".to_string(),
            None,
            None,
        );

        run_dos(
            &nodes,
            1,
            entrypoint_addr,
            "repair_highest".to_string(),
            10,
            "repair".to_string(),
            None,
            None,
        );

        run_dos(
            &nodes,
            1,
            entrypoint_addr,
            "repair_shred".to_string(),
            10,
            "serve_repair".to_string(),
            None,
            None,
        );
    }

    #[test]
    #[ignore]
    fn test_dos_local_cluster() {
        solana_logger::setup();
        let num_nodes = 1;
        let cluster =
            LocalCluster::new_with_equal_stakes(num_nodes, 100, 3, SocketAddrSpace::Unspecified);
        assert_eq!(cluster.validators.len(), num_nodes);

        let nodes = cluster.get_node_pubkeys();
        let node = cluster.get_contact_info(&nodes[0]).unwrap().clone();

        let tp = Some(TransactionParams {
            unique_transactons: false,
            num_sign: 2,
            valid_block_hash: false, // use valid blockhash or random
            valid_signatures: false, // use valid signatures or not
        });

        run_dos(
            &[node],
            1_000_000, // was 10_000_000
            cluster.entry_point_info.gossip,
            "transaction".to_string(),
            1000,
            "tpu".to_string(),
            None,
            tp,
        );
    }
}

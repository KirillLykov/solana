use {
    crate::bench_tps_client::{BenchTpsClient, BenchTpsError, Result},
    solana_connection_cache::connection_cache::{
        ConnectionManager, ConnectionPool, NewConnectionConfig,
    },
    solana_rpc_client_api::config::RpcBlockConfig,
    solana_sdk::{
        account::Account, commitment_config::CommitmentConfig, epoch_info::EpochInfo, hash::Hash,
        message::Message, pubkey::Pubkey, signature::Signature, slot_history::Slot,
        transaction::Transaction,
    },
    solana_tpu_client::tpu_client::TpuClient,
    solana_transaction_status::UiConfirmedBlock,
};

impl<P, M, C> BenchTpsClient for TpuClient<P, M, C>
where
    P: ConnectionPool<NewConnectionConfig = C>,
    M: ConnectionManager<ConnectionPool = P, NewConnectionConfig = C>,
    C: NewConnectionConfig,
{
    fn send_transaction(&self, transaction: Transaction) -> Result<Signature> {
        let signature = transaction.signatures[0];
        self.try_send_transaction(&transaction)?;
        Ok(signature)
    }
    fn send_batch(&self, transactions: Vec<Transaction>) -> Result<()> {
        self.try_send_transaction_batch(&transactions)?;
        /*let cc = self.connection_cache;
        let addresses = self
            .leader_tpu_service
            .leader_tpu_sockets(self.fanout_slots);
        */
        Ok(())
    }
    fn get_latest_blockhash(&self) -> Result<Hash> {
        self.rpc_client()
            .get_latest_blockhash()
            .map_err(|err| err.into())
    }

    fn get_latest_blockhash_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> Result<(Hash, u64)> {
        self.rpc_client()
            .get_latest_blockhash_with_commitment(commitment_config)
            .map_err(|err| err.into())
    }

    fn get_transaction_count(&self) -> Result<u64> {
        self.rpc_client()
            .get_transaction_count()
            .map_err(|err| err.into())
    }

    fn get_transaction_count_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> Result<u64> {
        self.rpc_client()
            .get_transaction_count_with_commitment(commitment_config)
            .map_err(|err| err.into())
    }

    fn get_epoch_info(&self) -> Result<EpochInfo> {
        self.rpc_client().get_epoch_info().map_err(|err| err.into())
    }

    fn get_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        self.rpc_client()
            .get_balance(pubkey)
            .map_err(|err| err.into())
    }

    fn get_balance_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> Result<u64> {
        self.rpc_client()
            .get_balance_with_commitment(pubkey, commitment_config)
            .map(|res| res.value)
            .map_err(|err| err.into())
    }

    fn get_fee_for_message(&self, message: &Message) -> Result<u64> {
        self.rpc_client()
            .get_fee_for_message(message)
            .map_err(|err| err.into())
    }

    fn get_minimum_balance_for_rent_exemption(&self, data_len: usize) -> Result<u64> {
        self.rpc_client()
            .get_minimum_balance_for_rent_exemption(data_len)
            .map_err(|err| err.into())
    }

    fn addr(&self) -> String {
        self.rpc_client().url()
    }

    fn request_airdrop_with_blockhash(
        &self,
        pubkey: &Pubkey,
        lamports: u64,
        recent_blockhash: &Hash,
    ) -> Result<Signature> {
        self.rpc_client()
            .request_airdrop_with_blockhash(pubkey, lamports, recent_blockhash)
            .map_err(|err| err.into())
    }

    fn get_account(&self, pubkey: &Pubkey) -> Result<Account> {
        self.rpc_client()
            .get_account(pubkey)
            .map_err(|err| err.into())
    }

    fn get_account_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> Result<Account> {
        self.rpc_client()
            .get_account_with_commitment(pubkey, commitment_config)
            .map(|res| res.value)
            .map_err(|err| err.into())
            .and_then(|account| {
                account.ok_or_else(|| {
                    BenchTpsError::Custom(format!("AccountNotFound: pubkey={pubkey}"))
                })
            })
    }

    fn get_multiple_accounts(&self, pubkeys: &[Pubkey]) -> Result<Vec<Option<Account>>> {
        self.rpc_client()
            .get_multiple_accounts(pubkeys)
            .map_err(|err| err.into())
    }

    fn get_slot_with_commitment(&self, commitment_config: CommitmentConfig) -> Result<Slot> {
        self.rpc_client()
            .get_slot_with_commitment(commitment_config)
            .map_err(|err| err.into())
    }

    fn get_blocks_with_commitment(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment_config: CommitmentConfig,
    ) -> Result<Vec<Slot>> {
        self.rpc_client()
            .get_blocks_with_commitment(start_slot, end_slot, commitment_config)
            .map_err(|err| err.into())
    }

    fn get_block_with_config(
        &self,
        slot: Slot,
        rpc_block_config: RpcBlockConfig,
    ) -> Result<UiConfirmedBlock> {
        self.rpc_client()
            .get_block_with_config(slot, rpc_block_config)
            .map_err(|err| err.into())
    }
}

/*
mod send_batch_impl {
    use {
        bincode::serialize,
        solana_client::connection_cache::ConnectionCache,
        solana_sdk::transaction::Transaction,
        solana_tpu_client::tpu_client::TpuClient,
        std::{net::SocketAddr, sync::Arc},
    };

    #[derive(Clone, Debug)]
    pub struct Config {
        pub retry_rate_ms: u64,
        pub leader_forward_count: u64,
        pub default_max_retries: Option<usize>,
        pub service_max_retries: usize,
        /// The batch size for sending transactions in batches
        pub batch_size: usize,
        /// How frequently batches are sent
        pub batch_send_rate_ms: u64,
        /// When the retry pool exceeds this max size, new transactions are dropped after their first broadcast attempt
        pub retry_pool_max_size: usize,
        pub tpu_peers: Option<Vec<SocketAddr>>,
    }

    /// Process transactions in batch.
    fn send_transactions_in_batch(
        //<T: TpuInfo>(
        //client: &TpuClient,
        addresses: Vec<&SocketAddr>,
        //tpu_address: &SocketAddr,
        transactions: Vec<Transaction>,
        //leader_info: Option<&T>,
        connection_cache: &Arc<ConnectionCache>,
        config: &Config,
    ) {
        // Processing the transactions in batch
        // send_transaction_service uses predefined peers
        //let mut addresses = config
        //    .tpu_peers
        //    .as_ref()
        //    .map(|addrs| addrs.iter().map(|a| (a, 0)).collect::<Vec<_>>())
        //    .unwrap_or_default();
        //let leader_addresses = get_tpu_addresses_with_slots(
        //    tpu_address,
        //    leader_info,
        //    config,
        //    connection_cache.protocol(),
        //);
        //addresses.extend(leader_addresses);
        // how we can get it from client
        //let addresses = client
        //    .leader_tpu_service
        //    .leader_tpu_sockets(client.fanout_slots);

        let wire_transactions = transactions
            .into_par_iter()
            .map(|tx| bincode::serialize(&tx).expect("serialize Transaction in send_batch"))
            .collect::<Vec<_>>();

        for address in &addresses {
            send_transactions_with_metrics(address, &wire_transactions, connection_cache);
        }
    }

    // in TpuClient, there is a connection cache which is in tpu-client/src/nonblocking/tpu_client.rs::get_leader_sockets
    // I'm not sure it is very performant.
    // TODO(klykov): check how performant is this cache (compare with the function below).
    fn get_tpu_addresses_with_slots<'a, T: TpuInfo>(
        tpu_address: &'a SocketAddr,
        leader_info: Option<&'a T>,
        config: &'a Config,
        protocol: Protocol,
    ) -> Vec<(&'a SocketAddr, Slot)> {
        leader_info
            .as_ref()
            .map(|leader_info| {
                leader_info.get_leader_tpus_with_slots(config.leader_forward_count, protocol)
            })
            .filter(|addresses| !addresses.is_empty())
            .unwrap_or_else(|| vec![(tpu_address, 0)])
    }

    //fn send_transactions_with_metrics(
    //    tpu_address: &SocketAddr,
    //    wire_transactions: &[&[u8]],
    //    connection_cache: &Arc<ConnectionCache>,
    //) -> Result<(), TransportError> {
    //    let wire_transactions = wire_transactions.iter().map(|t| t.to_vec()).collect();
    //    let conn = connection_cache.get_connection(tpu_address);
    //    conn.send_data_batch_async(wire_transactions)
    //}
}
*/

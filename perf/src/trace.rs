use agave_perf_trace::{
    TransactionEvent, TransactionState, TxProducer, timestamp, write_or_record_drop,
};

pub(crate) fn trace_deduped_transaction(
    tx_trace: Option<&TxProducer>,
    flow_id: u64,
    sig: [u8; 64],
) {
    if let Some(tx_trace) = tx_trace {
        write_or_record_drop(
            tx_trace,
            TransactionEvent {
                flow_id,
                sig,
                ts: timestamp(),
                state: TransactionState::Deduped,
            },
        );
    }
}

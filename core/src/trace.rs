use {
    agave_perf_trace::{
        Event, EventsProducer, RepairStatsEvent, ReplaySlotCompleteEvent, SchedulingDetailsEvent,
        TransactionEvent, TransactionState, TxProducer, thread_id_u32, timestamp, u64_from_usize,
        write_or_record_drop,
    },
    solana_clock::Slot,
    std::time::Instant,
};

pub(crate) fn trace_transaction_state(
    tx_trace: Option<&TxProducer>,
    flow_id: u64,
    sig: [u8; 64],
    ts: u64,
    state: TransactionState,
) {
    if let Some(tx_trace) = tx_trace {
        write_or_record_drop(
            tx_trace,
            TransactionEvent {
                flow_id,
                sig,
                ts,
                state,
            },
        );
    }
}

pub(crate) fn trace_replay_slot_complete(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    started: Instant,
    num_shreds: u64,
    num_entries: usize,
    num_txs: usize,
) {
    if let Some(events_trace) = events_trace {
        let end_ts = timestamp();
        let start_ts =
            end_ts.saturating_sub(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        write_or_record_drop(
            events_trace,
            Event::replay_slot_complete(ReplaySlotCompleteEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                slot,
                start_ts,
                end_ts,
                num_shreds,
                num_entries: u64_from_usize(num_entries),
                num_txs: u64_from_usize(num_txs),
            }),
        );
    }
}

pub(crate) fn trace_repair_stats(events_trace: Option<&EventsProducer>, num_repairs: usize) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::repair_stats(RepairStatsEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                num_repairs: u64_from_usize(num_repairs),
            }),
        );
    }
}

pub(crate) fn trace_scheduling_details(
    events_trace: Option<&EventsProducer>,
    blocked: usize,
    queue_size: usize,
    buffer_size: usize,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::scheduling_details(SchedulingDetailsEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                blocked: u64_from_usize(blocked),
                queue_size: u64_from_usize(queue_size),
                buffer_size: u64_from_usize(buffer_size),
            }),
        );
    }
}

use {
    agave_perf_trace::{
        Event, EventsProducer, ReplaySlotCompleteEvent, TurbineSlotCompleteEvent, thread_id_u32,
        timestamp, u64_from_usize, wallclock_millis, write_or_record_drop,
    },
    solana_clock::Slot,
    std::time::Instant,
};

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

pub(crate) fn trace_turbine_slot_complete(
    events_trace: Option<&EventsProducer>,
    slot: Slot,
    first_shred_timestamp: u64,
) {
    if let Some(events_trace) = events_trace {
        let duration_ms = wallclock_millis().saturating_sub(first_shred_timestamp);
        let end_ts = timestamp();
        let start_ts = end_ts.saturating_sub(duration_ms.saturating_mul(1_000_000));
        write_or_record_drop(
            events_trace,
            Event::turbine_slot_complete(TurbineSlotCompleteEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                slot,
                start_ts,
                end_ts,
            }),
        );
    }
}

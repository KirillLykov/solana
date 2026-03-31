use agave_perf_trace::{
    Event, EventsProducer, RetransmitStatsEvent, thread_id_u32, timestamp, u64_from_usize,
    write_or_record_drop,
};

pub(crate) fn trace_retransmit_stats(
    events_trace: Option<&EventsProducer>,
    num_nodes: usize,
    num_shreds: usize,
) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::retransmit_stats(RetransmitStatsEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                num_nodes: u64_from_usize(num_nodes),
                num_shreds: u64_from_usize(num_shreds),
            }),
        );
    }
}

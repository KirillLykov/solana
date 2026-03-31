use {
    agave_perf_trace::{
        Event, EventsProducer, PohSlotEvent, PohSlotTag, thread_id_u32, timestamp,
        write_or_record_drop,
    },
    solana_clock::Slot,
};

pub(crate) fn trace_poh_slot(events_trace: Option<&EventsProducer>, slot: Slot, tag: PohSlotTag) {
    if let Some(events_trace) = events_trace {
        write_or_record_drop(
            events_trace,
            Event::poh_slot(PohSlotEvent {
                pid: std::process::id(),
                tid: thread_id_u32(),
                ts: timestamp(),
                slot,
                tag: tag as u64,
            }),
        );
    }
}

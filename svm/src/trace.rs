use agave_perf_trace::{SvmEvent, SvmProducer, thread_id_u64, write_or_record_drop};

pub(crate) fn trace_svm_transaction(
    svm_trace: Option<&SvmProducer>,
    sig: [u8; 64],
    start: u64,
    end: u64,
) {
    if let Some(svm_trace) = svm_trace {
        write_or_record_drop(
            svm_trace,
            SvmEvent {
                sig,
                start,
                end,
                tid: thread_id_u64(),
            },
        );
    }
}

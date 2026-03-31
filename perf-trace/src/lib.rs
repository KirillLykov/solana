#![cfg_attr(
    not(feature = "agave-unstable-api"),
    deprecated(
        since = "3.1.0",
        note = "This crate has been marked for formal inclusion in the Agave Unstable API. From \
                v4.0.0 onward, the `agave-unstable-api` crate feature must be specified to \
                acknowledge use of an interface that may break without warning."
    )
)]
#![warn(if_let_rescope)]
#![warn(keyword_idents_2024)]
#![warn(rust_2024_incompatible_pat)]
#![warn(tail_expr_drop_order)]
#![warn(unsafe_attr_outside_unsafe)]
#![warn(unsafe_op_in_unsafe_fn)]

pub use slowlana_trace::agave::{
    AgaveTracePaths, Event, PohSlotEvent, PohSlotTag, RepairStatsEvent, ReplaySlotCompleteEvent,
    RetransmitStatsEvent, SchedulingDetailsEvent, ShredFrontierEvent, ShredGapEvent, ShredKind,
    ShredRecvRangeEvent, ShredRecvTimestampEvent, ShredSource, ShredTurbineLayerEvent, SvmEvent,
    TransactionEvent, TransactionState, TurbineSlotCompleteEvent,
};
use {
    slowlana_trace::{
        TraceProducer, TraceQueuePaths,
        agave::{DEFAULT_TRACE_DIR, trace_paths},
    },
    std::{
        env,
        ffi::OsStr,
        ops::Deref,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub const ENABLE_ENV: &str = "SLOWLANA_AGAVE_TRACE";
pub const TRACE_DIR_ENV: &str = "SLOWLANA_AGAVE_TRACE_DIR";

pub const EVENTS_CAPACITY: usize = 16_384;
pub const TX_CAPACITY: usize = 100_000;
pub const SVM_CAPACITY: usize = 100_000;

pub fn enabled() -> bool {
    matches!(
        env::var_os(ENABLE_ENV).as_deref(),
        Some(value) if value == OsStr::new("1")
    )
}

pub fn trace_dir() -> PathBuf {
    env::var_os(TRACE_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_TRACE_DIR))
}

pub fn trace_paths_for_current_process() -> AgaveTracePaths {
    trace_paths(std::process::id(), trace_dir())
}

macro_rules! define_producer {
    ($name:ident, $event:ty, $queue:ident, $capacity:expr, $queue_name:literal) => {
        pub struct $name(TraceProducer<$event>);

        impl $name {
            pub fn create() -> Result<Option<Self>, InitError> {
                if !enabled() {
                    return Ok(None);
                }

                let paths = trace_paths_for_current_process();
                create_producer(paths.$queue, $capacity, $queue_name)
                    .map(Self)
                    .map(Some)
            }

            pub fn join() -> Result<Option<Self>, InitError> {
                if !enabled() {
                    return Ok(None);
                }

                let paths = trace_paths_for_current_process();
                join_producer(paths.$queue, $queue_name).map(Self).map(Some)
            }
        }

        impl Deref for $name {
            type Target = TraceProducer<$event>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

define_producer!(EventsProducer, Event, events, EVENTS_CAPACITY, "events");
define_producer!(TxProducer, TransactionEvent, tx, TX_CAPACITY, "tx");
define_producer!(SvmProducer, SvmEvent, svm, SVM_CAPACITY, "svm");

pub fn write_or_record_drop<T>(producer: &TraceProducer<T>, item: T) {
    if producer.try_write(item).is_err() {
        producer.record_drop();
    }
}

pub fn timestamp() -> u64 {
    let mut time_spec = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time_spec as *mut _) };
    if result != 0 {
        return 0;
    }

    (time_spec.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(time_spec.tv_nsec as u64)
}

pub fn wallclock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

pub fn thread_id_u32() -> u32 {
    u32::try_from(thread_id_u64()).unwrap_or(u32::MAX)
}

pub fn thread_id_u64() -> u64 {
    unsafe { libc::syscall(libc::SYS_gettid) as u64 }
}

pub fn u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn create_producer<T>(
    paths: TraceQueuePaths,
    capacity: usize,
    queue_name: &'static str,
) -> Result<TraceProducer<T>, InitError> {
    TraceProducer::create(paths, capacity)
        .map_err(|source| InitError::CreateQueue { queue_name, source })
}

fn join_producer<T>(
    paths: TraceQueuePaths,
    queue_name: &'static str,
) -> Result<TraceProducer<T>, InitError> {
    TraceProducer::join(paths).map_err(|source| InitError::JoinQueue { queue_name, source })
}

#[derive(Debug)]
pub enum InitError {
    CreateQueue {
        queue_name: &'static str,
        source: slowlana_trace::Error,
    },
    JoinQueue {
        queue_name: &'static str,
        source: slowlana_trace::Error,
    },
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateQueue { queue_name, source } => {
                write!(f, "failed to create {queue_name} trace queue: {source}")
            }
            Self::JoinQueue { queue_name, source } => {
                write!(f, "failed to join {queue_name} trace queue: {source}")
            }
        }
    }
}

impl std::error::Error for InitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CreateQueue { source, .. } | Self::JoinQueue { source, .. } => Some(source),
        }
    }
}

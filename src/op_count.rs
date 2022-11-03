use tracing::trace;

#[cfg(debug_assertions)]
static OP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub fn count_op() {
    #[cfg(debug_assertions)]
    {
        let _new_count = OP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if _new_count < OP_THRESHOLD && OP_THRESHOLD % 1024 == 0 {
            dbg!(_new_count);
        }
        if _new_count == OP_THRESHOLD {
            tracing_subscriber::fmt::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ENTER)
                .without_time()
                .pretty()
                .init();
        }
        trace!(op_count = _new_count);
    }
}

#[cfg(debug_assertions)]
const OP_THRESHOLD: usize = usize::MAX;

#[cfg(debug_assertions)]
#[allow(dead_code)]
pub fn op_late() -> bool {
    OP_COUNT.load(std::sync::atomic::Ordering::Relaxed) >= OP_THRESHOLD
}

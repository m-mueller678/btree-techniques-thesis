use crate::util::common_prefix_len;
use crate::PrefixTruncatedKey;

/// returns slot_id and prefix truncated separator
/// the upper range starts at slot_id+1
/// slot_id is either in lower or moved to the parent
pub fn find_separator<'a, F: FnMut(usize) -> PrefixTruncatedKey<'a>>(
    count: usize,
    is_leaf: bool,
    mut k: F,
) -> (usize, PrefixTruncatedKey<'a>) {
    debug_assert!(count > 1);
    if !is_leaf {
        // inner nodes are split in the middle
        // do not truncate separator to retain fence keys in children
        let slot_id = count as usize / 2;
        return (slot_id, k(slot_id));
    }

    let best_slot = if count >= 16 {
        let lower = count / 2 - count / 16;
        let upper = count / 2;
        let best_prefix_len = common_prefix_len(k(0).0, k(lower).0);
        (lower + 1..=upper)
            .rev()
            .find(|&i| common_prefix_len(k(0).0, k(i).0) == best_prefix_len)
            .unwrap_or(lower)
    } else {
        (count - 1) / 2
    };

    // try to truncate separator
    if best_slot + 1 < count {
        let common = common_prefix_len(k(best_slot).0, k(best_slot + 1).0);
        if k(best_slot).0.len() > common && k(best_slot + 1).0.len() > common + 1 {
            return (
                best_slot,
                PrefixTruncatedKey(&k(best_slot + 1).0[..common + 1]),
            );
        }
    }
    (best_slot, k(best_slot))
}

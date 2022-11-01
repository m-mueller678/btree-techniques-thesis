use crate::util::common_prefix_len;
use crate::PrefixTruncatedKey;

impl<'a> KeyRef<'a> for PrefixTruncatedKey<'a> {
    fn common_prefix_len(self, b: Self) -> usize {
        common_prefix_len(self.0, b.0)
    }

    fn len(self) -> usize {
        self.0.len()
    }

    fn truncate(self, new_len: usize) -> Self {
        PrefixTruncatedKey(&self.0[..new_len])
    }
}

pub trait KeyRef<'a> {
    fn common_prefix_len(self, b: Self) -> usize;
    fn len(self) -> usize;
    fn truncate(self, new_len: usize) -> Self;
}

/// returns slot_id and prefix truncated separator
/// the upper range starts at slot_id+1
/// slot_id is either in lower or moved to the parent
pub fn find_separator<'a, K: KeyRef<'a>, F: FnMut(usize) -> K>(
    count: usize,
    is_leaf: bool,
    mut k: F,
) -> (usize, K) {
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
        let best_prefix_len = k(0).common_prefix_len(k(lower));
        (lower + 1..=upper)
            .rev()
            .find(|&i| k(0).common_prefix_len(k(i)) == best_prefix_len)
            .unwrap_or(lower)
    } else {
        (count - 1) / 2
    };

    // try to truncate separator
    if best_slot + 1 < count {
        let common = k(best_slot).common_prefix_len(k(best_slot + 1));
        if k(best_slot).len() > common && k(best_slot + 1).len() > common + 1 {
            return (best_slot, k(best_slot + 1).truncate(common + 1));
        }
    }
    (best_slot, k(best_slot))
}

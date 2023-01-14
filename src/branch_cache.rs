use bytemuck::Contiguous;
use crate::adaptive::gen_random;

#[derive(Default, Clone)]
struct BranchCacheEntry {
    position: u16,
    hit_probability: u8,
}

impl BranchCacheEntry {
    #[inline]
    fn get_hint(&self) -> Option<usize> {
        if self.hit_probability > 90 {
            Some(self.position as usize)
        } else {
            None
        }
    }

    #[inline]
    fn store(&mut self, position: usize) {
        const DIV: u8 = 4;
        let update_probability = ((127u8.saturating_sub(self.hit_probability)) as u32) << 21;
        if gen_random() < update_probability {
            self.position = position as u16;
        }
        self.hit_probability = self.hit_probability - self.hit_probability / DIV + if self.position as usize == position { u8::MAX_VALUE / DIV } else { 0 };
    }
}

#[derive(Clone)]
pub struct BranchCacheAccessor {
    levels: [BranchCacheEntry; 4],
    index: u8,
    active: bool,
    #[cfg(debug_assertions)]
    predict_next: bool,
}

impl BranchCacheAccessor {
    pub fn new() -> Self {
        BranchCacheAccessor {
            levels: Default::default(),
            index: 0,
            active: true,
            #[cfg(debug_assertions)]
            predict_next: true,
        }
    }

    #[inline]
    pub fn predict(&mut self) -> Option<usize> {
        if cfg!(feature="branch-cache_false") {
            return None;
        }
        if self.active {
            #[cfg(debug_assertions)]{
                assert!(self.predict_next);
                self.predict_next = false;
            }
            self.levels[self.index as usize].get_hint()
        } else {
            None
        }
    }

    #[inline]
    pub fn store(&mut self, position: usize) {
        if cfg!(feature="branch-cache_false") {
            return;
        }
        if self.active {
            #[cfg(debug_assertions)]{
                assert!(!self.predict_next);
                self.predict_next = true;
            }
            self.active = self.active && (self.index as usize) < self.levels.len() && self.levels[self.index as usize].position as usize == position;
            self.levels[self.index as usize].store(position);
            self.index += 1;
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        #[cfg(debug_assertions)]{
            assert!(self.predict_next);
        }
        if cfg!(feature="branch-cache_false") {
            return;
        }
        self.active = true;
        self.index = 0;
    }

    #[inline]
    pub fn set_inactive(&mut self) {
        #[cfg(debug_assertions)]{
            assert!(self.predict_next);
        }
        self.active = false;
    }
}
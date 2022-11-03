use std::mem::{transmute, MaybeUninit};
use std::ops::Range;
use std::ptr;
use std::sync::Once;
use crate::{BTreeNode, BTreeNodeTag, PrefixTruncatedKey};
use crate::basic_node::BasicNode;
use crate::head_node::{U32HeadNode, U64HeadNode};
use std::ptr::{DynMetadata};
use crate::util::common_prefix_len;

pub trait InnerConversionSource {
    fn fences(&self) -> FenceData;
    fn key_count(&self) -> usize;
    fn get_child(&self, index: usize) -> *mut BTreeNode;
    /// key will be written to end of dst
    /// returns length of stripped key
    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()>;
}

/// lower and upper should have no common prefix when passed around.
/// call restrip before if neccesary.
#[derive(Debug)]
pub struct FenceData<'a> {
    pub prefix_len: usize,
    pub lower_fence: PrefixTruncatedKey<'a>,
    pub upper_fence: PrefixTruncatedKey<'a>,
}

impl FenceData<'_> {
    pub fn restrip(self) -> Self {
        let common = common_prefix_len(self.lower_fence.0, self.upper_fence.0);
        FenceData {
            prefix_len: self.prefix_len + common,
            lower_fence: PrefixTruncatedKey(&self.lower_fence.0[common..]),
            upper_fence: PrefixTruncatedKey(&self.upper_fence.0[common..]),
        }
    }
}

pub trait InnerConversionSink {
    /// source has same prefix_len as self
    /// on error, state of self is unspecified
    fn create(
        dst: &mut BTreeNode,
        src: &impl InnerConversionSource,
    ) -> Result<(), ()>;
}

const INNER_COUNT: usize = 3;

static mut INNER_VTABLES: [MaybeUninit::<DynMetadata<dyn InnerConversionSource>>; INNER_COUNT] = [MaybeUninit::uninit(); INNER_COUNT];
static VTABLE_INIT: Once = Once::new();

/// must be called before BTreeNode methods are used
fn init_vtables() {
    fn make_inner_vtable<N: InnerConversionSource>(tag: BTreeNodeTag, index: usize) {
        assert_eq!(tag as usize - 128, index);
        let ptr: *mut N = ptr::null_mut();
        unsafe {
            //INNER_VTABLES[index] = vtable;
        }
    }
    VTABLE_INIT.call_once(|| {
        make_inner_vtable::<BasicNode>(BTreeNodeTag::BasicInner, 0);
        make_inner_vtable::<U64HeadNode>(BTreeNodeTag::U64HeadNode, 1);
        make_inner_vtable::<U32HeadNode>(BTreeNodeTag::U32HeadNode, 2);
    })
}

impl BTreeNode {
    fn to_inner_conversion_source(&self) -> &dyn InnerConversionSource {
        debug_assert!(VTABLE_INIT.is_completed());
        let tag = self.tag();
        let index = tag as usize - 128;
        unsafe {
            &*ptr::from_raw_parts(self as *const Self as *const (), INNER_VTABLES[index].assume_init())
        }
    }
}

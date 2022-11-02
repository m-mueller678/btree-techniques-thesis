use std::mem::{transmute, MaybeUninit};
use std::ops::Range;
use std::sync::Once;
use crate::{BTreeNode, BTreeNodeTag, PrefixTruncatedKey};
use crate::basic_node::BasicNode;
use crate::head_node::{U32HeadNode, U64HeadNode};

pub trait InnerConversionSource {
    fn fence(&self, upper: bool) -> PrefixTruncatedKey;
    fn prefix_len(&self) -> usize;
    fn key_count(&self) -> usize;
    fn get_child(&self, index: usize) -> *mut BTreeNode;
    /// key will be written to end of dst
    /// returns length of stripped key
    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()>;
    /// source has same prefix_len as self
    /// on error, state of self is unspecified
    /// should not attempt to compactify
}

pub trait InnerConversionSink{
    fn create(
        dst: &mut BTreeNode,
        prefix_len: usize,
        lower_fence: PrefixTruncatedKey,
        upper_fence: PrefixTruncatedKey,
        upper_child: *mut BTreeNode,
        src: &impl InnerConversionSource,
    );
}

struct InnerNodeVtable {
    fence: fn(&BTreeNode, upper: bool) -> PrefixTruncatedKey,
    prefix_len: fn(&BTreeNode) -> usize,
    key_count: fn(&BTreeNode) -> usize,
    get_child: fn(&BTreeNode, index: usize) -> *mut BTreeNode,
    get_key: fn(&BTreeNode, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()>,
    batch_insert: fn(&mut BTreeNode, range: Range<usize>, source: &BTreeNode) -> Result<(), ()>,
    construct: fn(this: &mut BTreeNode, prefix_len: usize, lower_fence: PrefixTruncatedKey, upper_fence: PrefixTruncatedKey, upper_child: *mut BTreeNode) -> Result<(), ()>,
}

const INNER_COUNT: usize = 3;

static mut INNER_VTABLES: [MaybeUninit<InnerNodeVtable>; INNER_COUNT] = transmute(MaybeUninit::<[InnerNodeVtable; INNER_COUNT]>::uninit());
static VTABLE_INIT: Once = Once::new();

/// must be called before BTreeNode methods are used
fn init_vtables() {
    fn make_inner_vtable<N: InnerNode>(tag: BTreeNodeTag, index: usize) {
        assert_eq!(tag as usize - 128, index);
        unsafe {
            INNER_VTABLES[index].write(InnerNodeVtable {
                fence: transmute(N::fence as fn(&N, bool) -> PrefixTruncatedKey),
                prefix_len: transmute(N::prefix_len as fn(&N) -> usize),
                key_count: transmute(N::key_count as for<'a> fn(&'a N) -> usize),
                get_child: transmute(N::get_child as for<'a> fn(&'a N, usize) -> *mut BTreeNode),
                get_key: transmute(N::get_key as for<'a, 'b> fn(&'a N, usize, &'b mut [u8]) -> usize),
                batch_insert: transmute(N::batch_insert as for<'a, 'b> fn(&'a mut N, std::ops::Range<usize>, &'b BTreeNode) -> Result<(), ()>),
                construct: N::construct as _,
            })
        }
    }
    VTABLE_INIT.call_once(|| {
        make_inner_vtable::<BasicNode>(BTreeNodeTag::BasicInner, 0);
        make_inner_vtable::<U64HeadNode>(BTreeNodeTag::U64HeadNode, 1);
        make_inner_vtable::<U32HeadNode>(BTreeNodeTag::U32HeadNode, 2);
    })
}

fn get_inner_vtable(tag: BTreeNodeTag) -> &'static InnerNodeVtable {
    debug_assert!(VTABLE_INIT.is_completed());
    let index = tag as usize - 128;
    unsafe {
        &INNER_VTABLES[index]
    }
}

impl InnerNode for BTreeNode {
    fn fence(&self, upper: bool) -> PrefixTruncatedKey {
        (get_inner_vtable(self.tag()).fence)(self, upper)
    }

    fn prefix_len(&self) -> usize {
        (get_inner_vtable(self.tag()).prefix_len)(self)
    }

    fn key_count(&self) -> usize {
        (get_inner_vtable(self.tag()).key_count)(self)
    }

    fn get_child(&self, index: usize) -> *mut BTreeNode {
        (get_inner_vtable(self.tag()).get_child)(self, index)
    }

    fn get_key(&self, index: usize, dst: &mut [u8], strip_prefix: usize) -> Result<usize, ()> {
        (get_inner_vtable(self.tag()).get_key)(self, index, dst)
    }

    fn batch_insert(&mut self, range: Range<usize>, source: &BTreeNode) -> Result<(), ()> {
        (get_inner_vtable(self.tag()).batch_insert)(self, range, source)
    }

    fn construct(_: &mut BTreeNode, _: usize, _: PrefixTruncatedKey, _: PrefixTruncatedKey, _: *mut BTreeNode) -> Result<(), ()> {
        panic!("invalid operation")
    }
}

impl BTreeNode {
    fn construct_inner_from_tag(this: &mut BTreeNode, tag: BTreeNodeTag, prefix_len: usize, lower_fence: PrefixTruncatedKey, upper_fence: PrefixTruncatedKey, upper_child: *mut BTreeNode) -> Result<(), ()> {
        (get_inner_vtable(tag).construct)(this, prefix_len, lower_fence, upper_fence, upper_child)
    }
}

pub fn convert_node<'dst, Src: InnerNode, Dst: InnerNode>(src: &Src, dst: &'dst mut BTreeNode) -> Result<(), ()> {
    let count = src.key_count();
    Dst::construct(
        dst,
        src.prefix_len(),
        src.fence(false),
        src.fence(true),
        src.get_child(count),
    )?;
    todo!()
}

pub fn copy_range_growing_prefix<Src: InnerNode, Dst: InnerNode>(src: &Src, range: Range<usize>, dst: &mut Dst) -> Result<(), ()> {
    todo!()
}


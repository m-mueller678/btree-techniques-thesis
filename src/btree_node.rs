use std::ptr;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::basic_node::BasicNode;

pub const PAGE_SIZE: usize = 4096;

#[derive(IntoPrimitive,TryFromPrimitive,Debug,Clone,Copy)]
#[repr(u8)]
pub enum BTreeNodeTag {
    BasicLeaf,
    BasicInner,
}

#[repr(C)]
pub union BTreeNode{
    pub raw_bytes: [u8; PAGE_SIZE],
    pub basic:BasicNode,
}

impl BTreeNode{
    pub fn tag(&self)->BTreeNodeTag{
        BTreeNodeTag::try_from_primitive(unsafe{self.raw_bytes[0]}).unwrap()
    }

    /// descends to target node, returns target node, parent, and index within parent
    pub fn descend(mut self:&mut Self,key:&[u8],mut filter:impl FnMut(*mut BTreeNode)->bool)->(*mut BTreeNode,*mut BTreeNode,usize){
        let mut parent=ptr::null_mut();
        let mut index=0;
        while !filter(self){
            match self.tag(){
                BTreeNodeTag::BasicLeaf=>break,
                BTreeNodeTag::BasicInner=>unsafe {
                    index = self.basic.lower_bound(key).0;
                    parent = self;
                    self = &mut * self.basic.get_child(index);
                }
            }
        }
        (self,parent,index)
    }
}
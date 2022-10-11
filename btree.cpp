#include <cassert>
#include <cstdint>
#include <cstring>
#include "btree2020.hpp"
#include "cstdlib"
#include <new>
#include <functional>

unsigned min(unsigned a, unsigned b)
{
  return a < b ? a : b;
}

template <class T>
static T loadUnaligned(void* p)
{
  T x;
  memcpy(&x, p, sizeof(T));
  return x;
}

// Get order-preserving head of key (assuming little endian)
static uint32_t head(uint8_t* key, unsigned keyLength)
{
  switch (keyLength) {
    case 0:
      return 0;
    case 1:
      return static_cast<uint32_t>(key[0]) << 24;
    case 2:
      return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16;
    case 3:
      return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16) | (static_cast<uint32_t>(key[2]) << 8);
    default:
      return __builtin_bswap32(loadUnaligned<uint32_t>(key));
  }
}

// maximum page size (in bytes) is 65536
static constexpr unsigned pageSize = 4096;
static constexpr unsigned maxKVSize = pageSize / 4;

constexpr uint8_t TAG_SORTED_LEAF=0;

struct BTreeNodeTagHeader{
  uint8_t tag;
  
  uint8_t* ptr(){
    return reinterpret_cast<uint8_t*>(this);
  }
  
  bool isLeaf(){
    switch(tag){
      case TAG_SORTED_LEAF: return true;
      default:assert(false);
    }
  }
  
  bool isInner(){
    return !isLeaf();
  }
};

template<class T>
bool ptrInPage(BTreeNodeTagHeader* page, T* ptr){
  auto p1 = reinterpret_cast<intptr_t>(page);
  auto p2 = reinterpret_cast<intptr_t>(ptr);
  return p1<=p2 && (p2<p1+pageSize);
}

class FatSlot{
  uint16_t offset;
  uint16_t keyLen;
  uint16_t payloadLen;
  union{
    uint32_t head;
    uint8_t headBytes[4];
  } __attribute__((packed));
public:  
  unsigned getPayloadLen(BTreeNodeTagHeader* container){
    assert(ptrInPage(container,this));
    return payloadLen;
  }
  
  unsigned getKeyLen(BTreeNodeTagHeader* container){
    assert(ptrInPage(container,this));
    return keyLen;
  }
  
  uint8_t* getPayload(BTreeNodeTagHeader* container){
    assert(ptrInPage(container,this));
    return container->ptr() + offset + keyLen;
  }
  
  uint8_t* getKey(BTreeNodeTagHeader* container){
    assert(ptrInPage(container,this));
    return container->ptr() + offset;
  }
  
  uint32_t getHead(){
    return head;
  }
  
  void write(BTreeNodeTagHeader* container,uint16_t offset,uint16_t keyLen,uint16_t payloadLen, uint32_t head){
    assert(ptrInPage(container,this));
    assert(offset+keyLen+payloadLen<=pageSize);
    this->offset=offset;
    this->keyLen=keyLen;
    this->payloadLen=payloadLen;
    this->head=head;
  }
};

struct SortedLeafNodeheader:BTreeNodeTagHeader{
  BTreeNode* upper = nullptr;  // only used in inner nodes, points to last child
  
  struct FenceKeySlot {
    uint16_t offset;
    uint16_t length;
  };
  FenceKeySlot lowerFence = {0, 0};  // exclusive
  FenceKeySlot upperFence = {0, 0};  // inclusive
  
  uint16_t count = 0;
  uint16_t spaceUsed = 0;
  uint16_t dataOffset = static_cast<uint16_t>(pageSize);
  uint16_t prefixLength = 0;
  
  static const unsigned hintCount = 16;
  uint32_t hint[hintCount];
};

struct SortedLeafNode:SortedLeafNodeheader{
  uint8_t data[pageSize - sizeof(SortedLeafNodeheader)];
  
  SortedLeafNode(){
    tag=TAG_SORTED_LEAF;
  }
  
  unsigned freeSpace() { return (ptr() + dataOffset) - reinterpret_cast<uint8_t*>(slot(count)); }
  unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<uint8_t*>(slot(count)) - ptr()) - spaceUsed; }
  
  FatSlot* slot(unsigned slot_id){
    return reinterpret_cast<FatSlot*>(ptr() + sizeof(SortedLeafNode))+slot_id;
  }
  
  uint8_t* getLowerFence() { return ptr() + lowerFence.offset; }
  uint8_t* getUpperFence() { return ptr() + upperFence.offset; }
  uint8_t* getPrefix() { return ptr() + lowerFence.offset; }
  
  void searchHint(uint32_t keyHead, unsigned& lowerOut, unsigned& upperOut)
  {
    if (count > hintCount * 2) {
      unsigned dist = upperOut / (hintCount + 1);
      unsigned pos, pos2;
      for (pos = 0; pos < hintCount; pos++)
        if (hint[pos] >= keyHead)
          break;
        for (pos2 = pos; pos2 < hintCount; pos2++)
          if (hint[pos2] != keyHead)
            break;
          lowerOut = pos * dist;
        if (pos2 < hintCount)
          upperOut = (pos2 + 1) * dist;
    }
  }
  
  void copyKeyValueRange(SortedLeafNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount)
  {
    if (prefixLength <= dst->prefixLength) {  // prefix grows
      unsigned diff = dst->prefixLength - prefixLength;
      for (unsigned i = 0; i < srcCount; i++) {
        unsigned newKeyLength = slot(srcSlot + i)->getKeyLen(this) - diff;
        unsigned space = newKeyLength + slot(srcSlot + i)->getPayloadLen(this);
        assert(space <= dst->freeSpace());
        dst->dataOffset -= space;
        dst->spaceUsed += space;
        uint8_t* key = slot(srcSlot + i)->getKey(this) + diff;
        dst->slot(dstSlot + i)->write(
          dst,
          dst->dataOffset,
          newKeyLength,
          slot(srcSlot + i)->getPayloadLen(this),
          head(key, newKeyLength)
        );
        memcpy(dst->slot(dstSlot + i)->getKey(dst), key, space);
      }
    } else {
      for (unsigned i = 0; i < srcCount; i++)
        copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    dst->count += srcCount;
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot(dst->count)));
  }
  
  void copyKeyValue(uint16_t srcSlot, SortedLeafNode* dst, uint16_t dstSlot)
  {
    unsigned fullLength = slot(srcSlot)->getKeyLen(this) + prefixLength;
    uint8_t key[fullLength];
    memcpy(key, getPrefix(), prefixLength);
    memcpy(key+prefixLength, slot(srcSlot)->getKey(this), slot(srcSlot)->getKeyLen(this));
    dst->storeKeyValue(dstSlot, key, fullLength, slot(srcSlot)->getPayload(this), slot(srcSlot)->getPayloadLen(this));
  }
  
  void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
  {
    assert(slotId<count);
    key += prefixLength;
    keyLength -= prefixLength;
    unsigned space = keyLength + payloadLength;
    dataOffset -= space;
    spaceUsed += space;
    slot(slotId)->write(this,dataOffset,keyLength,payloadLength,head(key, keyLength));
    assert(reinterpret_cast<uint8_t*>(slot(count))<=reinterpret_cast<uint8_t*>(slot(slotId)->getKey(this)));
    memcpy(slot(slotId)->getKey(this), key, keyLength);
    memcpy(slot(slotId)->getPayload(this), payload, payloadLength);
  }
  
  void makeHint()
  {
    unsigned dist = count / (hintCount + 1);
    for (unsigned i = 0; i < hintCount; i++)
      hint[i] = slot(dist * (i + 1))->getHead();
  }
  
  void compactify()
  {
    unsigned should = freeSpaceAfterCompaction();
    SortedLeafNode tmp{};
    tmp.setFences(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length);
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.upper = upper;
    memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(SortedLeafNode));
    makeHint();
    assert(freeSpace() == should);
  }
  
  unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut)
  {
    foundOut = false;
    
    // check prefix
    int cmp = memcmp(key, ptr() + sizeof(SortedLeafNode), min(keyLength, prefixLength));
    if (cmp < 0) // key is less than prefix
      return 0;
    if (cmp > 0) // key is greater than prefix
      return count;
    if (keyLength < prefixLength) // key is equal but shorter than prefix
      return 0;
    key += prefixLength;
    keyLength -= prefixLength;
    
    // check hint
    unsigned lower = 0;
    unsigned upper = count;
    uint32_t keyHead = head(key, keyLength);
    searchHint(keyHead, lower, upper);
    
    // binary search on remaining range
    while (lower < upper) {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (keyHead < slot(mid)->getHead()) {
        upper = mid;
      } else if (keyHead > slot(mid)->getHead()) {
        lower = mid + 1;
      } else { // head is equal, check full key
        int cmp = memcmp(key, slot(mid)->getKey(this), min(keyLength, slot(mid)->getKeyLen(this)));
        if (cmp < 0) {
          upper = mid;
        } else if (cmp > 0) {
          lower = mid + 1;
        } else {
          if (keyLength < slot(mid)->getKeyLen(this)) { // key is shorter
            upper = mid;
          } else if (keyLength > slot(mid)->getKeyLen(this)) { // key is longer
            lower = mid + 1;
          } else {
            foundOut = true;
            return mid;
          }
        }
      }
    }
    return lower;
  }
  
  unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength)
  {
    assert(keyLength>prefixLength);
    return keyLength - prefixLength + payloadLength +sizeof(FatSlot);
  }
  
  bool requestSpaceFor(unsigned spaceNeeded)
  {
    if (spaceNeeded <= freeSpace())
      return true;
    if (spaceNeeded <= freeSpaceAfterCompaction()) {
      compactify();
      return true;
    }
    return false;
  }
  
  void insertFence(FenceKeySlot& fk, uint8_t* key, unsigned keyLength)
  {
    assert(freeSpace() >= keyLength);
    dataOffset -= keyLength;
    spaceUsed += keyLength;
    fk.offset = dataOffset;
    fk.length = keyLength;
    memcpy(ptr() + dataOffset, key, keyLength);
  }
  
  void setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen)
  {
    insertFence(lowerFence, lowerKey, lowerLen);
    insertFence(upperFence, upperKey, upperLen);
    for (prefixLength = 0; (prefixLength < min(lowerLen, upperLen)) && (lowerKey[prefixLength] == upperKey[prefixLength]); prefixLength++)
      ;
  }
  
  unsigned commonPrefix(unsigned slotA, unsigned slotB)
  {
    assert(slotA < count);
    assert(slotB < count);
    unsigned limit = min(slot(slotA)->getKeyLen(this), slot(slotB)->getKeyLen(this));
    uint8_t *a = slot(slotA)->getKey(this), *b = slot(slotB)->getKey(this);
    unsigned i;
    for (i = 0; i < limit; i++)
      if (a[i] != b[i])
        break;
      return i;
  }
  
  struct SeparatorInfo {
    unsigned length;   // length of new separator
    unsigned slot;     // slot at which we split
    bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
  };
  
  SeparatorInfo findSeparator()
  {
    assert(count > 1);
    if (isInner()) {
      // inner nodes are split in the middle
      unsigned slotId = count / 2;
      return SeparatorInfo{static_cast<unsigned>(prefixLength + slot(slotId)->getKeyLen(this)), slotId, false};
    }
    
    // find good separator slot
    unsigned bestPrefixLength, bestSlot;
    if (count > 16) {
      unsigned lower = (count / 2) - (count / 16);
      unsigned upper = (count / 2);
      
      bestPrefixLength = commonPrefix(lower, 0);
      bestSlot = lower;
      
      if (bestPrefixLength != commonPrefix(upper - 1, 0))
        for (bestSlot = lower + 1; (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++)
          ;
    } else {
      bestSlot = (count-1) / 2;
      bestPrefixLength = commonPrefix(bestSlot, 0);
    }
    
    // try to truncate separator
    unsigned common = commonPrefix(bestSlot, bestSlot + 1);
    if ((bestSlot + 1 < count) && (slot(bestSlot)->getKeyLen(this) > common) && (slot(bestSlot+1)->getKeyLen(this) > (common + 1)))
      return SeparatorInfo{prefixLength + common + 1, bestSlot, true};
    
    return SeparatorInfo{static_cast<unsigned>(prefixLength + slot(bestSlot)->getKeyLen(this)), bestSlot, false};
  }
  
  void getSep(uint8_t* sepKeyOut, SeparatorInfo info)
  {
    memcpy(sepKeyOut, getPrefix(), prefixLength);
    memcpy(sepKeyOut + prefixLength, slot(info.slot + info.isTruncated)->getKey(this), info.length - prefixLength);
  }
  
  BTreeNode* getChild(unsigned slotId)
  {
    assert(isInner());
    return loadUnaligned<BTreeNode*>(slot(slotId)->getPayload(this));
  }
  
  bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
  {
    if (!requestSpaceFor(spaceNeeded(keyLength, payloadLength)))
      return false;  // no space, insert fails
    bool found;
    unsigned slotId = lowerBound(key, keyLength,found);
    memmove(slot(slotId + 1), slot(slotId), sizeof(FatSlot) * (count - slotId));
    storeKeyValue(slotId, key, keyLength, payload, payloadLength);
    count++;
    updateHint(slotId);
    return true;
  }
  
  void updateHint(unsigned slotId)
  {
    unsigned dist = count / (hintCount + 1);
    unsigned begin = 0;
    if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
      begin = (slotId / dist) - 1;
    for (unsigned i = begin; i < hintCount; i++)
      hint[i] = slot(dist * (i + 1))->getHead();
  }
  
  bool splitNode(BTreeNode* parent);
};

union BTreeNode{
  uint8_t tag;
  SortedLeafNode sorted_leaf;
  BTreeNodeTagHeader tagNode;
  
  static BTreeNode* makeLeaf(){
    return reinterpret_cast<BTreeNode*>(new SortedLeafNode());
  }
  
  static BTreeNode* makeInner(BTreeNode* child){
    assert(false);
  }
  
  static BTreeNode* descend(BTreeNode*& node,uint8_t* key,unsigned keyLen,std::function<bool(BTreeNode*)> early_stop = [](auto n){return false;}){
    BTreeNode* parent=nullptr;
    while(node->tagNode.isInner() && !early_stop(node)){
      switch(node->tag){
        default:assert(false);
      }
    }
    return parent;
  }
  
  // How much space would inserting a new key of length "keyLength" require?
  unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength)
  {
    switch(tag){
      case TAG_SORTED_LEAF: return reinterpret_cast<SortedLeafNode*>(this)->spaceNeeded(keyLength,payloadLength);
      default:assert(false);
    }
  }
  
  unsigned spaceNeededInner(unsigned keyLength){
    switch(tag){
      default:assert(false);
    }
  }
  
  bool requestSpaceFor(unsigned spaceNeeded)
  {
    switch(tag){
      case TAG_SORTED_LEAF: return reinterpret_cast<SortedLeafNode*>(this)->requestSpaceFor(spaceNeeded);
      default:assert(false);
    }
  }
  
  void destroy(){
    switch(tag){
      case TAG_SORTED_LEAF: return;
      default: assert(false);
    }
    free(this);
  }
  
  bool insertInner(uint8_t* key, unsigned keyLength, BTreeNode* child)
  {
    switch(tag){
      default: assert(false);
    }
  }
  
  bool splitNode(BTreeNode* parent){
    switch(tag){
      default: assert(false);
    }
  }
};

bool SortedLeafNode::splitNode(BTreeNode* parent)
{
  // split
  SortedLeafNode::SeparatorInfo sepInfo = findSeparator();
  unsigned spaceNeededParent = parent->spaceNeededInner(sepInfo.length);
  if (!parent->requestSpaceFor(spaceNeededParent)) {  // is there enough space in the parent for the separator?
    return false;
  }
  
  uint8_t sepKey[sepInfo.length];
  getSep(sepKey, sepInfo);
  // split this node into nodeLeft and nodeRight
  assert(sepInfo.slot > 0);
  assert(sepInfo.slot  < count);
  SortedLeafNode* nodeLeft = new SortedLeafNode();
  nodeLeft->setFences(getLowerFence(), lowerFence.length, sepKey, sepInfo.length);
  SortedLeafNode tmp{};
  SortedLeafNode* nodeRight = &tmp;
  nodeRight->setFences(sepKey, sepInfo.length, getUpperFence(), upperFence.length);
  bool succ = parent->insertInner(sepKey, sepInfo.length, reinterpret_cast<BTreeNode*>(nodeLeft) );
  static_cast<void>(succ);
  assert(succ);
  if (isLeaf()) {
    copyKeyValueRange(nodeLeft, 0, 0, sepInfo.slot  + 1);
    copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
  } else {
    // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
    copyKeyValueRange(nodeLeft, 0, 0, sepInfo.slot );
    copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
    nodeLeft->upper = getChild(nodeLeft->count);
    nodeRight->upper = upper;
  }
  nodeLeft->makeHint();
  nodeRight->makeHint();
  memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
  return true;
}

BTree::BTree() : root(BTreeNode::makeLeaf()) {}

BTree::~BTree() { root->destroy(); }

// point lookup
uint8_t* BTree::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
  BTreeNode* tagNode=root;
  BTreeNode::descend(tagNode,key,keyLength);
  switch (tagNode->tag){
    case TAG_SORTED_LEAF:{
      auto node = reinterpret_cast<SortedLeafNode*>(tagNode);
      bool found;
      unsigned pos = node->lowerBound(key, keyLength, found);
      if (!found)
        return nullptr;
      payloadSizeOut = node->slot(pos)->getPayloadLen(node);
      return node->slot(pos)->getPayload(node);
    } 
    default:assert(false);
  }
}

bool BTree::lookup(uint8_t* key, unsigned keyLength)
{
  unsigned x;
  return lookup(key, keyLength, x) != nullptr;
}

void BTree::ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
  BTreeNode* node = root;
  auto parent = BTreeNode::descend(node,key,keyLength,[=](auto n){return n==toSplit;});
  assert(node==toSplit);
  splitNode(toSplit, parent, key, keyLength, payloadLength);
}

void BTree::splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength)
{
  // create new root if necessary
  if (!parent) {
    parent = BTreeNode::makeInner(node);
    root = parent;
  }
  if (!node->splitNode(parent)) {
    // must split parent first to make space for separator, restart from root to do this
    ensureSpace(parent, key, keyLength, sizeof(sizeof(BTreeNode*)));
  }
}

void BTree::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
  assert((keyLength+payloadLength) <= maxKVSize);
  BTreeNode* tagNode = root;
  BTreeNode* parent = BTreeNode::descend(tagNode,key,keyLength);
  switch(tagNode->tag){
    case TAG_SORTED_LEAF:{
      auto node =reinterpret_cast<SortedLeafNode*>(tagNode);
      if (node->insert(key, keyLength, payload, payloadLength))
        return;
      // node is full: split and restart
      splitNode(tagNode, parent, key, keyLength, payloadLength);
      insert(key, keyLength, payload, payloadLength);
      return;
    }
    default:assert(false);
  }
}

bool BTree::remove(uint8_t* key, unsigned keyLength)
{
  assert(false);
}

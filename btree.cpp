#include <cassert>
#include <cstdint>
#include <cstring>
#include "btree2020.hpp"
#include "cstdlib"
#include <new>

static unsigned min(unsigned a, unsigned b)
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
static const unsigned pageSize = 4096;

constexpr uint8_t TAG_SORTED_LEAF=0;

struct BTreeNodeTagHeader{
  uint8_t tag;
  
  BTreeNodeTagHeader(uint8_t t):tag(t){}
  
  uint8_t* ptr(){
    return reinterpret_cast<uint8_t*>(this);
  }
};

struct FatSlot{
  uint16_t offset;
  uint16_t keyLen;
  uint16_t payloadLen;
  union{
    uint32_t head;
    uint8_t headBytes[4];
  } __attribute__((packed));
  
  unsigned getPayloadLen(BTreeNodeTagHeader* container){
    return payloadLen;
  }
  
  unsigned getKeyLen(BTreeNodeTagHeader* container){
    return keyLen;
  }
  
  uint8_t* getPayload(BTreeNodeTagHeader* container){
    return container->ptr() + offset + keyLen;
  }
  
  uint8_t* getKey(BTreeNodeTagHeader* container){
    return container->ptr() + offset;
  }
};

struct SortedLeafNode:BTreeNodeTagHeader{
  struct KeySlot {
    uint16_t offset;
  };
  
  BTreeNode* upper = nullptr;  // only used in inner nodes, points to last child
  
  uint16_t lowerFenceOffset = 0;  // exclusive
  uint16_t upperFenceOffset = 0;  // inclusive
  uint16_t count = 0;
  uint16_t spaceUsed = 0;
  uint16_t dataOffset = static_cast<uint16_t>(pageSize);
  uint16_t prefixLength = 0;
  
  static const unsigned hintCount = 16;
  uint32_t hint[hintCount];
  
  SortedLeafNode():BTreeNodeTagHeader(TAG_SORTED_LEAF){
    
  }
  
  FatSlot* slot(unsigned slot_id){
    return reinterpret_cast<FatSlot*>(ptr() + sizeof(SortedLeafNode) +prefixLength)+slot_id;
  }
  
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
      if (keyHead < slot(mid)->head) {
        upper = mid;
      } else if (keyHead > slot(mid)->head) {
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
  
  uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut){
    bool found;
    unsigned pos = lowerBound(key, keyLength, found);
    if (!found)
      return nullptr;
    assert(pos < count);
    payloadSizeOut = slot(pos)->getPayloadLen(this);
    return slot(pos)->getPayload(this);
  }
};

union BTreeNode{
  uint8_t tag;
  SortedLeafNode sorted_leaf;
  
  uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
  {
    switch (tag){
      case TAG_SORTED_LEAF: return sorted_leaf.lookup(key,keyLength,payloadSizeOut);
      default: assert(false);
    }
  }
  
  static BTreeNode* makeLeaf(){
    assert(sizeof(SortedLeafNode) <= pageSize);
    void* data = malloc(pageSize);
    return reinterpret_cast<BTreeNode*>(new(data) SortedLeafNode);
  }
  
  void destroy(){
    switch(tag){
      case TAG_SORTED_LEAF: return sorted_leaf.~SortedLeafNode();
      default: assert(false);
    }
    free(this);
  }
};


BTree::BTree() : root(BTreeNode::makeLeaf()) {}

BTree::~BTree() { root->destroy(); }

// point lookup
uint8_t* BTree::lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
{
  return root->lookup(key,keyLength,payloadSizeOut);
}

bool BTree::lookup(uint8_t* key, unsigned keyLength)
{
  unsigned x;
  return lookup(key, keyLength, x) != nullptr;
}

void BTree::insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
{
  assert(false);
}

bool BTree::remove(uint8_t* key, unsigned keyLength)
{
  assert(false);
}

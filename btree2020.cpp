#include <memory>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <cassert>
#include <string>
#include <csignal>
#include <utility>
#include <iostream>
#include "PerfEvent.hpp"

using namespace std;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

struct BTreeNode;
typedef BTreeNode* ValueType;

// Compare two strings
static int cmpKeys(u8* a, u8* b, unsigned aLength, unsigned bLength) {
   int c = memcmp(a, b, min(aLength, bLength));
   if (c)
      return c;
   return (aLength - bLength);
}

static const unsigned pageSize = 16*1024;

struct BTreeNodeHeader {
   static const unsigned underFullSize = pageSize*0.4; // merging threshold

   struct FenceKey {
      u16 offset;
      u16 length;
   };

   BTreeNode* upper = nullptr; // only used in inner nodes

   // slots for lower and upper fence keys
   FenceKey lowerFence = {0,0}; // exclusive
   FenceKey upperFence = {0,0}; // inclusive

   u16 count = 0;
   bool isLeaf;
   u16 spaceUsed = 0;
   u16 dataOffset = static_cast<u16>(pageSize);
   u16 prefixLength = 0;

   static const unsigned hintCount=16;
   u32 hint[hintCount];
   u32 padding;

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}

   inline u8* ptr() { return reinterpret_cast<u8*>(this); }
   inline bool isInner() { return !isLeaf; }
   inline u8* getLowerFenceKey() { return lowerFence.offset?ptr()+lowerFence.offset:nullptr; }
   inline u8* getUpperFenceKey() { return upperFence.offset?ptr()+upperFence.offset:nullptr; }
};

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      u16 offset;
      u16 len;
      union {
         u32 head;
         u8 headBytes[4];
      };
   };
   Slot slot[(pageSize-sizeof(BTreeNodeHeader))/(sizeof(Slot))];

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   unsigned freeSpace() { return dataOffset-(reinterpret_cast<u8*>(slot+count)-ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize-(reinterpret_cast<u8*>(slot+count)-ptr())-spaceUsed; }

   bool requestSpaceFor(unsigned spaceNeeded) {
      if (spaceNeeded<=freeSpace())
         return true;
      if (spaceNeeded<=freeSpaceAfterCompaction()) {
         compactify();
         return true;
      }
      return false;
   }

   static BTreeNode* makeLeaf() { return new BTreeNode(true); }
   static BTreeNode* makeInner() { return new BTreeNode(false); }

   // String layout: restKey | Value
   inline u8* getKey(unsigned slotId) { return ptr()+slot[slotId].offset; }
   inline unsigned getKeyLen(unsigned slotId) { return slot[slotId].len; }
   inline unsigned getFullKeyLength(unsigned slotId) { return prefixLength + slot[slotId].len; }
   inline ValueType& getChild(unsigned slotId) { assert(isInner()); return *reinterpret_cast<ValueType*>(ptr()+slot[slotId].offset+slot[slotId].len); }

   // Copy key at "slotId" to "out" array
   inline void copyFullKey(unsigned slotId, u8* out) {
      memcpy(out, getLowerFenceKey(), prefixLength);
      memcpy(out+prefixLength, getKey(slotId), getKeyLen(slotId));
   }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength) {
      assert(keyLength>=prefixLength);
      return sizeof(Slot) + (keyLength-prefixLength) + (isInner() ? sizeof(ValueType) : 0);
   }

   // Get order-preserving head of key (assuming little endian)
   static u32 head(u8* key, unsigned keyLength) {
      switch (keyLength) {
         case 0: return 0;
         case 1: return static_cast<u32>(key[0])<<24;
         case 2: return static_cast<u32>(__builtin_bswap16(*reinterpret_cast<u16*>(key)))<<16;
         case 3: return (static_cast<u32>(__builtin_bswap16(*reinterpret_cast<u16*>(key)))<<16) | (static_cast<u32>(key[2])<<8);
         default: return __builtin_bswap32(*reinterpret_cast<u32*>(key));
      }
   }

   void makeHint() {
      unsigned dist = count/(hintCount+1);
      for (unsigned i=0; i<hintCount; i++)
         hint[i] = slot[dist*(i+1)].head;
   }

   void updateHint(unsigned slotId) {
      unsigned dist = count/(hintCount+1);
      unsigned begin = 0;
      if ((count>hintCount*2+1) && (((count-1)/(hintCount+1))==dist) && ((slotId/dist)>1))
         begin = (slotId/dist)-1;
      for (unsigned i=begin; i<hintCount; i++)
         hint[i] = slot[dist*(i+1)].head;
   }

   void searchHint(u32 keyHead, unsigned& pos, unsigned& pos2) {
      for (pos=0; pos<hintCount; pos++)
         if (hint[pos]>=keyHead)
            break;
      for (pos2=pos; pos2<hintCount; pos2++)
         if (hint[pos2]!=keyHead)
            break;
   }

   // lower bound search, returns slotId (if equalityOnly=true -1 is returned on no match)
   template<bool equalityOnly=false>
   int lowerBound(u8* key, unsigned keyLength) {
      if (equalityOnly) {
         if ((keyLength<prefixLength) || (bcmp(key, getLowerFenceKey(), prefixLength)!=0))
            return -1;
      } else  {
         int prefixCmp = cmpKeys(key, getLowerFenceKey(), min<unsigned>(keyLength, prefixLength), prefixLength);
         if (prefixCmp<0)
            return 0;
         else if (prefixCmp>0)
            return count;
      }
      key += prefixLength;
      keyLength -= prefixLength;

      unsigned lower = 0;
      unsigned upper = count;
      u32 keyHead = head(key, keyLength);

      if (count > hintCount*2) {
         unsigned dist = count/(hintCount+1);
         unsigned pos, pos2;
         searchHint(keyHead, pos, pos2);
         lower = pos * dist;
         if (pos2<hintCount)
            upper = (pos2+1) * dist;
      }

      while (lower<upper) {
         unsigned mid = ((upper-lower)/2)+lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid+1;
         } else if (slot[mid].len <= 4) {
            // head is equal and we don't have to check rest of key
            if (keyLength < slot[mid].len) {
               upper = mid;
            } else if (keyLength > slot[mid].len) {
               lower = mid+1;
            } else {
               return mid;
            }
         } else {
            // head is equal, but full comparison necessary
            assert(keyLength>=4);
            int cmp = cmpKeys(key+4, getKey(mid)+4, keyLength-4, getKeyLen(mid)-4);
            if (cmp<0) {
               upper = mid;
            } else if (cmp>0) {
               lower = mid+1;
            } else {
               return mid;
            }
         }
      }
      if (equalityOnly)
         return -1;
      return lower;
   }

   bool insert(u8* key, unsigned keyLength, ValueType value) {
      if (!requestSpaceFor(spaceNeeded(keyLength)))
         return false; // no space, insert fails
      unsigned slotId = lowerBound<false>(key, keyLength);
      memmove(slot+slotId+1, slot+slotId, sizeof(Slot)*(count-slotId));
      storeKeyValue(slotId, key, keyLength, value);
      count++;
      updateHint(slotId);
      //assert(lowerBound<true>(key, keyLength)==static_cast<int>(slotId));
      return true;
   }

   bool removeSlot(unsigned slotId) {
      spaceUsed -= getKeyLen(slotId);
      spaceUsed -= isInner() ? sizeof(ValueType) : 0;
      memmove(slot+slotId, slot+slotId+1, sizeof(Slot)*(count-slotId-1));
      count--;
      makeHint();
      return true;
   }

   bool remove(u8* key, unsigned keyLength) {
      int slotId = lowerBound<true>(key, keyLength);
      if (slotId == -1)
         return false; // key not found
      return removeSlot(slotId);
   }

   void compactify() {
      unsigned should = freeSpaceAfterCompaction();
      static_cast<void>(should);
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFenceKey(), lowerFence.length, getUpperFenceKey(), upperFence.length);
      copyKeyValueRange(&tmp, 0, 0, count);
      tmp.upper = upper;
      memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
      makeHint();
      assert(freeSpace()==should);
   }

   // merge right node into this node
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right) {
      if (isLeaf) {
         assert(right->isLeaf);
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
         unsigned leftGrow = (prefixLength-tmp.prefixLength)*count;
         unsigned rightGrow = (right->prefixLength-tmp.prefixLength)*right->count;
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot+count+right->count)-ptr()) + leftGrow + rightGrow;
         if (spaceUpperBound>pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<u8*>(right), &tmp, sizeof(BTreeNode));
         right->makeHint();
         return true;
      } else {
         assert(right->isInner());
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
         unsigned leftGrow = (prefixLength-tmp.prefixLength)*count;
         unsigned rightGrow = (right->prefixLength-tmp.prefixLength)*right->count;
         unsigned extraKeyLength = parent->getFullKeyLength(slotId);
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<u8*>(slot+count+right->count)-ptr()) + leftGrow + rightGrow + tmp.spaceNeeded(extraKeyLength);
         if (spaceUpperBound>pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         u8 extraKey[extraKeyLength];
         parent->copyFullKey(slotId, extraKey);
         storeKeyValue(count, extraKey, extraKeyLength, parent->getChild(slotId));
         count++;
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<u8*>(right), &tmp, sizeof(BTreeNode));
         return true;
      }
   }

   // store key/value pair at slotId
   void storeKeyValue(u16 slotId, u8* key, unsigned keyLength, ValueType value) {
      // Head
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].head = head(key, keyLength);
      slot[slotId].len = keyLength;
      // Value
      unsigned space = keyLength + (isInner()?sizeof(ValueType):0);
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId)>=reinterpret_cast<u8*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLength);
      if (isInner())
         getChild(slotId) = value;
   }

   void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned count) {
      if (prefixLength<=dst->prefixLength) { // prefix grows
         unsigned diff = dst->prefixLength-prefixLength;
         for (unsigned i=0; i<count; i++) {
            unsigned keyLength = getKeyLen(srcSlot+i) - diff;
            unsigned space = keyLength + (isInner()?sizeof(ValueType):0);
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot+i].offset = dst->dataOffset;
            u8* key = ptr()+slot[srcSlot+i].offset + diff;
            memcpy(dst->getKey(dstSlot+i), key, space);
            dst->slot[dstSlot+i].head = head(key, keyLength);
            dst->slot[dstSlot+i].len = keyLength;
         }
      } else {
         for (unsigned i=0; i<count; i++)
            copyKeyValue(srcSlot+i, dst, dstSlot+i);
      }
      dst->count += count;
      assert((dst->ptr()+dst->dataOffset)>=reinterpret_cast<u8*>(dst->slot+dst->count));
   }

   void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot) {
      unsigned fullLength = getFullKeyLength(srcSlot);
      u8 key[fullLength];
      copyFullKey(srcSlot, key);
      dst->storeKeyValue(dstSlot, key, fullLength, (isInner()?getChild(srcSlot):nullptr));
   }

   void insertFence(FenceKey& fk, u8* key, unsigned keyLength) {
      if (!key)
         return;
      assert(freeSpace()>=keyLength);
      dataOffset -= keyLength;
      spaceUsed += keyLength;
      fk.offset = dataOffset;
      fk.length = keyLength;
      memcpy(ptr()+dataOffset, key, keyLength);
   }

   void setFences(u8* lowerKey, unsigned lowerLen, u8* upperKey, unsigned upperLen) {
      insertFence(lowerFence, lowerKey, lowerLen);
      insertFence(upperFence, upperKey, upperLen);
      for (prefixLength=0; (prefixLength<min(lowerLen, upperLen)) && (lowerKey[prefixLength]==upperKey[prefixLength]); prefixLength++);
   }

   void splitNode(BTreeNode* parent, unsigned sepSlot, u8* sepKey, unsigned sepLength) {
      assert(sepSlot>0);
      assert(sepSlot<(pageSize/sizeof(ValueType)));
      BTreeNode* nodeLeft = new BTreeNode(isLeaf);
      nodeLeft->setFences(getLowerFenceKey(), lowerFence.length, sepKey, sepLength);
      BTreeNode tmp(isLeaf);
      BTreeNode* nodeRight = &tmp;
      nodeRight->setFences(sepKey, sepLength, getUpperFenceKey(), upperFence.length);
      bool succ = parent->insert(sepKey, sepLength, nodeLeft);
      static_cast<void>(succ);
      assert(succ);
      if (isLeaf) {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
      } else {
         copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
         copyKeyValueRange(nodeRight, 0, nodeLeft->count+1, count - nodeLeft->count - 1);
         nodeLeft->upper = getChild(nodeLeft->count);
         nodeRight->upper = upper;
      }
      nodeLeft->makeHint();
      nodeRight->makeHint();
      memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
   }

   struct SeparatorInfo {
      unsigned length;
      unsigned slot;
      bool trunc;
   };

   unsigned commonPrefix(unsigned slotA, unsigned slotB) {
      assert(slotA<count);
      assert(slotB<count);
      unsigned limit = min(slot[slotA].len, slot[slotB].len);
      u8* a = getKey(slotA), *b = getKey(slotB);
      unsigned i;
      for (i=0; i<limit; i++)
         if (a[i]!=b[i])
            break;
      return i;
   }

   SeparatorInfo findSep(bool sortedInsert) {
      if (sortedInsert && isInner())
         return SeparatorInfo{getFullKeyLength(count-1), static_cast<unsigned>(count-1), false};

      if (isInner())
         return SeparatorInfo{getFullKeyLength(count/2), static_cast<unsigned>(count/2), false};

      unsigned lower = count/2 - count/16;
      unsigned upper = count/2 + count/16;
      assert(upper<count);
      unsigned maxPos = count/2;
      int maxPrefix = commonPrefix(maxPos, 0);
      for (unsigned i=lower; i<upper; i++) {
         int prefix = commonPrefix(i, 0);
         if (prefix>maxPrefix) {
            maxPrefix = prefix;
            maxPos = i;
         }
      }
      unsigned common = commonPrefix(maxPos, maxPos+1);
      if ((slot[maxPos].len > common) && (slot[maxPos+1].len > common+1)) {
         return SeparatorInfo{static_cast<unsigned>(prefixLength+common+1), maxPos, true};
      }
      return SeparatorInfo{getFullKeyLength(maxPos), maxPos, false};
   }

   void getSep(u8* sepKeyOut, SeparatorInfo info) {
      memcpy(sepKeyOut, getLowerFenceKey(), prefixLength);
      if (info.trunc)
         memcpy(sepKeyOut+prefixLength, getKey(info.slot+1), info.length-prefixLength);
      else
         memcpy(sepKeyOut+prefixLength, getKey(info.slot), info.length-prefixLength);
   }

   BTreeNode* lookupInner(u8* key, unsigned keyLength) {
      unsigned pos = lowerBound<false>(key, keyLength);
      if (pos==count)
         return upper;
      return getChild(pos);
   }

   void destroy() {
      if (isInner()) {
         for (unsigned i=0; i<count; i++)
            getChild(i)->destroy();
         upper->destroy();
      }
      delete this;
      return;
   }
};

static_assert(sizeof(BTreeNode)==pageSize, "page size problem");

struct BTree {
   BTreeNode* root;
   unsigned pageCount;

   BTree() : root(BTreeNode::makeLeaf()), pageCount(1) {}

   bool lookup(u8* key, unsigned keyLength) {
      BTreeNode* node = root;
      while (node->isInner())
         node = node->lookupInner(key, keyLength);
      int pos = node->lowerBound<true>(key, keyLength);
      if (pos!=-1) {
         return true;
      }
      return false;
   }

   void splitNode(BTreeNode* node, BTreeNode* parent, u8* key, unsigned keyLength, bool sortedInsert=false) {
      if (!parent) {
         // create new root
         parent = BTreeNode::makeInner();
         pageCount++;
         parent->upper = node;
         root = parent;
      }
      BTreeNode::SeparatorInfo sepInfo = node->findSep(sortedInsert);
      unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length);
      if (parent->requestSpaceFor(spaceNeededParent)) { // Is there enough space in the parent for the separator?
         u8 sepKey[sepInfo.length];
         node->getSep(sepKey, sepInfo);
         pageCount++;
         node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
      } else
         ensureSpace(parent, spaceNeededParent, key, keyLength); // Must split parent first to make space for separator
   }

   void ensureSpace(BTreeNode* toSplit, unsigned spaceNeeded, u8* key, unsigned keyLength) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner() && (node!=toSplit)) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      splitNode(toSplit, parent, key, keyLength);
   }

   void insert(u8* key, unsigned keyLength, ValueType value) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner()) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      if (node->insert(key, keyLength, value))
         return;
      // no more space, need to split
      splitNode(node, parent, key, keyLength);
      insert(key, keyLength, value);
   }

   void insertLeafSorted(u8* key, unsigned keyLength, BTreeNode* leaf) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      BTreeNode* parentParent = nullptr;
      while (node->isInner()) {
         parentParent = parent;
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      if (parent->insert(key, keyLength, leaf)) {
         pageCount++;
         return;
      }
      // no more space, need to split
      splitNode(parent, parentParent, key, keyLength, true);
      insertLeafSorted(key, keyLength, leaf);
   }

   bool remove(u8* key, unsigned keyLength) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      int pos = 0;
      while (node->isInner()) {
         parent = node;
         pos = node->lowerBound<false>(key, keyLength);
         node = (pos==node->count) ? node->upper : node->getChild(pos);
      }
      if (!node->remove(key, keyLength))
         return false; // key not found
      if (node->freeSpaceAfterCompaction()>=BTreeNodeHeader::underFullSize) {
         // find neighbor and merge
         if (parent && (parent->count>=2) && (pos+1)<parent->count) {
            BTreeNode* right = parent->getChild(pos+1);
            if (right->freeSpaceAfterCompaction()>=BTreeNodeHeader::underFullSize) {
               pageCount -= node->mergeNodes(pos, parent, right);
               return true; // key has been deleted already
            }
         }
      }
      return true;
   }

   ~BTree() {
      root->destroy();
   }
};

// tree stats

unsigned countInner(BTreeNode* node) {
   if (node->isLeaf)
      return 0;
   unsigned sum = 1;
   for (unsigned i=0; i<node->count; i++)
      sum += countInner(node->getChild(i));
   sum += countInner(node->upper);
   return sum;
}

unsigned countPages(BTreeNode* node) {
   if (node->isLeaf)
      return 1;
   unsigned sum = 1;
   for (unsigned i=0; i<node->count; i++)
      sum += countPages(node->getChild(i));
   sum += countPages(node->upper);
   return sum;
}

uint64_t bytesFree(BTreeNode* node) {
   if (node->isLeaf)
      return node->freeSpaceAfterCompaction();
   uint64_t sum = node->freeSpaceAfterCompaction();
   for (unsigned i=0; i<node->count; i++)
      sum += bytesFree(node->getChild(i));
   sum += bytesFree(node->upper);
   return sum;
}

unsigned height(BTreeNode* node) {
   if (node->isLeaf)
      return 1;
   return 1+height(node->upper);
}

void printInfos(BTreeNode* root) {
   uint64_t cnt = countPages(root);
   uint64_t bytesFr = bytesFree(root);
   cerr << "nodes:" << cnt << " innerNodes:" << countInner(root) << " height:" << height(root) << " rootCnt:" << root->count << " bytesFree:" << bytesFr << " fillfactor:" << (1-(bytesFr/((double)cnt*pageSize))) << endl;
}

int main(int argc, char** argv) {
   PerfEvent e;

   ifstream in(argv[1]);

   vector<string> data;

   if (getenv("INT")) {
      vector<u64> v;
      u64 n = atof(getenv("INT"));
      for (u64 i=0; i<n; i++)
         v.push_back(i);
      random_shuffle(v.begin(), v.end());
      string s; s.resize(4);
      for (auto x : v) {
         *(u32*)(s.data()) = x;
         data.push_back(s);
      }
   } else {
      string line;
      while (getline(in,line))
         data.push_back(line);
   }

   uint64_t count = data.size();

      {
      BTree t;
      e.setParam("type", "btr");
      e.setParam("factr", "0");
      e.setParam("base", "0");
      {
         e.setParam("op", "insert");
         PerfEventBlock b(e, count);
         for (uint64_t i=0; i<count; i++) {
            t.insert((u8*)data[i].data(), data[i].size(), reinterpret_cast<ValueType>(i));
         }
      }

      {
         e.setParam("op", "lookup");
         PerfEventBlock b(e, count);
         for (uint64_t i=0; i<count; i++) {
            if (!t.lookup((u8*)data[i].data(), data[i].size()))
               throw;
         }
      }
      printInfos(t.root);
   }

   return 0;
}

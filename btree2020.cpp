#include <cstring>
#include <cassert>
#include <cstdint>

struct BTreeNode;

static unsigned min(unsigned a, unsigned b) { return a<b ? a : b; }

// Compare two strings
static int cmpKeys(uint8_t* a, uint8_t* b, unsigned aLength, unsigned bLength) {
   int c = memcmp(a, b, min(aLength, bLength));
   if (c)
      return c;
   return (aLength - bLength);
}

static const unsigned pageSize = 16*1024;

struct BTreeNodeHeader {
   static const unsigned underFullSize = pageSize*0.4; // merging threshold

   struct FenceKey {
      uint16_t offset;
      uint16_t length;
   };

   BTreeNode* upper = nullptr; // only used in inner nodes

   // slots for lower and upper fence keys
   FenceKey lowerFence = {0,0}; // exclusive
   FenceKey upperFence = {0,0}; // inclusive

   uint16_t count = 0;
   bool isLeaf;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset = static_cast<uint16_t>(pageSize);
   uint16_t prefixLength = 0;

   static const unsigned hintCount=16;
   uint32_t hint[hintCount];
   uint32_t padding;

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}

   inline uint8_t* ptr() { return reinterpret_cast<uint8_t*>(this); }
   inline bool isInner() { return !isLeaf; }
   inline uint8_t* getLowerFenceKey() { return ptr()+lowerFence.offset; }
   inline uint8_t* getUpperFenceKey() { return ptr()+upperFence.offset; }
};

template<class T>
T loadUnaligned(void* p) {
   T x;
   memcpy(&x, p, sizeof(T));
   return x;
}

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      uint16_t offset;
      uint16_t len;
      union {
         uint32_t head;
         uint8_t headBytes[4];
      };
   };
   Slot slot[(pageSize-sizeof(BTreeNodeHeader))/(sizeof(Slot))];

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   unsigned freeSpace() { return dataOffset-(reinterpret_cast<uint8_t*>(slot+count)-ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize-(reinterpret_cast<uint8_t*>(slot+count)-ptr())-spaceUsed; }

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
   inline uint8_t* getKey(unsigned slotId) { return ptr()+slot[slotId].offset; }
   inline unsigned getKeyLen(unsigned slotId) { return slot[slotId].len; }
   inline unsigned getFullKeyLength(unsigned slotId) { return prefixLength + slot[slotId].len; }
   inline void setChild(unsigned slotId, BTreeNode* child) { assert(isInner()); memcpy(ptr()+slot[slotId].offset+slot[slotId].len, &child, sizeof(BTreeNode*)); }
   inline BTreeNode* getChild(unsigned slotId) { assert(isInner()); return loadUnaligned<BTreeNode*>(ptr()+slot[slotId].offset+slot[slotId].len); }

   // Copy key at "slotId" to "out" array
   inline void copyFullKey(unsigned slotId, uint8_t* out) {
      memcpy(out, getLowerFenceKey(), prefixLength);
      memcpy(out+prefixLength, getKey(slotId), getKeyLen(slotId));
   }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength) {
      assert(keyLength>=prefixLength);
      return sizeof(Slot) + (keyLength-prefixLength) + (isInner() ? sizeof(BTreeNode*) : 0);
   }

   // Get order-preserving head of key (assuming little endian)
   static uint32_t head(uint8_t* key, unsigned keyLength) {
      switch (keyLength) {
         case 0: return 0;
         case 1: return static_cast<uint32_t>(key[0])<<24;
         case 2: return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key)))<<16;
         case 3: return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key)))<<16) | (static_cast<uint32_t>(key[2])<<8);
         default: return __builtin_bswap32(loadUnaligned<uint32_t>(key));
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

   void searchHint(uint32_t keyHead, unsigned& pos, unsigned& pos2) {
      for (pos=0; pos<hintCount; pos++)
         if (hint[pos]>=keyHead)
            break;
      for (pos2=pos; pos2<hintCount; pos2++)
         if (hint[pos2]!=keyHead)
            break;
   }

   // lower bound search, returns slotId (if equalityOnly=true -1 is returned on no match)
   template<bool equalityOnly=false>
   int lowerBound(uint8_t* key, unsigned keyLength) {
      if (equalityOnly) {
         if ((keyLength<prefixLength) || (bcmp(key, getLowerFenceKey(), prefixLength)!=0))
            return -1;
      } else  {
         int prefixCmp = cmpKeys(key, getLowerFenceKey(), min(keyLength, prefixLength), prefixLength);
         if (prefixCmp<0)
            return 0;
         else if (prefixCmp>0)
            return count;
      }
      key += prefixLength;
      keyLength -= prefixLength;

      unsigned lower = 0;
      unsigned upper = count;
      uint32_t keyHead = head(key, keyLength);

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

   bool insert(uint8_t* key, unsigned keyLength, BTreeNode* value=nullptr) {
      if (!requestSpaceFor(spaceNeeded(keyLength)))
         return false; // no space, insert fails
      unsigned slotId = lowerBound<false>(key, keyLength);
      memmove(slot+slotId+1, slot+slotId, sizeof(Slot)*(count-slotId));
      storeKeyValue(slotId, key, keyLength, value);
      count++;
      updateHint(slotId);
      return true;
   }

   bool removeSlot(unsigned slotId) {
      spaceUsed -= getKeyLen(slotId);
      spaceUsed -= isInner() ? sizeof(BTreeNode*) : 0;
      memmove(slot+slotId, slot+slotId+1, sizeof(Slot)*(count-slotId-1));
      count--;
      makeHint();
      return true;
   }

   bool remove(uint8_t* key, unsigned keyLength) {
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
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot+count+right->count)-ptr()) + leftGrow + rightGrow;
         if (spaceUpperBound>pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<uint8_t*>(right), &tmp, sizeof(BTreeNode));
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
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot+count+right->count)-ptr()) + leftGrow + rightGrow + tmp.spaceNeeded(extraKeyLength);
         if (spaceUpperBound>pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         uint8_t extraKey[extraKeyLength];
         parent->copyFullKey(slotId, extraKey);
         storeKeyValue(count, extraKey, extraKeyLength, parent->getChild(slotId));
         count++;
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<uint8_t*>(right), &tmp, sizeof(BTreeNode));
         return true;
      }
   }

   // store key/value pair at slotId
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, BTreeNode* child) {
      // slot
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].head = head(key, keyLength);
      slot[slotId].len = keyLength;
      // key
      unsigned space = keyLength + (isInner()?sizeof(BTreeNode*):0);
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId)>=reinterpret_cast<uint8_t*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLength);
      if (isInner())
         setChild(slotId, child);
   }

   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned count) {
      if (prefixLength<=dst->prefixLength) { // prefix grows
         unsigned diff = dst->prefixLength-prefixLength;
         for (unsigned i=0; i<count; i++) {
            unsigned keyLength = getKeyLen(srcSlot+i) - diff;
            unsigned space = keyLength + (isInner()?sizeof(BTreeNode*):0);
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot+i].offset = dst->dataOffset;
            uint8_t* key = ptr()+slot[srcSlot+i].offset + diff;
            memcpy(dst->getKey(dstSlot+i), key, space);
            dst->slot[dstSlot+i].head = head(key, keyLength);
            dst->slot[dstSlot+i].len = keyLength;
         }
      } else {
         for (unsigned i=0; i<count; i++)
            copyKeyValue(srcSlot+i, dst, dstSlot+i);
      }
      dst->count += count;
      assert((dst->ptr()+dst->dataOffset)>=reinterpret_cast<uint8_t*>(dst->slot+dst->count));
   }

   void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot) {
      unsigned fullLength = getFullKeyLength(srcSlot);
      uint8_t key[fullLength];
      copyFullKey(srcSlot, key);
      dst->storeKeyValue(dstSlot, key, fullLength, (isInner()?getChild(srcSlot):nullptr));
   }

   void insertFence(FenceKey& fk, uint8_t* key, unsigned keyLength) {
      if (!key)
         return;
      assert(freeSpace()>=keyLength);
      dataOffset -= keyLength;
      spaceUsed += keyLength;
      fk.offset = dataOffset;
      fk.length = keyLength;
      memcpy(ptr()+dataOffset, key, keyLength);
   }

   void setFences(uint8_t* lowerKey, unsigned lowerLen, uint8_t* upperKey, unsigned upperLen) {
      insertFence(lowerFence, lowerKey, lowerLen);
      insertFence(upperFence, upperKey, upperLen);
      for (prefixLength=0; (prefixLength<min(lowerLen, upperLen)) && (lowerKey[prefixLength]==upperKey[prefixLength]); prefixLength++);
   }

   void splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength) {
      assert(sepSlot>0);
      assert(sepSlot<(pageSize/sizeof(BTreeNode*)));
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
      uint8_t* a = getKey(slotA), *b = getKey(slotB);
      unsigned i;
      for (i=0; i<limit; i++)
         if (a[i]!=b[i])
            break;
      return i;
   }

   SeparatorInfo findSep() {
      if (isInner())
         return SeparatorInfo{getFullKeyLength(count/2), static_cast<unsigned>(count/2), false};

      unsigned lower = count/2 - count/16; // XXX
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

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info) {
      memcpy(sepKeyOut, getLowerFenceKey(), prefixLength);
      if (info.trunc)
         memcpy(sepKeyOut+prefixLength, getKey(info.slot+1), info.length-prefixLength);
      else
         memcpy(sepKeyOut+prefixLength, getKey(info.slot), info.length-prefixLength);
   }

   BTreeNode* lookupInner(uint8_t* key, unsigned keyLength) {
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

   BTree() : root(BTreeNode::makeLeaf()) {}

   bool lookup(uint8_t* key, unsigned keyLength) {
      BTreeNode* node = root;
      while (node->isInner())
         node = node->lookupInner(key, keyLength);
      int pos = node->lowerBound<true>(key, keyLength);
      if (pos!=-1) {
         return true;
      }
      return false;
   }

   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength) {
      if (!parent) {
         // create new root
         parent = BTreeNode::makeInner();
         parent->upper = node;
         root = parent;
      }
      BTreeNode::SeparatorInfo sepInfo = node->findSep();
      unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length);
      if (parent->requestSpaceFor(spaceNeededParent)) { // Is there enough space in the parent for the separator?
         uint8_t sepKey[sepInfo.length];
         node->getSep(sepKey, sepInfo);
         node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
      } else
         ensureSpace(parent, spaceNeededParent, key, keyLength); // Must split parent first to make space for separator
   }

   void ensureSpace(BTreeNode* toSplit, unsigned spaceNeeded, uint8_t* key, unsigned keyLength) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner() && (node!=toSplit)) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      splitNode(toSplit, parent, key, keyLength);
   }

   void insert(uint8_t* key, unsigned keyLength) {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner()) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      if (node->insert(key, keyLength))
         return;

      // node is full: split and restart
      splitNode(node, parent, key, keyLength);
      insert(key, keyLength);
   }

   bool remove(uint8_t* key, unsigned keyLength) {
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

      // merge if necessary
      if (node->freeSpaceAfterCompaction()>=BTreeNodeHeader::underFullSize) {
         // find neighbor and merge
         if (parent && (parent->count>=2) && (pos+1)<parent->count) {
            BTreeNode* right = parent->getChild(pos+1);
            if (right->freeSpaceAfterCompaction()>=BTreeNodeHeader::underFullSize) {
               node->mergeNodes(pos, parent, right);
               return true;
            }
         }
      }
      return true;
   }

   ~BTree() {
      root->destroy();
   }
};

using namespace std;

#include <fstream>
#include <algorithm>
#include <string>
#include "PerfEvent.hpp"

int main(int argc, char** argv) {
   PerfEvent e;

   vector<string> data;

   if (getenv("INT")) {
      vector<uint64_t> v;
      uint64_t n = atof(getenv("INT"));
      for (uint64_t i=0; i<n; i++)
         v.push_back(i);
      random_shuffle(v.begin(), v.end());
      string s; s.resize(4);
      for (auto x : v) {
         *(uint32_t*)(s.data()) = x;
         data.push_back(s);
      }
   } else {
      ifstream in(argv[1]);
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
            t.insert((uint8_t*)data[i].data(), data[i].size());
         }
      }

      {
         e.setParam("op", "lookup");
         PerfEventBlock b(e, count);
         for (uint64_t i=0; i<count; i++) {
            if (!t.lookup((uint8_t*)data[i].data(), data[i].size()))
               throw;
         }
      }
   }

   return 0;
}

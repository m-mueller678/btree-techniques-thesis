#include <csignal>


#include <cassert>
#include <cstdint>
#include <cstring>

struct BTreeNode;

// maximum page size (in bytes) is 65536
static const unsigned pageSize = 4096;

struct BTreeNodeHeader {
   static const unsigned underFullSize = pageSize - (pageSize / 8);  // merge nodes below this size

   struct FenceKeySlot {
      uint16_t offset;
      uint16_t length;
   };

   BTreeNode* upper = nullptr;  // only used in inner nodes

   FenceKeySlot lowerFence = {0, 0};  // exclusive
   FenceKeySlot upperFence = {0, 0};  // inclusive

   uint16_t count = 0;
   bool isLeaf;
   uint16_t spaceUsed = 0;
   uint16_t dataOffset = static_cast<uint16_t>(pageSize);
   uint16_t prefixLength = 0;

   static const unsigned hintCount = 16;
   uint32_t hint[hintCount];
   uint32_t padding;

   BTreeNodeHeader(bool isLeaf) : isLeaf(isLeaf) {}
   ~BTreeNodeHeader() {}
};

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

struct BTreeNode : public BTreeNodeHeader {
   struct Slot {
      uint16_t offset;
      uint16_t len;
      union {
         uint32_t head;
         uint8_t headBytes[4];
      };
   };
   union {
      Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)];  // grows from front
      uint8_t heap[pageSize - sizeof(BTreeNodeHeader)];                // grows from back
   };

   static constexpr unsigned maxKeySize = ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   uint8_t* ptr() { return reinterpret_cast<uint8_t*>(this); }
   bool isInner() { return !isLeaf; }
   uint8_t* getLowerFenceKey() { return ptr() + lowerFence.offset; }
   uint8_t* getUpperFenceKey() { return ptr() + upperFence.offset; }

   unsigned freeSpace() { return dataOffset - (reinterpret_cast<uint8_t*>(slot + count) - ptr()); }
   unsigned freeSpaceAfterCompaction() { return pageSize - (reinterpret_cast<uint8_t*>(slot + count) - ptr()) - spaceUsed; }

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

   static BTreeNode* makeLeaf() { return new BTreeNode(true); }
   static BTreeNode* makeInner() { return new BTreeNode(false); }

   uint8_t* getKey(unsigned slotId) { return ptr() + slot[slotId].offset; }

   unsigned getKeyLen(unsigned slotId) { return slot[slotId].len; }

   unsigned getFullKeyLength(unsigned slotId) { return prefixLength + slot[slotId].len; }

   BTreeNode* getChild(unsigned slotId)
   {
      assert(isInner());
      return loadUnaligned<BTreeNode*>(ptr() + slot[slotId].offset + slot[slotId].len);
   }

   void setChild(unsigned slotId, BTreeNode* child)
   {
      assert(isInner());
      memcpy(ptr() + slot[slotId].offset + slot[slotId].len, &child, sizeof(BTreeNode*));
   }

   // Copy key at "slotId" to "out" array
   void copyFullKey(unsigned slotId, uint8_t* out)
   {
      memcpy(out, getLowerFenceKey(), prefixLength);
      memcpy(out + prefixLength, getKey(slotId), getKeyLen(slotId));
   }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength)
   {
      assert(keyLength >= prefixLength);
      return sizeof(Slot) + (keyLength - prefixLength) + (isInner() ? sizeof(BTreeNode*) : 0);
   }

   void makeHint()
   {
      unsigned dist = count / (hintCount + 1);
      for (unsigned i = 0; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
   }

   void updateHint(unsigned slotId)
   {
      unsigned dist = count / (hintCount + 1);
      unsigned begin = 0;
      if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
         begin = (slotId / dist) - 1;
      for (unsigned i = begin; i < hintCount; i++)
         hint[i] = slot[dist * (i + 1)].head;
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

   static int keyContained(uint8_t* k, unsigned kLength, uint8_t* s, unsigned sLength)
   {
      int cmp = memcmp(k, s, min(kLength, sLength));
      if (cmp)
         return cmp;
      if (kLength < sLength)
         return -1;
      return cmp;
   }

   // lower bound search, returns slotId, foundOut indicates if there is an exact match
   unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut)
   {
      foundOut = false;

      // check prefix
      {
         int cmp = memcmp(key, getLowerFenceKey(), min(keyLength, prefixLength));
         if (cmp < 0) // key is less than prefix
            return 0;
         if (cmp > 0) // key is greater than prefix
            return count;
         if (keyLength < prefixLength) // key is equal but shorter than prefix
            return 0;
      }
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
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else {
            int cmp = memcmp(key, getKey(mid), min(keyLength, getKeyLen(mid)));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (keyLength < getKeyLen(mid)) { // key is shorter
                  foundOut = true;
                  upper = mid;
               } else if (keyLength > getKeyLen(mid)) { // key is longer
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

   // lowerBound wrapper ignoring exact match argument (for convenience)
   unsigned lowerBound(uint8_t* key, unsigned keyLength)
   {
      bool ignore;
      return lowerBound(key, keyLength, ignore);
   }

   bool insert(uint8_t* key, unsigned keyLength, BTreeNode* value = nullptr)
   {
      if (!requestSpaceFor(spaceNeeded(keyLength)))
         return false;  // no space, insert fails
      unsigned slotId = lowerBound(key, keyLength);
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, keyLength, value);
      count++;
      updateHint(slotId);
      return true;
   }

   bool removeSlot(unsigned slotId)
   {
      spaceUsed -= getKeyLen(slotId);
      spaceUsed -= isInner() ? sizeof(BTreeNode*) : 0;
      memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
      count--;
      makeHint();
      return true;
   }

   bool remove(uint8_t* key, unsigned keyLength)
   {
      bool found;
      unsigned slotId = lowerBound(key, keyLength, found);
      if (!found)
         return false;
      return removeSlot(slotId);
   }

   void compactify()
   {
      unsigned should = freeSpaceAfterCompaction();
      static_cast<void>(should);
      BTreeNode tmp(isLeaf);
      tmp.setFences(getLowerFenceKey(), lowerFence.length, getUpperFenceKey(), upperFence.length);
      copyKeyValueRange(&tmp, 0, 0, count);
      tmp.upper = upper;
      memcpy(reinterpret_cast<char*>(this), &tmp, sizeof(BTreeNode));
      makeHint();
      assert(freeSpace() == should);
   }

   // merge right node into this node
   bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right)
   {
      if (isLeaf) {
         assert(right->isLeaf);
         assert(parent->isInner());
         BTreeNode tmp(isLeaf);
         tmp.setFences(getLowerFenceKey(), lowerFence.length, right->getUpperFenceKey(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned spaceUpperBound =
             spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;
         if (spaceUpperBound > pageSize)
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
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned extraKeyLength = parent->getFullKeyLength(slotId);
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow +
                                    rightGrow + tmp.spaceNeeded(extraKeyLength);
         if (spaceUpperBound > pageSize)
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
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, BTreeNode* child)
   {
      // slot
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].head = head(key, keyLength);
      slot[slotId].len = keyLength;
      // key
      unsigned space = keyLength + (isInner() ? sizeof(BTreeNode*) : 0);
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId) >= reinterpret_cast<uint8_t*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLength);
      if (isInner())
         setChild(slotId, child);
   }

   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount)
   {
      if (prefixLength <= dst->prefixLength) {  // prefix grows
         unsigned diff = dst->prefixLength - prefixLength;
         for (unsigned i = 0; i < srcCount; i++) {
            unsigned keyLength = getKeyLen(srcSlot + i) - diff;
            unsigned space = keyLength + (isInner() ? sizeof(BTreeNode*) : 0);
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            uint8_t* key = ptr() + slot[srcSlot + i].offset + diff;
            memcpy(dst->getKey(dstSlot + i), key, space);
            dst->slot[dstSlot + i].head = head(key, keyLength);
            dst->slot[dstSlot + i].len = keyLength;
         }
      } else {
         for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
      }
      dst->count += srcCount;
      assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t*>(dst->slot + dst->count));
   }

   void copyKeyValue(uint16_t srcSlot, BTreeNode* dst, uint16_t dstSlot)
   {
      unsigned fullLength = getFullKeyLength(srcSlot);
      uint8_t key[fullLength];
      copyFullKey(srcSlot, key);
      dst->storeKeyValue(dstSlot, key, fullLength, (isInner() ? getChild(srcSlot) : nullptr));
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

   void splitNode(BTreeNode* parent, unsigned sepSlot, uint8_t* sepKey, unsigned sepLength)
   {
      assert(sepSlot > 0);
      assert(sepSlot < (pageSize / sizeof(BTreeNode*)));
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
         copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
         nodeLeft->upper = getChild(nodeLeft->count);
         nodeRight->upper = upper;
      }
      nodeLeft->makeHint();
      nodeRight->makeHint();
      memcpy(reinterpret_cast<char*>(this), nodeRight, sizeof(BTreeNode));
   }

   struct SeparatorInfo {
      unsigned length;   // length of new separator
      unsigned slot;     // slot at which we split
      bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
   };

   unsigned commonPrefix(unsigned slotA, unsigned slotB)
   {
      assert(slotA < count);
      unsigned limit = min(slot[slotA].len, slot[slotB].len);
      uint8_t *a = getKey(slotA), *b = getKey(slotB);
      unsigned i;
      for (i = 0; i < limit; i++)
         if (a[i] != b[i])
            break;
      return i;
   }

   SeparatorInfo findSep()
   {
      assert(count > 1);
      if (isInner()) {
         // we split inner nodes in the middle
         unsigned slot = count / 2;
         return SeparatorInfo{getFullKeyLength(slot), slot, false};
      }

      unsigned lower, upper;
      if (count < 4) {
         lower = count / 2;
         upper = lower + 1;
      } else {
         lower = count / 2 - count / 16;
         upper = count / 2 + count / 16;
      }

      // find best separator
      unsigned bestSlot = count / 2;
      unsigned bestPrefixLength = commonPrefix(bestSlot, 0);
      for (unsigned i = lower; i < upper; i++) {
         unsigned prefix = commonPrefix(i, 0);
         if (prefix > bestPrefixLength) {
            bestPrefixLength = prefix;
            bestSlot = i;
         }
      }

      // truncate separator
      unsigned common = commonPrefix(bestSlot, bestSlot + 1);
      if ((bestSlot + 1 < count) && (slot[bestSlot].len > common) && (slot[bestSlot + 1].len > common + 1)) {
         return SeparatorInfo{prefixLength + common + 1, bestSlot, true};
      }
      return SeparatorInfo{getFullKeyLength(bestSlot), bestSlot, false};
   }

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info)
   {
      memcpy(sepKeyOut, getLowerFenceKey(), prefixLength);
      memcpy(sepKeyOut + prefixLength, getKey(info.slot + info.isTruncated), info.length - prefixLength);
   }

   BTreeNode* lookupInner(uint8_t* key, unsigned keyLength)
   {
      unsigned pos = lowerBound(key, keyLength);
      if (pos == count)
         return upper;
      return getChild(pos);
   }

   void destroy()
   {
      if (isInner()) {
         for (unsigned i = 0; i < count; i++)
            getChild(i)->destroy();
         upper->destroy();
      }
      delete this;
      return;
   }
};

struct BTree {
   BTreeNode* root;

   BTree() : root(BTreeNode::makeLeaf()) {}

   bool lookup(uint8_t* key, unsigned keyLength)
   {
      BTreeNode* node = root;
      while (node->isInner())
         node = node->lookupInner(key, keyLength);
      bool found;
      unsigned pos = node->lowerBound(key, keyLength, found);
      static_cast<void>(pos);
      return found;
   }

   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength)
   {
      if (!parent) {
         // create new root
         parent = BTreeNode::makeInner();
         parent->upper = node;
         root = parent;
      }
      BTreeNode::SeparatorInfo sepInfo = node->findSep();
      unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length);
      if (parent->requestSpaceFor(spaceNeededParent)) {  // Is there enough space in the parent for the separator?
         uint8_t sepKey[sepInfo.length];
         node->getSep(sepKey, sepInfo);
         node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
      } else
         ensureSpace(parent, spaceNeededParent, key, keyLength);  // Must split parent first to make space for separator
   }

   void ensureSpace(BTreeNode* toSplit, unsigned spaceNeeded, uint8_t* key, unsigned keyLength)
   {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner() && (node != toSplit)) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      splitNode(toSplit, parent, key, keyLength);
   }

   void insert(uint8_t* key, unsigned keyLength)
   {
      assert(keyLength <= BTreeNode::maxKeySize);
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

   bool remove(uint8_t* key, unsigned keyLength)
   {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      unsigned pos = 0;
      while (node->isInner()) {
         parent = node;
         pos = node->lowerBound(key, keyLength);
         node = (pos == node->count) ? node->upper : node->getChild(pos);
      }
      if (!node->remove(key, keyLength))
         return false;  // key not found

      // merge if underfull
      if (node->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
         // find neighbor and merge
         if (parent && (parent->count >= 2) && (pos + 1) < parent->count) {  // XXX
            BTreeNode* right = parent->getChild(pos + 1);
            if (right->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
               node->mergeNodes(pos, parent, right);
               return true;
            }
         }
      }
      return true;
   }

   ~BTree() { root->destroy(); }
};

using namespace std;

#include <algorithm>
#include <fstream>
#include <string>
#include "PerfEvent.hpp"

unsigned countInner(BTreeNode* node)
{
   if (node->isLeaf)
      return 0;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countInner(node->getChild(i));
   sum += countInner(node->upper);
   return sum;
}

unsigned countPages(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countPages(node->getChild(i));
   sum += countPages(node->upper);
   return sum;
}

uint64_t bytesFree(BTreeNode* node)
{
   if (node->isLeaf)
      return node->freeSpaceAfterCompaction();
   uint64_t sum = node->freeSpaceAfterCompaction();
   for (unsigned i = 0; i < node->count; i++)
      sum += bytesFree(node->getChild(i));
   sum += bytesFree(node->upper);
   return sum;
}

unsigned height(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   return 1 + height(node->upper);
}

void printInfos(BTreeNode* root)
{
   uint64_t cnt = countPages(root);
   uint64_t bytesFr = bytesFree(root);
   cerr << "nodes:" << cnt << " innerNodes:" << countInner(root) << " height:" << height(root) << " rootCnt:" << root->count
        << " bytesFree:" << bytesFr << " fillfactor:" << (1 - (bytesFr / ((double)cnt * pageSize))) << endl;
}

void runTest(PerfEvent& e, vector<string>& data)
{
   if (getenv("SHUF"))
      random_shuffle(data.begin(), data.end());

   // add payload
   unsigned payloadSize = 8;
   for (uint64_t i=0; i<data.size(); i++) {
      string& s = data[i];
      s.append("ABCDEDFG");
      *(uint64_t*)(s.data() + s.size() - payloadSize) = i;
   }

   BTree t;
   uint64_t count = data.size();
   e.setParam("type", "btr");
   e.setParam("factr", "0");
   e.setParam("base", "0");
   {
      // insert
      e.setParam("op", "insert");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         if (i==24981) raise(SIGTRAP);  //j = 1940
         t.insert((uint8_t*)data[i].data(), data[i].size());

         //for (uint64_t j=0; j<=i; j+=1) if (!t.lookup((uint8_t*)data[j].data(), data[j].size())) throw;
         //for (uint64_t j=0; j<=i; j++) if (!t.lookup((uint8_t*)data[j].data(), data[j].size()-8)) throw;
      }
      printInfos(t.root);
   }

   {
      // lookup
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++)
         if (!t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
   }

   {
      // lookup prefix
      e.setParam("op", "lookup prefix");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++)
         if (!t.lookup((uint8_t*)data[i].data(), data[i].size()-8))
            throw;
   }

   // prefix lookup
   for (uint64_t i = 0; i < count; i++)
      t.lookup((uint8_t*)data[i].data(), data[i].size() - (data[i].size() / 4));

   {
      for (uint64_t i = 0; i < count; i += 4) // remove some
         if (!t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count; i++) // lookup all, causes some misses
         if ((i % 4 == 0) == t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // remove some more
         if ((i % 4 == 0) == t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // insert all
         t.insert((uint8_t*)data[i].data(), data[i].size());
      for (uint64_t i = 0; i < count; i++) // remove all
         t.remove((uint8_t*)data[i].data(), data[i].size());
   }
   printInfos(t.root);

   data.clear();
}

int main(int argc, char** argv)
{
   PerfEvent e;

   vector<string> data;

   if (getenv("INT")) {
      vector<uint64_t> v;
      uint64_t n = atof(getenv("INT"));
      for (uint64_t i = 0; i < n; i++)
         v.push_back(i);
      string s;
      s.resize(4);
      for (auto x : v) {
         *(uint32_t*)(s.data()) = x;
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG1")) {
      uint64_t n = atof(getenv("LONG1"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A');
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG2")) {
      uint64_t n = atof(getenv("LONG2"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A' + random() % 60);
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("FILE")) {
      ifstream in(getenv("FILE"));
      string line;
      while (getline(in, line))
         data.push_back(line);
      runTest(e, data);
   }

   return 0;
}

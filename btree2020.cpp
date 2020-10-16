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
      uint16_t keyLen;
      union {
         uint32_t head;
         uint8_t headBytes[4];
      };
      uint16_t payloadLen;
   } __attribute__((packed));
   union {
      Slot slot[(pageSize - sizeof(BTreeNodeHeader)) / sizeof(Slot)];  // grows from front
      uint8_t heap[pageSize - sizeof(BTreeNodeHeader)];                // grows from back
   };

   static constexpr unsigned maxKeySize = ((pageSize - sizeof(BTreeNodeHeader) - (2 * sizeof(Slot)))) / 4;

   BTreeNode(bool isLeaf) : BTreeNodeHeader(isLeaf) {}

   uint8_t* ptr() { return reinterpret_cast<uint8_t*>(this); }
   bool isInner() { return !isLeaf; }
   uint8_t* getLowerFence() { return ptr() + lowerFence.offset; }
   uint8_t* getUpperFence() { return ptr() + upperFence.offset; }
   uint8_t* getPrefix() { return ptr() + lowerFence.offset; } // any key on page is ok

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
   uint8_t* getPayload(unsigned slotId) { return ptr() + slot[slotId].offset + slot[slotId].keyLen; }

   BTreeNode* getChild(unsigned slotId)
   {
      assert(isInner());
      return loadUnaligned<BTreeNode*>(getPayload(slotId));
   }

   // How much space would inserting a new key of length "keyLength" require?
   unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength)
   {
      assert(keyLength >= prefixLength);
      return sizeof(Slot) + (keyLength - prefixLength) + payloadLength;
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

   // lower bound search, returns slotId, foundOut indicates if there is an exact match
   unsigned lowerBound(uint8_t* key, unsigned keyLength, bool& foundOut)
   {
      foundOut = false;

      // check prefix
      {
         int cmp = memcmp(key, getPrefix(), min(keyLength, prefixLength));
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

      // binary search on remaining range
      if (keyLength<4) {
         uint32_t mask = ~0 << ((4-keyLength)*8);
         if (keyLength == 0) // XXX
            mask = 0;
         while (lower < upper) {
            unsigned mid = ((upper - lower) / 2) + lower;
            if (keyHead < (slot[mid].head&mask)) {
               upper = mid;
            } else if (keyHead > (slot[mid].head&mask)) {
               lower = mid + 1;
            } else { // compared bytes are equal
               if (keyLength < slot[mid].keyLen) { // key is shorter
                  foundOut = true;
                  upper = mid;
               } else if (keyLength > slot[mid].keyLen) { // key is longer
                  lower = mid + 1;
               } else {
                  foundOut = true;
                  return mid;
               }
            }
         }
         return lower;
      }

      searchHint(keyHead, lower, upper);

      // binary search on remaining range
      while (lower < upper) {
         unsigned mid = ((upper - lower) / 2) + lower;
         if (keyHead < slot[mid].head) {
            upper = mid;
         } else if (keyHead > slot[mid].head) {
            lower = mid + 1;
         } else { // compared bytes are equal
            int cmp = memcmp(key, getKey(mid), min(keyLength, slot[mid].keyLen));
            if (cmp < 0) {
               upper = mid;
            } else if (cmp > 0) {
               lower = mid + 1;
            } else {
               if (keyLength < slot[mid].keyLen) { // key is shorter
                  foundOut = true;
                  upper = mid;
               } else if (keyLength > slot[mid].keyLen) { // key is longer
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

   bool insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      if (!requestSpaceFor(spaceNeeded(keyLength, payloadLength)))
         return false;  // no space, insert fails
      unsigned slotId = lowerBound(key, keyLength);
      memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
      storeKeyValue(slotId, key, keyLength, payload, payloadLength);
      count++;
      updateHint(slotId);
      return true;
   }

   bool removeSlot(unsigned slotId)
   {
      spaceUsed -= slot[slotId].keyLen;
      spaceUsed -= slot[slotId].payloadLen;
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
      tmp.setFences(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length);
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
         tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned spaceUpperBound = // XXX
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
         tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
         unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
         unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
         unsigned extraKeyLength = parent->prefixLength + parent->slot[slotId].keyLen;
         unsigned spaceUpperBound = spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow + tmp.spaceNeeded(extraKeyLength, sizeof(BTreeNode*));
         if (spaceUpperBound > pageSize)
            return false;
         copyKeyValueRange(&tmp, 0, 0, count);
         uint8_t extraKey[extraKeyLength];
         memcpy(extraKey, parent->getLowerFence(), parent->prefixLength);
         memcpy(extraKey+parent->prefixLength, parent->getKey(slotId), parent->slot[slotId].keyLen);
         storeKeyValue(count, extraKey, extraKeyLength, parent->getPayload(slotId), parent->slot[slotId].payloadLen);
         count++;
         right->copyKeyValueRange(&tmp, count, 0, right->count);
         parent->removeSlot(slotId);
         memcpy(reinterpret_cast<uint8_t*>(right), &tmp, sizeof(BTreeNode));
         return true;
      }
   }

   // store key/value pair at slotId
   void storeKeyValue(uint16_t slotId, uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      // slot
      key += prefixLength;
      keyLength -= prefixLength;
      slot[slotId].head = head(key, keyLength);
      slot[slotId].keyLen = keyLength;
      slot[slotId].payloadLen = payloadLength;
      // key
      unsigned space = keyLength + payloadLength;
      dataOffset -= space;
      spaceUsed += space;
      slot[slotId].offset = dataOffset;
      assert(getKey(slotId) >= reinterpret_cast<uint8_t*>(&slot[slotId]));
      memcpy(getKey(slotId), key, keyLength);
      memcpy(getPayload(slotId), payload, payloadLength);
   }

   void copyKeyValueRange(BTreeNode* dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount)
   {
      if (prefixLength <= dst->prefixLength) {  // prefix grows
         unsigned diff = dst->prefixLength - prefixLength;
         for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLength = slot[srcSlot + i].keyLen - diff;
            unsigned space = newKeyLength + slot[srcSlot + i].payloadLen;
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            dst->slot[dstSlot + i].offset = dst->dataOffset;
            uint8_t* key = getKey(srcSlot + i) + diff;
            memcpy(dst->getKey(dstSlot + i), key, space);
            dst->slot[dstSlot + i].head = head(key, newKeyLength);
            dst->slot[dstSlot + i].keyLen = newKeyLength;
            dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
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
      unsigned fullLength = slot[srcSlot].keyLen + prefixLength;
      uint8_t key[fullLength];
      memcpy(key, getPrefix(), prefixLength);
      memcpy(key+prefixLength, getKey(srcSlot), slot[srcSlot].keyLen);
      dst->storeKeyValue(dstSlot, key, fullLength, getPayload(srcSlot), slot[srcSlot].payloadLen);
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
      nodeLeft->setFences(getLowerFence(), lowerFence.length, sepKey, sepLength);
      BTreeNode tmp(isLeaf);
      BTreeNode* nodeRight = &tmp;
      nodeRight->setFences(sepKey, sepLength, getUpperFence(), upperFence.length);
      bool succ = parent->insert(sepKey, sepLength, reinterpret_cast<uint8_t*>(&nodeLeft), sizeof(BTreeNode*));
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
      unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
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
         unsigned slotId = count / 2;
         return SeparatorInfo{static_cast<unsigned>(prefixLength + slot[slotId].keyLen), slotId, false};
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
      unsigned bestSlot = count / 2; // XXX: compare with lowerFence or with 0?, only shrinks, mid bias
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
      if ((bestSlot + 1 < count) && (slot[bestSlot].keyLen > common) && (slot[bestSlot + 1].keyLen > common + 1)) {
         return SeparatorInfo{prefixLength + common + 1, bestSlot, true};
      }
      return SeparatorInfo{static_cast<unsigned>(prefixLength + slot[bestSlot].keyLen), bestSlot, false};
   }

   void getSep(uint8_t* sepKeyOut, SeparatorInfo info)
   {
      memcpy(sepKeyOut, getPrefix(), prefixLength);
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

   // point lookup
   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut)
   {
      BTreeNode* node = root;
      while (node->isInner())
         node = node->lookupInner(key, keyLength);
      bool found;
      unsigned pos = node->lowerBound(key, keyLength, found);
      if (!found)
         return nullptr;

      // key found, copy payload
      assert(pos < node->count);
      payloadSizeOut = node->slot[pos].payloadLen;
      return node->getPayload(pos);
   }

   bool lookup(uint8_t* key, unsigned keyLength)
   {
      unsigned x;
      return lookup(key, keyLength, x) != nullptr;
   }

   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength, unsigned payloadLength)
   {
      // create new root if necessary
      if (!parent) {
         parent = BTreeNode::makeInner();
         parent->upper = node;
         root = parent;
      }

      // split
      BTreeNode::SeparatorInfo sepInfo = node->findSep();
      unsigned spaceNeededParent = parent->spaceNeeded(sepInfo.length, payloadLength);
      if (parent->requestSpaceFor(spaceNeededParent)) {  // is there enough space in the parent for the separator?
         uint8_t sepKey[sepInfo.length];
         node->getSep(sepKey, sepInfo);
         node->splitNode(parent, sepInfo.slot, sepKey, sepInfo.length);
      } else {
         // must split parent first to make space for separator, restart from root to do this
         ensureSpace(parent, key, keyLength, payloadLength);
      }
   }

   void ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength, unsigned payloadLength)
   {
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner() && (node != toSplit)) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      splitNode(toSplit, parent, key, keyLength, payloadLength);
   }

   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength)
   {
      assert(keyLength <= BTreeNode::maxKeySize);
      BTreeNode* node = root;
      BTreeNode* parent = nullptr;
      while (node->isInner()) {
         parent = node;
         node = node->lookupInner(key, keyLength);
      }
      if (node->insert(key, keyLength, payload, payloadLength))
         return;

      // node is full: split and restart
      splitNode(node, parent, key, keyLength, payloadLength);
      insert(key, keyLength, payload, payloadLength);
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
         if (parent && (parent->count >= 2) && ((pos + 1) < parent->count)) {  // XXX
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

#include <csignal>
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

void pr(uint8_t* s, unsigned len) {
   for (unsigned i=0; i<len; i++)
      cout << s[i];
}

/*void printTree(BTreeNode* node) {
   cout << (node->isLeaf?"L":"I") << " " << node << endl;

   if (node->isLeaf) {
      for (unsigned i=0; i<node->count; i++) {
         cout << i << " " << node->getKey(i) << " ";
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << endl;
      }
      cout << endl;
      return;
   }

   for (unsigned i=0; i<node->count; i++) {
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << " " << node->getChild(i);
         cout << endl;
   }
   cout << " " << node->upper;

   for (unsigned i=0; i<node->count; i++)
      printTree(node->getChild(i));
   printTree(node->upper);
}
*/

void runTest(PerfEvent& e, vector<string>& data)
{
   if (getenv("SHUF"))
      random_shuffle(data.begin(), data.end());
   if (getenv("SORT"))
      sort(data.begin(), data.end());

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
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));

         //for (uint64_t j=0; j<=i; j+=1) if (!t.lookup((uint8_t*)data[j].data(), data[j].size())) throw;
         //for (uint64_t j=0; j<=i; j++) if (!t.lookup((uint8_t*)data[j].data(), data[j].size()-8)) throw;
      }
      printInfos(t.root);
      //printTree(t.root);
   }

   {
      // lookup
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         unsigned payloadSize;
         uint8_t* payload = t.lookup((uint8_t*)data[i].data(), data[i].size(), payloadSize);
         if (!payload || (payloadSize != sizeof(uint64_t)) || loadUnaligned<uint64_t>(payload) != i)
            throw;
      }
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
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // insert
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));
      for (uint64_t i = 0; i < count; i++) // remove all
         t.remove((uint8_t*)data[i].data(), data[i].size());
      for (uint64_t i = 0; i < count; i++)
         if (t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
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

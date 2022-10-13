//
// Created by marcus on 12.10.22.
//

#include "BTreeNode.h"
#include "btree.h"
#include <functional>
#include <new>
#include <cstring>
#include <cstdint>
#include <cassert>
#include "BasicNode.h"
#include "util.h"

bool BasicNode::splitNode(BTreeNode *parent) {
    // split
    BasicNode::SeparatorInfo sepInfo = findSeparator();
    unsigned spaceNeededParent = parent->spaceNeededInner(sepInfo.length);
    if (!parent->requestSpaceFor(spaceNeededParent)) {  // is there enough space in the parent for the separator?
        return false;
    }

    uint8_t sepKey[sepInfo.length];
    getSep(sepKey, sepInfo);
    // split this node into nodeLeft and nodeRight
    assert(sepInfo.slot > 0);
    assert(sepInfo.slot < count);
    BasicNode *nodeLeft = new BasicNode(isLeaf());
    nodeLeft->setFences(getLowerFence(), lowerFence.length, sepKey, sepInfo.length);
    BasicNode tmp(isLeaf());
    BasicNode *nodeRight = &tmp;
    nodeRight->setFences(sepKey, sepInfo.length, getUpperFence(), upperFence.length);
    bool succ = parent->insertInner(sepKey, sepInfo.length, reinterpret_cast<BTreeNode *>(nodeLeft));
    static_cast<void>(succ);
    assert(succ);
    if (isLeaf()) {
        copyKeyValueRange(nodeLeft, 0, 0, sepInfo.slot + 1);
        copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
    } else {
        // in inner node split, separator moves to parent (count == 1 + nodeLeft->count + nodeRight->count)
        copyKeyValueRange(nodeLeft, 0, 0, sepInfo.slot);
        copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
        nodeLeft->upper = getChild(nodeLeft->count);
        nodeRight->upper = upper;
    }
    nodeLeft->makeHint();
    nodeRight->makeHint();
    memcpy(reinterpret_cast<char *>(this), nodeRight, sizeof(BasicNode));
    return true;
}

BasicNode::BasicNode(bool leaf) {
    if (leaf)
        tag = BasicLeaf;
    else
        tag = BasicInner;
}

unsigned BasicNode::freeSpace() { return (ptr() + dataOffset) - reinterpret_cast<uint8_t *>(slot(count)); }

void BasicNode::searchHint(uint32_t keyHead, unsigned int &lowerOut, unsigned int &upperOut) {
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

void BasicNode::validate() {
    for (unsigned i = 0; i < count; ++i) {
        slot(i)->validate(this);
        if (i > 0) {
            unsigned int l1 = slot(i - 1)->getKeyLen(this);
            unsigned int l2 = slot(i)->getKeyLen(this);
            auto cmp = memcmp(slot(i - 1)->getKey(this), slot(i)->getKey(this), min(l1, l2));
            assert(i == 0 || cmp < 0 || cmp == 0 && l1 < l2);
        }
    }
}

void BasicNode::copyKeyValueRange(BasicNode *dst, uint16_t dstSlot, uint16_t srcSlot, unsigned int srcCount) {
    if (prefixLength <= dst->prefixLength) {  // prefix grows
        dst->count += srcCount;
        unsigned diff = dst->prefixLength - prefixLength;
        for (unsigned i = 0; i < srcCount; i++) {
            unsigned newKeyLength = slot(srcSlot + i)->getKeyLen(this) - diff;
            unsigned space = newKeyLength + slot(srcSlot + i)->getPayloadLen(this);
            assert(space <= dst->freeSpace());
            dst->dataOffset -= space;
            dst->spaceUsed += space;
            uint8_t *key = slot(srcSlot + i)->getKey(this) + diff;
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
        dst->count += srcCount;
        for (unsigned i = 0; i < srcCount; i++)
            copyKeyValue(srcSlot + i, dst, dstSlot + i);
    }
    dst->validate();
    assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<uint8_t *>(dst->slot(dst->count)));
}

void BasicNode::copyKeyValue(uint16_t srcSlot, BasicNode *dst, uint16_t dstSlot) {
    assert(dst != this);
    unsigned fullLength = slot(srcSlot)->getKeyLen(this) + prefixLength;
    uint8_t key[fullLength];
    memcpy(key, getPrefix(), prefixLength);
    memcpy(key + prefixLength, slot(srcSlot)->getKey(this), slot(srcSlot)->getKeyLen(this));
    dst->storeKeyValue(dstSlot, key + dst->prefixLength, fullLength - dst->prefixLength,
                       slot(srcSlot)->getPayload(this),
                       slot(srcSlot)->getPayloadLen(this));
}

void BasicNode::storeKeyValue(uint16_t slotId, uint8_t *truncatedKey, unsigned int truncatedKeyLength, uint8_t *payload,
                              unsigned int payloadLength) {
    assert(slotId < count);
    unsigned space = truncatedKeyLength + payloadLength;
    dataOffset -= space;
    spaceUsed += space;
    slot(slotId)->write(this, dataOffset, truncatedKeyLength, payloadLength, head(truncatedKey, truncatedKeyLength));
    assert(reinterpret_cast<uint8_t *>(slot(count)) <= reinterpret_cast<uint8_t *>(slot(slotId)->getKey(this)));
    memcpy(slot(slotId)->getKey(this), truncatedKey, truncatedKeyLength);
    memcpy(slot(slotId)->getPayload(this), payload, payloadLength);
}

FatSlot *BasicNode::slot(unsigned int slotId) {
    auto offset = sizeof(BasicNodeheader) + slotId * sizeof(FatSlot);
    assert(offset + sizeof(FatSlot) <= pageSize);
    return reinterpret_cast<FatSlot *>(ptr() + offset);
}

unsigned BasicNode::freeSpaceAfterCompaction() {
    return pageSize - (reinterpret_cast<uint8_t *>(slot(count)) - ptr()) - spaceUsed;
}

void BasicNode::makeHint() {
    unsigned dist = count / (hintCount + 1);
    for (unsigned i = 0; i < hintCount; i++) {
        hint[i] = slot(dist * (i + 1))->getHead();
        assert(i == 0 || hint[i - 1] <= hint[i]);
    }
}

void BasicNode::compactify() {
    unsigned should = freeSpaceAfterCompaction();
    BasicNode tmp(isLeaf());
    tmp.setFences(getLowerFence(), lowerFence.length, getUpperFence(), upperFence.length);
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.upper = upper;
    memcpy(reinterpret_cast<char *>(this), &tmp, sizeof(BasicNode));
    validate();
    makeHint();
    assert(freeSpace() == should);
    (void) (should);
}

unsigned BasicNode::lowerBound(uint8_t *key, unsigned int keyLength, bool &foundOut) {
    foundOut = false;

    // check prefix
    int cmp = memcmp(key, getPrefix(), min(keyLength, prefixLength));
    if (cmp < 0) // key is less than prefix
        throw;//return 0;
    if (cmp > 0) // key is greater than prefix
        throw;//return count;
    if (keyLength < prefixLength) // key is equal but shorter than prefix
        throw;//return 0;
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

unsigned BasicNode::spaceNeeded(unsigned int keyLength, unsigned int payloadLength) {
    assert(keyLength > prefixLength);
    return keyLength - prefixLength + payloadLength + sizeof(FatSlot);
}

bool BasicNode::requestSpaceFor(unsigned int spaceNeeded) {
    if (spaceNeeded <= freeSpace())
        return true;
    if (spaceNeeded <= freeSpaceAfterCompaction()) {
        compactify();
        return true;
    }
    return false;
}

void BasicNode::insertFence(BasicNodeheader::FenceKeySlot &fk, uint8_t *key, unsigned int keyLength) {
    assert(freeSpace() >= keyLength);
    dataOffset -= keyLength;
    spaceUsed += keyLength;
    fk.offset = dataOffset;
    fk.length = keyLength;
    memcpy(ptr() + dataOffset, key, keyLength);
}

void BasicNode::setFences(uint8_t *lowerKey, unsigned int lowerLen, uint8_t *upperKey, unsigned int upperLen) {
    insertFence(lowerFence, lowerKey, lowerLen);
    insertFence(upperFence, upperKey, upperLen);
    for (prefixLength = 0; (prefixLength < min(lowerLen, upperLen)) &&
                           (lowerKey[prefixLength] == upperKey[prefixLength]); prefixLength++);
}

unsigned BasicNode::commonPrefix(unsigned int slotA, unsigned int slotB) {
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

BasicNode::SeparatorInfo BasicNode::findSeparator() {
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
            for (bestSlot = lower + 1;
                 (bestSlot < upper) && (commonPrefix(bestSlot, 0) == bestPrefixLength); bestSlot++);
    } else {
        bestSlot = (count - 1) / 2;
        bestPrefixLength = commonPrefix(bestSlot, 0);
    }

    // try to truncate separator
    unsigned common = commonPrefix(bestSlot, bestSlot + 1);
    if ((bestSlot + 1 < count) && (slot(bestSlot)->getKeyLen(this) > common) &&
        (slot(bestSlot + 1)->getKeyLen(this) > (common + 1)))
        return SeparatorInfo{prefixLength + common + 1, bestSlot, true};

    return SeparatorInfo{static_cast<unsigned>(prefixLength + slot(bestSlot)->getKeyLen(this)), bestSlot, false};
}

void BasicNode::getSep(uint8_t *sepKeyOut, BasicNode::SeparatorInfo info) {
    memcpy(sepKeyOut, getPrefix(), prefixLength);
    memcpy(sepKeyOut + prefixLength, slot(info.slot + info.isTruncated)->getKey(this), info.length - prefixLength);
}

BTreeNode *BasicNode::getChild(unsigned int slotId) {
    assert(isInner());
    assert(slotId <= count);
    if (slotId == count)
        return upper;
    return loadUnaligned<BTreeNode *>(slot(slotId)->getPayload(this));
}

bool BasicNode::insert(uint8_t *key, unsigned int keyLength, uint8_t *payload, unsigned int payloadLength) {
    if (!requestSpaceFor(spaceNeeded(keyLength, payloadLength)))
        return false;  // no space, insert fails
    bool found;
    unsigned slotId = lowerBound(key, keyLength, found);
    assert(slotId <= count);
    memmove(slot(slotId + 1), slot(slotId), sizeof(FatSlot) * (count - slotId));
    assert(count < pageSize);
    count++;
    assert(keyLength >= prefixLength);
    storeKeyValue(slotId, key + prefixLength, keyLength - prefixLength, payload, payloadLength);
    validate();
    updateHint(slotId);
    return true;
}

void BasicNode::updateHint(unsigned int slotId) {
    unsigned dist = count / (hintCount + 1);
    unsigned begin = 0;
    if ((count > hintCount * 2 + 1) && (((count - 1) / (hintCount + 1)) == dist) && ((slotId / dist) > 1))
        begin = (slotId / dist) - 1;
    for (unsigned i = begin; i < hintCount; i++) {
        hint[i] = slot(dist * (i + 1))->getHead();
        assert(i == 0 || hint[i - 1] <= hint[i]);
    }
}

void BasicNode::destroyInner() {
    for (unsigned i = 0; i < count; i++)
        getChild(i)->destroy();
    upper->destroy();
}

void BasicNode::removeSlot(unsigned int slotId) {
    spaceUsed -= slot(slotId)->getKeyLen(this);
    spaceUsed -= slot(slotId)->getPayloadLen(this);
    memmove(slot(slotId), slot(slotId + 1), sizeof(FatSlot) * (count - slotId - 1));
    count--;
    validate();
    makeHint();
}

bool BasicNode::remove(uint8_t *key, unsigned int keyLen) {
    bool found;
    unsigned slotId = lowerBound(key, keyLen, found);
    if (!found)
        return false;
    removeSlot(slotId);
    return true;
}

bool BasicNode::mergeRightInner(uint8_t *sepKey, unsigned sepPrefixLen, unsigned sepRemainingLen, BasicNode *right) {
    assert(tag == BasicInner);
    assert(right->tag == BasicInner);
    BasicNode tmp(false);
    tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
    assert(tmp.prefixLength >= sepPrefixLen);
    unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
    unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
    unsigned spaceUpperBound =
            spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t *>(slot(count + right->count)) - ptr()) +
            leftGrow + rightGrow + tmp.spaceNeeded(sepPrefixLen + sepRemainingLen, sizeof(BTreeNode *));
    if (spaceUpperBound > pageSize)
        return false;
    copyKeyValueRange(&tmp, 0, 0, count);
    tmp.count++;
    tmp.storeKeyValue(count, sepKey + (tmp.prefixLength - sepPrefixLen),
                      sepPrefixLen + sepRemainingLen - tmp.prefixLength, reinterpret_cast<uint8_t *>(&upper),
                      sizeof(upper));
    right->copyKeyValueRange(&tmp, tmp.count, 0, right->count);
    tmp.upper = right->upper;
    tmp.makeHint();
    memcpy(reinterpret_cast<uint8_t *>(right), &tmp, sizeof(BasicNode));
    return true;
}

bool BasicNode::mergeRightLeaf(BasicNode *right) {
    assert(tag == BasicLeaf);
    assert(right->tag == BasicLeaf);
    BasicNode tmp(true);
    tmp.setFences(getLowerFence(), lowerFence.length, right->getUpperFence(), right->upperFence.length);
    unsigned leftGrow = (prefixLength - tmp.prefixLength) * count;
    unsigned rightGrow = (right->prefixLength - tmp.prefixLength) * right->count;
    unsigned spaceUpperBound =
            spaceUsed + right->spaceUsed + (reinterpret_cast<uint8_t *>(slot(count + right->count)) - ptr()) +
            leftGrow + rightGrow;
    if (spaceUpperBound > pageSize)
        return false;
    copyKeyValueRange(&tmp, 0, 0, count);
    right->copyKeyValueRange(&tmp, tmp.count, 0, right->count);
    tmp.makeHint();
    tmp.validate();
    memcpy(reinterpret_cast<uint8_t *>(right), &tmp, sizeof(BasicNode));
    return true;
}

bool BasicNode::mergeChildrenCheck(unsigned int pos) {
    assert(pos <= count);
    if (pos == count) {
        if (count == 0)
            return false;
        pos -= 1;
    }
    if (getChild(pos)->mergeRight(slot(pos)->getKey(this), prefixLength, slot(pos)->getKeyLen(this),
                                  getChild(pos + 1))) {
        delete getChild(pos);
        this->removeSlot(pos);
        validate();
        return true;
    }
    return false;
}

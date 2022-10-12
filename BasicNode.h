//
// Created by marcus on 12.10.22.
//

#ifndef BTREE_BASICNODE_H
#define BTREE_BASICNODE_H

#include "FatSlot.h"

struct BasicNodeheader : BTreeNode {
    BTreeNode *upper = nullptr;  // only used in inner nodes, points to last child

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

struct BasicNode : BasicNodeheader {
    uint8_t data[pageSize - sizeof(BasicNodeheader)];

    BasicNode(bool leaf);

    unsigned freeSpace();

    unsigned freeSpaceAfterCompaction();

    FatSlot *slot(unsigned slotId);

    uint8_t *getLowerFence() { return ptr() + lowerFence.offset; }

    uint8_t *getUpperFence() { return ptr() + upperFence.offset; }

    uint8_t *getPrefix() { return ptr() + lowerFence.offset; }

    void validate();

    void searchHint(uint32_t keyHead, unsigned &lowerOut, unsigned &upperOut);

    void copyKeyValueRange(BasicNode *dst, uint16_t dstSlot, uint16_t srcSlot, unsigned srcCount);

    void copyKeyValue(uint16_t srcSlot, BasicNode *dst, uint16_t dstSlot);

    void storeKeyValue(uint16_t slotId, uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength);

    void makeHint();

    void compactify();

    unsigned lowerBound(uint8_t *key, unsigned keyLength, bool &foundOut);

    unsigned spaceNeeded(unsigned keyLength, unsigned payloadLength);

    bool requestSpaceFor(unsigned spaceNeeded);

    void insertFence(FenceKeySlot &fk, uint8_t *key, unsigned keyLength);

    void setFences(uint8_t *lowerKey, unsigned lowerLen, uint8_t *upperKey, unsigned upperLen);

    unsigned commonPrefix(unsigned slotA, unsigned slotB);

    struct SeparatorInfo {
        unsigned length;   // length of new separator
        unsigned slot;     // slot at which we split
        bool isTruncated;  // if true, we truncate the separator taking length bytes from slot+1
    };

    SeparatorInfo findSeparator();

    void getSep(uint8_t *sepKeyOut, SeparatorInfo info);

    BTreeNode *getChild(unsigned slotId);

    bool insert(uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength);

    void updateHint(unsigned slotId);

    bool splitNode(BTreeNode *parent);

    void destroyInner();

    void removeSlot(unsigned slotId);

    bool remove(uint8_t *key, unsigned keyLen);

    bool mergeRightInner(uint8_t *sepKey, unsigned sepPrefixLen, unsigned sepRemainingLen, BasicNode *right);

    bool mergeRightLeaf(BasicNode *right);

    bool mergeChildrenCheck(unsigned pos);
};

#endif //BTREE_BASICNODE_H

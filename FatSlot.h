//
// Created by marcus on 12.10.22.
//

#ifndef BTREE_FATSLOT_H
#define BTREE_FATSLOT_H

#include <cstdint>
#include <cassert>
#include "BTreeNode.h"

class FatSlot {
    uint16_t offset;
    uint16_t keyLen;
    uint16_t payloadLen;
    union {
        uint32_t head;
        uint8_t headBytes[4];
    } __attribute__((packed));
public:
    inline void validate(BTreeNode *container) {
        (void) (container);
        assert(container->containsPtr(this));
        assert(offset <= pageSize);
        assert(keyLen <= pageSize);
        assert(payloadLen <= pageSize);
        assert(offset + keyLen + payloadLen <= pageSize);
    }

    inline unsigned getPayloadLen(BTreeNode *container) {
        validate(container);
        return payloadLen;
    }

    inline unsigned getKeyLen(BTreeNode *container) {
        validate(container);
        return keyLen;
    }

    inline uint8_t *getPayload(BTreeNode *container) {
        validate(container);
        return container->ptr() + offset + keyLen;
    }

    inline uint8_t *getKey(BTreeNode *container) {
        validate(container);
        return container->ptr() + offset;
    }

    inline uint32_t getHead() {
        return head;
    }

    inline void write(BTreeNode *container, uint16_t offset, uint16_t keyLen, uint16_t payloadLen, uint32_t head) {
        this->offset = offset;
        this->keyLen = keyLen;
        this->payloadLen = payloadLen;
        this->head = head;
        validate(container);
    }
};

#endif //BTREE_FATSLOT_H

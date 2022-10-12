//
// Created by marcus on 12.10.22.
//

#ifndef BTREE_BTREENODE_H
#define BTREE_BTREENODE_H

#include <cstdint>
#include <tuple>
#include <functional>

// maximum page size (in bytes) is 65536
constexpr unsigned pageSize = 4096;
constexpr unsigned maxKVSize = pageSize / 4;

enum NodeTag : uint8_t {
    BasicLeaf = 0,
    BasicInner = 1,
};

struct BTreeNode {
    NodeTag tag;

    uint8_t *ptr() {
        return reinterpret_cast<uint8_t *>(this);
    }

    bool isLeaf() {
        switch (tag) {
            case NodeTag::BasicLeaf:
                return true;
            case NodeTag::BasicInner:
                return false;
        }
    }

    bool isInner() {
        return !isLeaf();
    }

    template<class T>
    bool containsPtr(T *ptr) {
        auto p1 = reinterpret_cast<intptr_t>(this);
        auto p2 = reinterpret_cast<intptr_t>(ptr);
        return p1 <= p2 && (p2 < p1 + pageSize);
    }

    static BTreeNode *makeLeaf();

    static BTreeNode *makeInner(BTreeNode *child);

    static BTreeNode *descend(BTreeNode *&node, uint8_t *key, unsigned keyLen, unsigned &outPos,
                              std::function<bool(BTreeNode *)> early_stop = [](auto) { return false; });

    unsigned spaceNeededLeaf(unsigned keyLength, unsigned payloadLength);

    unsigned spaceNeededInner(unsigned keyLength);

    bool requestSpaceFor(unsigned spaceNeeded);

    void destroy();

    bool insertInner(uint8_t *key, unsigned keyLength, BTreeNode *child);

    bool splitNode(BTreeNode *parent);

    bool remove(uint8_t *key, unsigned keyLength);

    bool isUnderfull();

    // merges adjacent children if appropriate
    bool mergeChildrenCheck(unsigned firstChild);

    std::tuple<bool, BTreeNode *> copyTo(BTreeNode *dest);
};

#endif //BTREE_BTREENODE_H

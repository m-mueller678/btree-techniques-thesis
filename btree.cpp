#include <cassert>
#include <cstdint>
#include <cstring>
#include <new>
#include <functional>
#include "btree.h"
#include "BTreeNode.h"
#include "BasicNode.h"
#include "FatSlot.h"

BTree::BTree() : root(BTreeNode::makeLeaf()) {}

BTree::~BTree() { root->destroy(); }

// point lookup
uint8_t *BTree::lookup(uint8_t *key, unsigned keyLength, unsigned &payloadSizeOut) {
    BTreeNode *tagNode = root;
    unsigned outPos = 0;
    BTreeNode::descend(tagNode, key, keyLength, outPos);
    switch (tagNode->tag) {
        case TAG_BASIC_LEAF: {
            auto node = reinterpret_cast<BasicNode *>(tagNode);
            bool found;
            unsigned pos = node->lowerBound(key, keyLength, found);
            if (!found)
                return nullptr;
            payloadSizeOut = node->slot(pos)->getPayloadLen(node);
            return node->slot(pos)->getPayload(node);
        }
        default:
            throw;
    }
}

bool BTree::lookup(uint8_t *key, unsigned keyLength) {
    unsigned x;
    return lookup(key, keyLength, x) != nullptr;
}

void BTree::ensureSpace(BTreeNode *toSplit, uint8_t *key, unsigned keyLength) {
    BTreeNode *node = root;
    unsigned outPos = 0;
    auto parent = BTreeNode::descend(node, key, keyLength, outPos, [=](auto n) { return n == toSplit; });
    assert(node == toSplit);
    splitNode(toSplit, parent, key, keyLength);
}

void BTree::splitNode(BTreeNode *node, BTreeNode *parent, uint8_t *key, unsigned keyLength) {
    // create new root if necessary
    if (!parent) {
        parent = BTreeNode::makeInner(node);
        root = parent;
    }
    if (!node->splitNode(parent)) {
        // must split parent first to make space for separator, restart from root to do this
        ensureSpace(parent, key, keyLength);
    }
}

void BTree::insert(uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength) {
    if ((keyLength + payloadLength) > maxKVSize) {
        throw;
    }
    BTreeNode *tagNode = root;
    unsigned outPos = 0;
    BTreeNode *parent = BTreeNode::descend(tagNode, key, keyLength, outPos);
    switch (tagNode->tag) {
        case TAG_BASIC_LEAF: {
            auto node = reinterpret_cast<BasicNode *>(tagNode);
            if (node->insert(key, keyLength, payload, payloadLength)) {
                return;
            }
            // node is full: split and restart
            splitNode(tagNode, parent, key, keyLength);
            insert(key, keyLength, payload, payloadLength);
            return;
        }
        default:
            throw;
    }
}

bool BTree::remove(uint8_t *key, unsigned keyLength) {
    BTreeNode *mergeTarget = nullptr;
    continueMerge:
    BTreeNode *node = root;
    unsigned pos = 0;
    BTreeNode *parent = BTreeNode::descend(node, key, keyLength, pos, [=](auto n) { return n == mergeTarget; });
    if (mergeTarget == nullptr) {
        if (!node->remove(key, keyLength))
            return false;// key not found
        if (node->isUnderfull())
            mergeTarget = node;
        else
            return true;
    }
    assert(mergeTarget == node);
    if (parent->mergeChildrenCheck(pos) && parent->isUnderfull() && parent != root) {
        mergeTarget = parent;
        goto continueMerge;
    }
    return true;
}

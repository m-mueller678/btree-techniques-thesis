//
// Created by marcus on 12.10.22.
//

#include "BTreeNode.h"
#include <new>
#include <cstring>
#include <cassert>
#include "btree.h"
#include "BasicNode.h"

BTreeNode *BTreeNode::makeLeaf() {
    return reinterpret_cast<BTreeNode *>(new BasicNode(true));
}

BTreeNode *BTreeNode::makeInner(BTreeNode *child) {
    auto node = new BasicNode(false);
    node->upper = child;
    return reinterpret_cast<BTreeNode *>(node);
}

BTreeNode *BTreeNode::descend(BTreeNode *&tagNode, uint8_t *key, unsigned keyLen, unsigned &outPos,
                              std::function<bool(BTreeNode *)> early_stop) {
    BTreeNode *parent = nullptr;
    while (tagNode->isInner() && !early_stop(tagNode)) {
        switch (tagNode->tag) {
            case TAG_BASIC_INNER: {
                auto node = reinterpret_cast<BasicNode *>(tagNode);
                bool found;
                outPos = node->lowerBound(key, keyLen, found);
                parent = tagNode;
                tagNode = node->getChild(outPos);
                break;
            }
            default:
                throw;
        }
    }
    return parent;
}

// How much space would inserting a new key of length "keyLength" require?
unsigned BTreeNode::spaceNeededLeaf(unsigned keyLength, unsigned payloadLength) {
    switch (tag) {
        case TAG_BASIC_LEAF:
            return reinterpret_cast<BasicNode *>(this)->spaceNeeded(keyLength, payloadLength);
        default:
            throw;
    }
}

unsigned BTreeNode::spaceNeededInner(unsigned keyLength) {
    switch (tag) {
        case TAG_BASIC_INNER:
            return reinterpret_cast<BasicNode *>(this)->spaceNeeded(keyLength, sizeof(void *));
        default:
            throw;
    }
}

bool BTreeNode::requestSpaceFor(unsigned spaceNeeded) {
    switch (tag) {
        case TAG_BASIC_INNER:
        case TAG_BASIC_LEAF:
            return reinterpret_cast<BasicNode *>(this)->requestSpaceFor(spaceNeeded);
        default:
            throw;
    }
}

void BTreeNode::destroy() {
    switch (tag) {
        case TAG_BASIC_INNER:
            reinterpret_cast<BasicNode *>(this)->destroyInner();
        case TAG_BASIC_LEAF:
            return;
        default:
            throw;
    }
    delete this;
}

bool BTreeNode::insertInner(uint8_t *key, unsigned keyLength, BTreeNode *child) {
    switch (tag) {
        case TAG_BASIC_INNER:
            return reinterpret_cast<BasicNode *>(this)->insert(key, keyLength, reinterpret_cast<uint8_t *>(&child),
                                                               sizeof(child));
        default:
            throw;
    }
}

bool BTreeNode::splitNode(BTreeNode *parent) {
    switch (tag) {
        case TAG_BASIC_INNER:
        case TAG_BASIC_LEAF:
            return reinterpret_cast<BasicNode *>(this)->splitNode(parent);
        default:
            throw;
    }
}

bool BTreeNode::isUnderfull() {
    switch (tag) {
        case TAG_BASIC_INNER:
        case TAG_BASIC_LEAF: {
            auto node = reinterpret_cast<BasicNode *>(this);
            return node->freeSpaceAfterCompaction() >= pageSize * 3 / 4;
        }
        default:
            throw;
    }
}

bool BTreeNode::remove(uint8_t *key, unsigned keyLen) {
    switch (tag) {
        case TAG_BASIC_INNER:
        case TAG_BASIC_LEAF:
            return reinterpret_cast<BasicNode *>(this)->remove(key, keyLen);
        default:
            throw;
    }
}

bool BTreeNode::mergeChildrenCheck(unsigned) {
    return false; // TODO perform merge
}
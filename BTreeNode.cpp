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
            case BasicInner: {
                auto node = reinterpret_cast<BasicNode *>(tagNode);
                bool found;
                outPos = node->lowerBound(key, keyLen, found);
                parent = tagNode;
                tagNode = node->getChild(outPos);
                break;
            }
            case BasicLeaf:
                throw;
        }
    }
    return parent;
}

// How much space would inserting a new key of length "keyLength" require?
unsigned BTreeNode::spaceNeededLeaf(unsigned keyLength, unsigned payloadLength) {
    switch (tag) {
        case BasicLeaf:
            return reinterpret_cast<BasicNode *>(this)->spaceNeeded(keyLength, payloadLength);
        case BasicInner:
            throw;
    }
}

unsigned BTreeNode::spaceNeededInner(unsigned keyLength) {
    switch (tag) {
        case BasicInner:
            return reinterpret_cast<BasicNode *>(this)->spaceNeeded(keyLength, sizeof(void *));
        case BasicLeaf:
            throw;
    }
}

bool BTreeNode::requestSpaceFor(unsigned spaceNeeded) {
    switch (tag) {
        case BasicInner:
        case BasicLeaf:
            return reinterpret_cast<BasicNode *>(this)->requestSpaceFor(spaceNeeded);
    }
}

void BTreeNode::destroy() {
    switch (tag) {
        case BasicInner:
            reinterpret_cast<BasicNode *>(this)->destroyInner();
        case BasicLeaf:
            return;
    }
    delete this;
}

bool BTreeNode::insertInner(uint8_t *key, unsigned keyLength, BTreeNode *child) {
    switch (tag) {
        case BasicInner:
            return reinterpret_cast<BasicNode *>(this)->insert(key, keyLength, reinterpret_cast<uint8_t *>(&child),
                                                               sizeof(uint8_t *));
        case BasicLeaf:
            throw;
    }
}

bool BTreeNode::splitNode(BTreeNode *parent) {
    switch (tag) {
        case BasicInner:
        case BasicLeaf:
            return reinterpret_cast<BasicNode *>(this)->splitNode(parent);
    }
}

bool BTreeNode::isUnderfull() {
    switch (tag) {
        case BasicInner:
        case BasicLeaf: {
            auto node = reinterpret_cast<BasicNode *>(this);
            return node->freeSpaceAfterCompaction() >= pageSize * 3 / 4;
        }
    }
}

bool BTreeNode::remove(uint8_t *key, unsigned keyLen) {
    switch (tag) {
        case BasicInner:
        case BasicLeaf:
            return reinterpret_cast<BasicNode *>(this)->remove(key, keyLen);
    }
}

bool BTreeNode::mergeChildrenCheck(unsigned) {
    return false; // TODO perform merge
}
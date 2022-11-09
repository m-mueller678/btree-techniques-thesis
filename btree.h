#ifndef BTREE_BTREE_H
#define BTREE_BTREE_H

#include "btree-rust.h"

struct BTree {
private:
    RustBTree *root;
public:

    BTree(){
        root = btree_new();
    }

    ~BTree(){
        btree_destroy(root);
    }

    uint8_t *lookup(uint8_t *key, unsigned keyLength, unsigned &payloadSizeOut){
        uint64_t sizeOut=0;
        auto ret= btree_lookup(root,key,keyLength,&sizeOut);
        payloadSizeOut = sizeOut;
        return ret;
    }

    bool lookup(uint8_t *key, unsigned keyLength){
        uint64_t sizeOut=0;
        return btree_lookup(root, key, keyLength, &sizeOut);
    }

    void insert(uint8_t *key, unsigned keyLength, uint8_t *payload, unsigned payloadLength) {
        btree_insert(root, key, keyLength, payload, payloadLength);
    }

    bool remove(uint8_t *key, unsigned keyLength) {
        return btree_remove(root, key, keyLength);
    }

    void print_info() {
        btree_print_info(root);
    }
};


#endif //BTREE_BTREE_H

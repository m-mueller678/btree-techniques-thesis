//
// Created by marcus on 13.10.22.
//

#ifndef BTREE_BTREE_RUST_H
#define BTREE_BTREE_RUST_H

extern "C" {

struct RustBTree;

RustBTree *btree_new();
void btree_insert(RustBTree *b_tree, std::uint8_t *key, std::uint64_t keyLen, std::uint8_t *payload,
                  std::uint64_t payloadLen);
std::uint8_t *btree_lookup(RustBTree *b_tree, std::uint8_t *key, std::uint64_t keyLen, std::uint64_t *payloadLenOut);
bool btree_remove(RustBTree *b_tree, std::uint8_t *key, std::uint64_t keyLen);
void btree_destroy(RustBTree *b_tree);
void btree_print_info(RustBTree *b_tree);

}
#endif //BTREE_BTREE_RUST_H

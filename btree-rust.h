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
void print_tpcc_result(double time_sec, std::uint64_t tx_count, std::uint64_t warehouse_count);
void tpcc_begin();

// key_buffer and key must not be null, even if zero length
void btree_scan_asc(RustBTree *b_tree, std::uint8_t const *key, std::uint64_t key_len, std::uint8_t *key_buffer,
                    bool (*continue_callback)(std::uint8_t const *));
void btree_scan_desc(RustBTree *b_tree, std::uint8_t const *key, std::uint64_t key_len, std::uint8_t *key_buffer,
                     bool (*continue_callback)(std::uint8_t const *));

}
#endif //BTREE_BTREE_RUST_H

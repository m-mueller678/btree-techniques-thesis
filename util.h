//
// Created by marcus on 12.10.22.
//

#ifndef BTREE_UTIL_H
#define BTREE_UTIL_H

#include <cstdint>

inline unsigned min(unsigned int a, unsigned int b) {
    return a < b ? a : b;
}

template<class T>
static T loadUnaligned(void *p) {
    T x;
    memcpy(&x, p, sizeof(T));
    return x;
}


// Get order-preserving head of key (assuming little endian)
inline static uint32_t head(uint8_t *key, unsigned keyLength) {
    switch (keyLength) {
        case 0:
            return 0;
        case 1:
            return static_cast<uint32_t>(key[0]) << 24;
        case 2:
            return static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16;
        case 3:
            return (static_cast<uint32_t>(__builtin_bswap16(loadUnaligned<uint16_t>(key))) << 16) |
                   (static_cast<uint32_t>(key[2]) << 8);
        default:
            return __builtin_bswap32(loadUnaligned<uint32_t>(key));
    }
}


#endif //BTREE_UTIL_H

#include <atomic>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cassert>

#include <sys/mman.h>

#include "PerfEvent.hpp"

using namespace std;

void* allocHuge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_HUGEPAGE);
   return p;
}

void* allocSmall(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
   madvise(p, size, MADV_NOHUGEPAGE);
   return p;
}

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID;

struct PageState {
   atomic<u64> stateAndVersion;

   static const u64 Unlocked = 0;
   static const u64 MaxShared = 252;
   static const u64 Locked = 253;
   static const u64 Marked = 254;
   static const u64 Evicted = 255;

   PageState() {}

   void init() { stateAndVersion.store(sameVersion(0, Evicted), std::memory_order_release); }

   static inline u64 sameVersion(u64 oldStateAndVersion, u64 newState) { return ((oldStateAndVersion<<8)>>8) | newState<<56; }
   static inline u64 nextVersion(u64 oldStateAndVersion, u64 newState) { return (((oldStateAndVersion<<8)>>8)+1) | newState<<56; }

   bool tryLockX(u64 oldStateAndVersion) {
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Locked));
   }

   void unlockX() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Unlocked), std::memory_order_release);
   }

   void unlockXEvicted() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), Evicted), std::memory_order_release);
   }

   void downgradeLock() {
      assert(getState() == Locked);
      stateAndVersion.store(nextVersion(stateAndVersion.load(), 1), std::memory_order_release);
   }

   bool tryLockS(u64 oldStateAndVersion) {
      u64 s = getState(oldStateAndVersion);
      if (s<MaxShared)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, s+1));
      if (s==Marked)
         return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, 1));
      return false;
   }

   void unlockS() {
      while (true) {
         u64 oldStateAndVersion = stateAndVersion.load();
         u64 state = getState(oldStateAndVersion);
         assert(state>0 && state<=MaxShared);
         if (stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, state-1)))
            return;
      }
   }

   bool tryMark(u64 oldStateAndVersion) {
      assert(getState(oldStateAndVersion)==Unlocked);
      return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
   }

   static u64 getState(u64 v) { return v >> 56; };
   u64 getState() { return getState(stateAndVersion.load()); }

   void operator=(PageState&) = delete;
};

static const u64 pageSize = 4096;

struct alignas(4096) Page {
   u64 nextPage;
};

struct Hashtable {
   static const u64 empty = ~0ull;
   static const u64 tombstone = (~0ull)-1;

   struct Entry {
      u64 pid;
      Page* ptr;
   };

   Entry* ht;
   u64 count;
   u64 mask;
   atomic<u64> clockPos;

   Hashtable(u64 maxCount) : count(next_pow2(maxCount * 1.5)), mask(count - 1), clockPos(0) {
      ht = (Entry*)allocHuge(count * sizeof(Entry));
      memset((void*)ht, 0xFF, count * sizeof(Entry));
   }

   ~Hashtable() {
      munmap(ht, count * sizeof(u64));
   }

   u64 next_pow2(u64 x) {
      return 1<<(64-__builtin_clzl(x-1));
   }

   static u64 hash(u64 k) {
      const u64 m = 0xc6a4a7935bd1e995;
      const int r = 47;
      u64 h = 0x8445d61a4e774912 ^ (8*m);
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
   }

   void insert(u64 pid, Page* ptr) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid;
         assert(curr != pid);
         if ((curr == empty) || (curr == tombstone)) {
            ht[pos].pid = pid;
            ht[pos].ptr = ptr;
            return;
         }
         pos = (pos + 1) & mask;
      }
   }

   Page* lookup(u64 pid) {
      u64 pos = hash(pid) & mask;
      while (true) {
         u64 curr = ht[pos].pid;
         if (curr == pid)
            return ht[pos].ptr;
         if (curr == empty)
            return nullptr;
         pos = (pos + 1) & mask;
      }
   }

};

struct MurmurHash { size_t operator()(const u64& p) const { return Hashtable::hash(p); } };

int main() {
   u64 gb = 1024ull * 1024 * 1024;
   u64 physSize = 4 * gb;
   u64 physCount = physSize / pageSize;
   u64 virtSize = 16 * gb;
   u64 virtCount = virtSize / pageSize;

   vector<u64> accesses(physCount);
   for (u64 i=0; i<physCount; i++)
      accesses[i] = i;
   random_shuffle(accesses.begin(), accesses.end());

   u64 repeat = 10e6;
   PerfEvent e;

   {
      e.setParam("op", "read huge ");
      Page* physMem = (Page*)allocHuge(physSize);
      for (u64 i=0; i<physCount; i++)
         physMem[accesses[i]].nextPage = accesses[(i+1) % physCount];

      {
         PerfEventBlock b(e, repeat);
         u64 pos = 0, sum = 0;
         for (u64 r=0; r<repeat; r++) {
            pos = physMem[pos].nextPage;
            sum += pos;
         }
         assert(sum);
      }
      munmap(physMem, physSize);
   }

   {
      e.setParam("op", "read small");
      Page* physMem = (Page*)allocSmall(physSize);
      for (u64 i=0; i<physCount; i++)
         physMem[accesses[i]].nextPage = accesses[(i+1) % physCount];

      {
         PerfEventBlock b(e, repeat);
         u64 pos = 0, sum = 0;
         for (u64 r=0; r<repeat; r++) {
            pos = physMem[pos].nextPage;
            sum += pos;
         }
         assert(sum);
      }
      munmap(physMem, physSize);
   }

   {
      e.setParam("op", "HT small  ");
      Page* physMem = (Page*)allocSmall(physSize);
      for (u64 i=0; i<physCount; i++)
         physMem[accesses[i]].nextPage = accesses[(i+1) % physCount];

      Hashtable h(physCount);
      for (u64 a : accesses)
         h.insert(a, physMem+a);

      {
         PerfEventBlock b(e, repeat);
         u64 pos = 0, sum = 0;
         for (u64 r=0; r<repeat; r++) {
            Page* p = h.lookup(pos);
            pos = p->nextPage;
            sum += pos;
         }
         assert(sum);
      }

      e.setParam("op", "HT unord  ");
      unordered_map<u64, Page*, MurmurHash > m;
      for (u64 a : accesses)
         m.insert({a, physMem+a});

      {
         PerfEventBlock b(e, repeat);
         u64 pos = 0, sum = 0;
         for (u64 r=0; r<repeat; r++) {
            auto it = m.find(pos);
            assert(it != m.end());
            Page* p = it->second;
            pos = p->nextPage;
            sum += pos;
         }
         assert(sum);
      }

      munmap(physMem, physSize);
   }

   {
      unordered_set<u64> s;
      while (s.size() < physCount)
         s.insert(random() % virtCount);
      accesses.clear();
      for (u64 x : s)
         accesses.push_back(x);
   }

   {
      Page* virtMem = (Page*)allocSmall(virtSize);
      for (u64 i=0; i<physCount; i++)
         virtMem[accesses[i]].nextPage = accesses[(i+1) % physCount];
      PageState* state = (PageState*)allocHuge(virtCount * sizeof(PageState));

      for (u64 i=0; i<virtCount; i++)
         state[i].stateAndVersion = PageState::Evicted;
      for (u64 a : accesses)
         state[a].stateAndVersion = PageState::Unlocked;

      e.setParam("op", "vmcache   ");
      {
         PerfEventBlock b(e, repeat);
         u64 pos = accesses[0], sum = 0;
         for (u64 r=0; r<repeat; r++) {
            pos = virtMem[pos].nextPage;
            sum += pos;
         }
         assert(sum);
      }

      e.setParam("op", "vmc+state ");
      {
         PerfEventBlock b(e, repeat);
         u64 pos = accesses[0], sum = 0;
         for (u64 r=0; r<repeat; r++) {
            while (state[pos].stateAndVersion.load() != PageState::Unlocked)
               ;
            pos = virtMem[pos].nextPage;
            sum += pos;
         }
         assert(sum);
      }

      e.setParam("op", "vmc+optloc");
      {
         PerfEventBlock b(e, repeat);
         u64 pos = accesses[0], sum = 0;
         for (u64 r=0; r<repeat; r++) {
            u64 version;
         restart:
            do {
               version = state[pos].stateAndVersion.load();
            } while(version != PageState::Unlocked);
            pos = virtMem[pos].nextPage;
            if (state[pos].stateAndVersion.load() != version)
               goto restart;
            sum += pos;
         }
         assert(sum);
      }

      e.setParam("op", "vmc+lock  ");
      {
         PerfEventBlock b(e, repeat);
         u64 pos = accesses[0], sum = 0;
         for (u64 r=0; r<repeat; r++) {
            u64 oldPos = pos;
            bool succ = state[pos].tryLockX(state[pos].stateAndVersion.load());
            assert(succ);
            pos = virtMem[pos].nextPage;
            sum += pos;
            state[oldPos].unlockX();
         }
         assert(sum);
      }

      munmap(virtMem, virtSize);
   }

   return 0;
}

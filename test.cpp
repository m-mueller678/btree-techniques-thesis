#include <csignal>
#include <algorithm>
#include <fstream>
#include <string>
#include "PerfEvent.hpp"
#include "btree2020.hpp"

using namespace std;

/*
unsigned countInner(BTreeNode* node)
{
   if (node->isLeaf)
      return 0;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countInner(node->getChild(i));
   sum += countInner(node->upper);
   return sum;
}

unsigned countPages(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   unsigned sum = 1;
   for (unsigned i = 0; i < node->count; i++)
      sum += countPages(node->getChild(i));
   sum += countPages(node->upper);
   return sum;
}

uint64_t bytesFree(BTreeNode* node)
{
   if (node->isLeaf)
      return node->freeSpaceAfterCompaction();
   uint64_t sum = node->freeSpaceAfterCompaction();
   for (unsigned i = 0; i < node->count; i++)
      sum += bytesFree(node->getChild(i));
   sum += bytesFree(node->upper);
   return sum;
}

unsigned height(BTreeNode* node)
{
   if (node->isLeaf)
      return 1;
   return 1 + height(node->upper);
}

void printInfos(BTreeNode* root)
{
   uint64_t cnt = countPages(root);
   uint64_t bytesFr = bytesFree(root);
   cerr << "nodes:" << cnt << " innerNodes:" << countInner(root) << " height:" << height(root) << " rootCnt:" << root->count
        << " bytesFree:" << bytesFr << " fillfactor:" << (1 - (bytesFr / ((double)cnt * pageSize))) << endl;
}

void pr(uint8_t* s, unsigned len) {
   for (unsigned i=0; i<len; i++)
      cout << s[i];
}

void printTree(BTreeNode* node) {
   cout << (node->isLeaf?"L":"I") << " " << node << endl;

   if (node->isLeaf) {
      for (unsigned i=0; i<node->count; i++) {
         cout << i << " " << node->getKey(i) << " ";
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << endl;
      }
      cout << endl;
      return;
   }

   for (unsigned i=0; i<node->count; i++) {
         pr(node->getPrefix(), node->prefixLength);
         cout << " ";
         pr(node->getKey(i), node->getKeyLen(i));
         cout << " " << node->getChild(i);
         cout << endl;
   }
   cout << " " << node->upper;

   for (unsigned i=0; i<node->count; i++)
      printTree(node->getChild(i));
   printTree(node->upper);
}
*/

void runTest(PerfEvent& e, vector<string>& data)
{
   if (getenv("SHUF"))
      random_shuffle(data.begin(), data.end());
   if (getenv("SORT"))
      sort(data.begin(), data.end());

   BTree t;
   uint64_t count = data.size();
   e.setParam("type", "btr");
   e.setParam("factr", "0");
   e.setParam("base", "0");
   {
      // insert
      e.setParam("op", "insert");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));

         //for (uint64_t j=0; j<=i; j+=1) if (!t.lookup((uint8_t*)data[j].data(), data[j].size())) throw;
         //for (uint64_t j=0; j<=i; j++) if (!t.lookup((uint8_t*)data[j].data(), data[j].size()-8)) throw;
      }
      //printInfos(t.root);
      //printTree(t.root);
   }

   {
      // lookup
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for (uint64_t i = 0; i < count; i++) {
         unsigned payloadSize;
         uint8_t* payload = t.lookup((uint8_t*)data[i].data(), data[i].size(), payloadSize);
         if (!payload || (payloadSize != sizeof(uint64_t)) || *reinterpret_cast<uint64_t*>(payload) != i)
            throw;
      }
   }

   // prefix lookup
   for (uint64_t i = 0; i < count; i++)
      t.lookup((uint8_t*)data[i].data(), data[i].size() - (data[i].size() / 4));

   {
      for (uint64_t i = 0; i < count; i += 4) // remove some
         if (!t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count; i++) // lookup all, causes some misses
         if ((i % 4 == 0) == t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // remove some more
         if ((i % 4 == 0) == t.remove((uint8_t*)data[i].data(), data[i].size()))
            throw;
      for (uint64_t i = 0; i < count / 2 + count / 4; i++) // insert
         t.insert((uint8_t*)data[i].data(), data[i].size(), reinterpret_cast<uint8_t*>(&i), sizeof(uint64_t));
      for (uint64_t i = 0; i < count; i++) // remove all
         t.remove((uint8_t*)data[i].data(), data[i].size());
      for (uint64_t i = 0; i < count; i++)
         if (t.lookup((uint8_t*)data[i].data(), data[i].size()))
            throw;
   }
   //printInfos(t.root);

   data.clear();
}

int main(int argc, char** argv)
{
   PerfEvent e;

   vector<string> data;

   if (getenv("INT")) {
      vector<uint64_t> v;
      uint64_t n = atof(getenv("INT"));
      for (uint64_t i = 0; i < n; i++)
         v.push_back(i);
      string s;
      s.resize(4);
      for (auto x : v) {
         *(uint32_t*)(s.data()) = x;
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG1")) {
      uint64_t n = atof(getenv("LONG1"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A');
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("LONG2")) {
      uint64_t n = atof(getenv("LONG2"));
      for (unsigned i = 0; i < n; i++) {
         string s;
         for (unsigned j = 0; j < i; j++)
            s.push_back('A' + random() % 60);
         data.push_back(s);
      }
      runTest(e, data);
   }

   if (getenv("FILE")) {
      ifstream in(getenv("FILE"));
      string line;
      while (getline(in, line))
         data.push_back(line);
      runTest(e, data);
   }

   return 0;
}

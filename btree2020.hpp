#pragma once

struct BTreeNode;

struct BTree {
   private:

   BTreeNode* root;

   void splitNode(BTreeNode* node, BTreeNode* parent, uint8_t* key, unsigned keyLength);
   void ensureSpace(BTreeNode* toSplit, uint8_t* key, unsigned keyLength);

   public:

   BTree();
   ~BTree();

   uint8_t* lookup(uint8_t* key, unsigned keyLength, unsigned& payloadSizeOut);
   bool lookup(uint8_t* key, unsigned keyLength);
   void insert(uint8_t* key, unsigned keyLength, uint8_t* payload, unsigned payloadLength);
   bool remove(uint8_t* key, unsigned keyLength);
};

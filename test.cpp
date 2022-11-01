#include <csignal>
#include <algorithm>
#include <fstream>
#include <string>
#include <cassert>
#include "PerfEvent.hpp"
#include "btree.h"

using namespace std;

void runTest(BenchmarkParameters parameters, vector<string> &data) {
    if (getenv("SHUF")) {
        parameters.setParam("sort", "false");
        random_shuffle(data.begin(), data.end());
    }
    if (getenv("SORT")) {
        parameters.setParam("sort", "true");
        sort(data.begin(), data.end());
    }

    BTree t;
    uint64_t count = data.size();
    {
        // insert
        parameters.setParam("op", "insert");
        PerfEventBlock b(count, parameters);
        for (uint64_t i = 0; i < count; i++) {
            t.insert((uint8_t *) data[i].data(), data[i].size(), reinterpret_cast<uint8_t *>(&i), sizeof(uint64_t));

            //for (uint64_t j=0; j<=i; j+=1) if (!t.lookup((uint8_t*)data[j].data(), data[j].size())) throw;
            //for (uint64_t j=0; j<=i; j++) if (!t.lookup((uint8_t*)data[j].data(), data[j].size()-8)) throw;
        }
    }

    {
        // lookup
        parameters.setParam("op", "lookup");
        PerfEventBlock b(count, parameters);
        for (uint64_t i = 0; i < count; i++) {
            unsigned payloadSize;
            uint8_t *payload = t.lookup((uint8_t *) data[i].data(), data[i].size(), payloadSize);
            if (!payload || (payloadSize != sizeof(uint64_t)) || *reinterpret_cast<uint64_t *>(payload) != i)
                throw;
        }
    }

    return;

    // prefix lookup
    for (uint64_t i = 0; i < count; i++)
        t.lookup((uint8_t *) data[i].data(), data[i].size() - (data[i].size() / 4));

    {
        {
            parameters.setParam("op", "remove");
            PerfEventBlock b(count / 4, parameters);
            for (uint64_t i = 0; i < count; i += 4) // remove some
                if (!t.remove((uint8_t *) data[i].data(), data[i].size()))
                    throw;
        }
        for (uint64_t i = 0; i < count; i++) // lookup all, causes some misses
            if ((i % 4 == 0) == t.lookup((uint8_t *) data[i].data(), data[i].size()))
                throw;
        for (uint64_t i = 0; i < count / 2 + count / 4; i++) // remove some more
            if ((i % 4 == 0) == t.remove((uint8_t *) data[i].data(), data[i].size()))
                throw;
        for (uint64_t i = 0; i < count / 2 + count / 4; i++) // insert
            t.insert((uint8_t *) data[i].data(), data[i].size(), reinterpret_cast<uint8_t *>(&i), sizeof(uint64_t));
        for (uint64_t i = 0; i < count; i++) { // remove all
            bool should = i < count / 2 + count / 4 || i % 4 != 0;
            (void) (should);
            if (should != t.remove((uint8_t *) data[i].data(), data[i].size()))
                throw;
        }
        for (uint64_t i = 0; i < count; i++)
            if (t.lookup((uint8_t *) data[i].data(), data[i].size()))
                throw;
    }

    data.clear();
}

int main() {
    srand(0x1a2b3c4d);
    vector<string> data;
    BenchmarkParameters parameters;

    parameters.setParam("name", getenv("NAME") ? getenv("NAME") : "unnamed");

    if (getenv("INT")) {
        vector<uint64_t> v;
        uint64_t n = atof(getenv("INT"));
        for (uint64_t i = 0; i < n; i++)
            v.push_back(i);
        string s;
        s.resize(4);
        for (auto x: v) {
            *(uint32_t *) (s.data()) = x;
            data.push_back(s);
        }
        parameters.setParam("sort", "?");
        parameters.setParam("bench", string("INT-") + to_string(n));
        runTest(parameters, data);
    }

    if (getenv("LONG1")) {
        uint64_t n = atof(getenv("LONG1"));
        for (unsigned i = 0; i < n; i++) {
            string s;
            for (unsigned j = 0; j < i; j++)
                s.push_back('A');
            data.push_back(s);
        }
        parameters.setParam("sort", "false");
        parameters.setParam("bench", string("LONG1-") + to_string(n));
        runTest(parameters, data);
    }

    if (getenv("LONG2")) {
        uint64_t n = atof(getenv("LONG2"));
        for (unsigned i = 0; i < n; i++) {
            string s;
            for (unsigned j = 0; j < i; j++)
                s.push_back('A' + random() % 60);
            data.push_back(s);
        }
        parameters.setParam("sort", "false");
        parameters.setParam("bench", string("LONG2-") + to_string(n));
        runTest(parameters, data);
    }

    if (getenv("FILE")) {
        ifstream in(getenv("FILE"));
        string line;
        while (getline(in, line))
            data.push_back(line);
        parameters.setParam("sort", "?");
        parameters.setParam("bench", string("FILE"));
        runTest(parameters, data);
    }

    return 0;
}

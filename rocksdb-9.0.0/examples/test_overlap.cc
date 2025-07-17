// rocksdb/examples/test_overlap.cpp

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <iostream>
#include <cassert>

using namespace rocksdb;

std::string kDBPath = "/tmp/overlap_testdb";

void write_range(DB* db, int start, int end) {
    WriteOptions wopt;
    for (int i = start; i <= end; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        db->Put(wopt, key, val);
    }
}

int main() {
    DestroyDB(kDBPath, Options());

    Options options;
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.level0_file_num_compaction_trigger = 10000; // 禁止自动触发

    DB* db;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    write_range(db, 0, 999);
    db->Flush(FlushOptions());

    write_range(db, 500, 1499);
    db->Flush(FlushOptions());

    write_range(db, 750, 1749);
    db->Flush(FlushOptions());

    std::cout << "Inserted 3 batches with increasing overlap." << std::endl;

    std::vector<LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);
    for (const auto& f : files) {
        std::cout << "SST: " << f.name << ", Level: " << f.level
                  << ", Range: [" << f.smallestkey
                  << ", " << f.largestkey << "]" << std::endl;
    }

    delete db;
    return 0;
}

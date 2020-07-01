// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "util/compression.h"

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"
#include "util/simple_rle_compressor.h"

namespace ROCKSDB_NAMESPACE {

class DummyCompressor : public Compressor {
 public:
  DummyCompressor(int id) : name_("DummyCompressor" + std::to_string(id)){};

  const char* Name() const override { return name_.c_str(); }

  Status Compress(const CompressionInfo* info, uint32_t compress_format_version,
                  const char* input, size_t input_length,
                  std::string* output) override {
    (void)info;
    (void)compress_format_version;
    (void)input;
    (void)input_length;
    (void)output;
    return Status::OK();
  }

  Status Uncompress(const UncompressionInfo* info,
                    uint32_t compress_format_version, const char* input,
                    size_t input_length, char** output, size_t* output_length,
                    MemoryAllocator* allocator = nullptr) override {
    (void)info;
    (void)compress_format_version;
    (void)input;
    (void)input_length;
    (void)output;
    (void)output_length;
    (void)allocator;
    return Status::OK();
  }

 private:
  std::string name_;
};

TEST(Compression, SimpleRLECompressor) {
  SimpleRLECompressor* compressor = new SimpleRLECompressor();
  char input[] = "aaaaaaaaaabbbbbbbbbb";
  size_t input_length = 21;
  std::string compressed;
  Status s = compressor->Compress(nullptr, 0, input, input_length, &compressed);
  ASSERT_OK(s);
  ASSERT_STREQ(compressed.c_str(), "~\na~\nb");
  char* decompress_data = nullptr;
  size_t decompress_size = 0;
  s = compressor->Uncompress(nullptr, 0, compressed.c_str(),
                             compressed.length(), &decompress_data,
                             &decompress_size);
  ASSERT_OK(s);
  CacheAllocationPtr original;
  original.reset(decompress_data);
  ASSERT_NE(original, nullptr);
  ASSERT_EQ(decompress_size, input_length);
  ASSERT_STREQ(original.get(), input);
  delete compressor;
}

TEST(Compression, InitializeBuiltInCompressors) {
  auto registry = CompressorRegistry::NewInstance();
  ASSERT_EQ(registry->GetCompressors().size(), 8);
  ASSERT_STREQ(registry->GetCompressor(kSnappyCompression)->Name(),
               SnappyCompressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kZlibCompression)->Name(),
               ZlibCompressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kBZip2Compression)->Name(),
               BZip2Compressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kLZ4Compression)->Name(),
               LZ4Compressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kLZ4HCCompression)->Name(),
               LZ4HCCompressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kXpressCompression)->Name(),
               XpressCompressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kZSTD)->Name(), ZSTDCompressor().Name());
  ASSERT_STREQ(registry->GetCompressor(kZSTDNotFinalCompression)->Name(),
               ZSTDNotFinalCompressor().Name());
  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, InitializeCompressors) {
  auto registry = CompressorRegistry::NewInstance(
      Env::Default(), ".", ".*rocksdb_simple_rle_compressor.*");
  if (!registry->LoadCompressorSupported()) {
    return;
  }
  ASSERT_NE(registry->GetCompressor(SimpleRLECompressor().Name()), nullptr);
  ASSERT_EQ(registry->GetCompressors().size(), 9);  // all compressors
  ASSERT_EQ(registry->GetCompressors(true, false).size(),
            8);  // built-in compressors
  ASSERT_EQ(registry->GetCompressors(false, true).size(),
            1);  // custom compressors
  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, AddCompressor) {
  auto registry = CompressorRegistry::NewInstance();
  std::shared_ptr<SimpleRLECompressor> compressor =
      std::make_shared<SimpleRLECompressor>();
  unsigned char type = registry->AddCompressor(compressor);
  unsigned char expected_type = CompressorRegistry::firstCustomType;
  ASSERT_EQ(type, expected_type);
  ASSERT_EQ(registry->GetCompressor(type)->Name(), compressor->Name());
  ASSERT_NE(registry->GetCompressor(compressor->Name()), nullptr);
  ASSERT_EQ(registry->GetCompressorType(SimpleRLECompressor().Name()),
            expected_type);

  // Adding the compressor again will give the same type
  type = registry->AddCompressor(compressor);
  ASSERT_EQ(type, expected_type);

  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, AddCompressors) {
  // Add compressors until running out of types
  auto registry = CompressorRegistry::NewInstance();
  for (int i = CompressorRegistry::firstCustomType;
       i < CompressorRegistry::maxCompressorType; i++) {
    unsigned char type =
        registry->AddCompressor(std::make_shared<DummyCompressor>(i));
    ASSERT_NE(type, kDisableCompressionOption);
  }
  unsigned char type = registry->AddCompressor(
      std::make_shared<DummyCompressor>(CompressorRegistry::maxCompressorType));
  ASSERT_EQ(type, kDisableCompressionOption);
  ASSERT_EQ(registry->GetCompressors(false).size(),
            CompressorRegistry::maxCompressorType -
                CompressorRegistry::firstCustomType);
  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, AddCompressorWithSpecificType) {
  auto registry = CompressorRegistry::NewInstance();
  std::shared_ptr<SimpleRLECompressor> compressor =
      std::make_shared<SimpleRLECompressor>();
  unsigned char type = registry->AddCompressor(compressor, 1);
  ASSERT_EQ(type, 1);
  ASSERT_EQ(registry->GetCompressor(type)->Name(), compressor->Name());
  ASSERT_NE(registry->GetCompressor(compressor->Name()), nullptr);
  ASSERT_EQ(registry->GetCompressorType(SimpleRLECompressor().Name()), 1);

  // Adding the compressor again with a different type will assign the new type
  // and unassign the previous one
  type = registry->AddCompressor(compressor, 2);
  ASSERT_EQ(type, 2);
  ASSERT_EQ(registry->GetCompressor(1), nullptr);

  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, LoadCompressor) {
  auto registry = CompressorRegistry::NewInstance();
  if (!registry->LoadCompressorSupported()) {
    return;
  }
  std::shared_ptr<Compressor> compressor =
      registry->LoadCompressor("rocksdb_simple_rle_compressor", ".");
  ASSERT_NE(compressor, nullptr);
  ASSERT_STREQ(compressor->Name(), SimpleRLECompressor().Name());

  std::shared_ptr<Compressor> missing_compressor =
      registry->LoadCompressor("rocksdb_missing_compressor", ".");
  ASSERT_EQ(missing_compressor, nullptr);
  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, LoadCompressors) {
  auto registry = CompressorRegistry::NewInstance();
  if (!registry->LoadCompressorSupported()) {
    return;
  }
  std::vector<std::shared_ptr<Compressor>> compressors =
      registry->LoadCompressors(".", ".*rocksdb_simple_rle_compressor.*");
  ASSERT_EQ(compressors.size(), 1);
  std::shared_ptr<Compressor> compressor = compressors[0];
  ASSERT_NE(compressor, nullptr);
  ASSERT_STREQ(compressor->Name(), SimpleRLECompressor().Name());
  CompressorRegistry::ReleaseInstance();
}

TEST(Compression, LoadAndAddCompressors) {
  auto registry = CompressorRegistry::NewInstance();
  if (!registry->LoadCompressorSupported()) {
    return;
  }
  std::vector<unsigned char> types =
      registry->LoadAndAddCompressors(".", ".*rocksdb_simple_rle_compressor.*");
  ASSERT_EQ(types.size(), 1);
  unsigned char expected_type = CompressorRegistry::firstCustomType;
  ASSERT_EQ(types[0], expected_type);
  ASSERT_NE(registry->GetCompressor(types[0]), nullptr);
  ASSERT_STREQ(registry->GetCompressor(types[0])->Name(),
               SimpleRLECompressor().Name());
  CompressorRegistry::ReleaseInstance();
}

#ifndef ROCKSDB_LITE
TEST(Compression, ColumnFamilyOptionsFromString) {
  ColumnFamilyOptions options, new_options;
  ConfigOptions config_options;

  // Custom compressor not loaded
  Status s = GetColumnFamilyOptionsFromString(
      config_options, options, "compression=SimpleRLECompressor;",
      &new_options);
  ASSERT_NOK(s);
  ASSERT_EQ(s.ToString(), "Invalid argument: Error parsing:: compression");
  CompressorRegistry::ReleaseInstance();

  auto registry = CompressorRegistry::NewInstance();
  std::shared_ptr<SimpleRLECompressor> compressor =
      std::make_shared<SimpleRLECompressor>();
  registry->AddCompressor(compressor);
  s = GetColumnFamilyOptionsFromString(config_options, options,
                                       "compression=SimpleRLECompressor;",
                                       &new_options);
  ASSERT_OK(s);
  unsigned char expected_type = CompressorRegistry::firstCustomType;
  ASSERT_EQ(new_options.compression, expected_type);
  CompressorRegistry::ReleaseInstance();
}
#endif

TEST(Compression, SimpleRLECompressorDB) {
  // Create database
  Options options;
  std::string dbname = test::PerThreadDBPath("compression_test");
  ASSERT_OK(DestroyDB(dbname, options));

  options.create_if_missing = true;
  std::shared_ptr<SimpleRLECompressor> compressor =
      std::make_shared<SimpleRLECompressor>();
  auto registry = CompressorRegistry::NewInstance();
  unsigned char type = registry->AddCompressor(compressor);
  options.compression = static_cast<CompressionType>(type);

  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);
  ASSERT_EQ(compressor->num_compress_calls, 0);
  ASSERT_EQ(compressor->num_uncompress_calls, 0);

  // Write 200 values, each 20 bytes
  int num_keys = 200;
  WriteOptions wo;
  std::string val = "aaaaaaaaaabbbbbbbbbb";
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Put(wo, Slice(key), Slice(val));
    ASSERT_OK(s);
  }
  ASSERT_OK(db->Flush(FlushOptions()));  // Flush all data from memtable so that
                                         // an SST file is written
  ASSERT_EQ(compressor->num_compress_calls, 3);

  // Read and verify
  ReadOptions ro;
  std::string result;
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  // index block not compressed, as not passing GoodCompressionRatio test
  ASSERT_EQ(compressor->num_uncompress_calls, 2);

#ifndef ROCKSDB_LITE
  // Verify options file
  DBOptions db_options;
  ConfigOptions config_options;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  LoadLatestOptions(config_options, db->GetName(), &db_options, &cf_descs);
  ASSERT_EQ(cf_descs[0].options.compression, type);
#endif

  // Close database
  s = db->Close();
  ASSERT_OK(s);
  delete db;

  // Reopen the database
  // Create new instance of compressor to reset call counters
  Options reopen_options;
  compressor = std::make_shared<SimpleRLECompressor>();
  registry->AddCompressor(compressor);

  db = nullptr;
  s = DB::Open(reopen_options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);

#ifndef ROCKSDB_LITE
  // Verify table properties
  TablePropertiesCollection all_tables_props;
  db->GetPropertiesOfAllTables(&all_tables_props);
  for (auto it = all_tables_props.begin(); it != all_tables_props.end(); ++it)
    ASSERT_STREQ(it->second->compression_name.c_str(), compressor->Name());
#endif

  // Read and verify
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  ASSERT_EQ(compressor->num_compress_calls, 0);
  ASSERT_EQ(compressor->num_uncompress_calls, 2);

  s = db->Close();
  ASSERT_OK(s);
  delete db;
  ASSERT_OK(DestroyDB(dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

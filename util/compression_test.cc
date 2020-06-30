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

namespace ROCKSDB_NAMESPACE {

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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

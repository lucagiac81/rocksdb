// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once
#include "rocksdb/compressor.h"

namespace ROCKSDB_NAMESPACE {

// Simple RLE compressor for testing purposes.
// It needs to compress enough to pass GoodCompressionRatio check.
class SimpleRLECompressor : public Compressor {
 public:
  const char* Name() const override { return "SimpleRLECompressor"; }

  Status Compress(const CompressionInfo* info, uint32_t compress_format_version,
                  const char* input, size_t input_length,
                  std::string* output) override;

  Status Uncompress(const UncompressionInfo* info,
                    uint32_t compress_format_version, const char* input,
                    size_t input_length, char** output, size_t* output_length,
                    MemoryAllocator* allocator = nullptr) override;

  int num_compress_calls = 0;
  int num_uncompress_calls = 0;

 private:
  const char delim_ = '~';

  void outputSeq(char last, char seq, std::string* output);
};

}  // namespace ROCKSDB_NAMESPACE

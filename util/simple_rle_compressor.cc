// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "util/simple_rle_compressor.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace ROCKSDB_NAMESPACE {

Status SimpleRLECompressor::Compress(const CompressionInfo* info,
                                     uint32_t compress_format_version,
                                     const char* input, size_t input_length,
                                     std::string* output) {
  (void)info;
  (void)compress_format_version;

  output->clear();
  char last = input[0];
  char seq = 0;
  for (size_t i = 0; i < input_length; i++) {
    if (input[i] == last && seq < delim_ - 1) {
      seq++;
    } else {
      outputSeq(last, seq, output);
      seq = 1;
    }
    last = input[i];
  }
  outputSeq(last, seq, output);

  num_compress_calls++;
  return Status::OK();
}

Status SimpleRLECompressor::Uncompress(const UncompressionInfo* info,
                                       uint32_t compress_format_version,
                                       const char* input, size_t input_length,
                                       char** output, size_t* output_length,
                                       MemoryAllocator* allocator) {
  (void)info;
  (void)compress_format_version;

  std::string uncompressed;
  size_t i = 0;
  while (i < input_length) {
    if (i < input_length - 1 && input[i] == delim_ && input[i + 1] == delim_) {
      uncompressed += delim_;
      i += 2;
    } else if (i < input_length - 2 && input[i] == delim_) {
      uncompressed.append(input[i + 1], input[i + 2]);
      i += 3;
    } else {
      uncompressed += input[i];
      i++;
    }
  }

  *output = Allocate(uncompressed.length(), allocator);
  if (!*output) {
    return Status::MemoryLimit();
  }
  memcpy(*output, uncompressed.c_str(), uncompressed.length());
  *output_length = uncompressed.length();
  num_uncompress_calls++;
  return Status::OK();
}

void SimpleRLECompressor::outputSeq(char last, char seq, std::string* output) {
  if (last != delim_) {
    if (seq >= 4) {
      *output += delim_;
      *output += seq;
      *output += last;
    } else {
      output->append(seq, last);
    }
  } else {
    if (seq >= 2) {
      *output += delim_;
      *output += seq;
      *output += last;
    } else {
      output->append(seq * 2, last);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

extern "C" ROCKSDB_NAMESPACE::Compressor* CreateCompressor() {
  return new ROCKSDB_NAMESPACE::SimpleRLECompressor();
}

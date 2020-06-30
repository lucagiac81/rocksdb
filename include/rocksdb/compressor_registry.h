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
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// CompressorRegistry holds instances of all available compressors.
class CompressorRegistry {
 public:
  // Constructor used in NewInstance.
  CompressorRegistry();

  // Get an instance of CompressorRegistry (singleton).
  static std::shared_ptr<CompressorRegistry> NewInstance();

  static void ReleaseInstance();

  // Get a compressor by the associated numeric type.
  // @param type Numeric type of the compressor in the registry.
  std::shared_ptr<Compressor> GetCompressor(unsigned char type);

  // Get a compressor by its name.
  // @param name Name of the compressor (as returned by its Name() method).
  std::shared_ptr<Compressor> GetCompressor(const std::string& name);

  // Get compressors currently in the registry.
  std::vector<std::shared_ptr<Compressor>> GetCompressors();

  // Get the numeric type associated to the compressor with the specified name.
  // @param name Name of the compressor (as returned by its Name() method).
  unsigned char GetCompressorType(const std::string& name);

  // Max type that a compressor can have.
  static const unsigned char maxCompressorType = 0xfe;

 private:
  // Initialize compressors and add them to the registry.
  void InitializeBuiltInCompressors();

  // Singleton instance of CompressorRegistry.
  static std::shared_ptr<CompressorRegistry> instance;

  // Array of compressor instances, indexing by their numeric types.
  std::shared_ptr<Compressor> compressors[maxCompressorType + 1] = {nullptr};
};

}  // namespace ROCKSDB_NAMESPACE

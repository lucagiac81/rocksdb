// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "rocksdb/compressor_registry.h"

#include "rocksdb/file_system.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

// to avoid undefined reference error with CMake
const unsigned char CompressorRegistry::maxCompressorType;
std::shared_ptr<CompressorRegistry> CompressorRegistry::instance = nullptr;

CompressorRegistry::CompressorRegistry() { InitializeBuiltInCompressors(); }

std::shared_ptr<CompressorRegistry> CompressorRegistry::NewInstance() {
  if (instance == nullptr) {
    instance = std::make_shared<CompressorRegistry>();
  }
  return instance;
}

void CompressorRegistry::ReleaseInstance() {
  if (instance != nullptr) {
    instance.reset();
  }
}

std::shared_ptr<Compressor> CompressorRegistry::GetCompressor(
    unsigned char type) {
  return compressors[type];
}

std::shared_ptr<Compressor> CompressorRegistry::GetCompressor(
    const std::string& name) {
  for (int i = 0; i <= maxCompressorType; i++) {
    if (compressors[i] != nullptr && name == compressors[i]->Name()) {
      return compressors[i];
    }
  }
  return nullptr;
}

std::vector<std::shared_ptr<Compressor>> CompressorRegistry::GetCompressors() {
  std::vector<std::shared_ptr<Compressor>> available_compressors;
  for (int i = 0; i <= maxCompressorType; i++) {
    if (compressors[i] != nullptr) {
      available_compressors.push_back(compressors[i]);
    }
  }
  return available_compressors;
}

unsigned char CompressorRegistry::GetCompressorType(const std::string& name) {
  if (name == "NoCompression") return kNoCompression;
  if (name == "DisableOption") return kDisableCompressionOption;

  for (unsigned char i = 0; i <= maxCompressorType; i++) {
    if (compressors[i] != nullptr && name == compressors[i]->Name()) {
      return i;
    }
  }
  return kDisableCompressionOption;
}

void CompressorRegistry::InitializeBuiltInCompressors() {
  compressors[kSnappyCompression] = std::make_shared<SnappyCompressor>();
  compressors[kZlibCompression] = std::make_shared<ZlibCompressor>();
  compressors[kBZip2Compression] = std::make_shared<BZip2Compressor>();
  compressors[kLZ4Compression] = std::make_shared<LZ4Compressor>();
  compressors[kLZ4HCCompression] = std::make_shared<LZ4HCCompressor>();
  compressors[kXpressCompression] = std::make_shared<XpressCompressor>();
  compressors[kZSTD] = std::make_shared<ZSTDCompressor>();
  compressors[kZSTDNotFinalCompression] =
      std::make_shared<ZSTDNotFinalCompressor>();
}

}  // namespace ROCKSDB_NAMESPACE

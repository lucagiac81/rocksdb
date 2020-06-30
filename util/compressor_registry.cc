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

std::vector<std::shared_ptr<Compressor>> CompressorRegistry::GetCompressors(
    bool include_builtin, bool include_custom) {
  std::vector<std::shared_ptr<Compressor>> available_compressors;
  for (int i = 0; i <= maxCompressorType; i++) {
    if (compressors[i] != nullptr) {
      if (builtin[i] && include_builtin) {
        available_compressors.push_back(compressors[i]);
      } else if (!builtin[i] && include_custom) {
        available_compressors.push_back(compressors[i]);
      }
    }
  }
  return available_compressors;
}

std::vector<unsigned char> CompressorRegistry::GetCompressorTypes(
    bool include_builtin, bool include_custom) {
  std::vector<unsigned char> available_compressors;
  for (unsigned char i = 0; i <= maxCompressorType; i++) {
    if (compressors[i] != nullptr) {
      if (builtin[i] && include_builtin) {
        available_compressors.push_back(i);
      } else if (!builtin[i] && include_custom) {
        available_compressors.push_back(i);
      }
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

CompressionType CompressorRegistry::GetCustomCompressorType(
    unsigned char type) {
  assert(type != kCustomCompression);
  // If it's a built-in compressor, just return its type
  if (builtin[type] || type == kNoCompression ||
      type == kDisableCompressionOption)
    return static_cast<CompressionType>(type);
  else
    return kCustomCompression;
}

void CompressorRegistry::InitializeBuiltInCompressors() {
  // If the user decided to override the compressor for a supported algorithm,
  // preserve the compressor the user provided.
  if (compressors[kSnappyCompression] == nullptr) {
    compressors[kSnappyCompression] = std::make_shared<SnappyCompressor>();
    builtin[kSnappyCompression] = true;
  }
  if (compressors[kZlibCompression] == nullptr) {
    compressors[kZlibCompression] = std::make_shared<ZlibCompressor>();
    builtin[kZlibCompression] = true;
  }
  if (compressors[kBZip2Compression] == nullptr) {
    compressors[kBZip2Compression] = std::make_shared<BZip2Compressor>();
    builtin[kBZip2Compression] = true;
  }
  if (compressors[kLZ4Compression] == nullptr) {
    compressors[kLZ4Compression] = std::make_shared<LZ4Compressor>();
    builtin[kLZ4Compression] = true;
  }
  if (compressors[kLZ4HCCompression] == nullptr) {
    compressors[kLZ4HCCompression] = std::make_shared<LZ4HCCompressor>();
    builtin[kLZ4HCCompression] = true;
  }
  if (compressors[kXpressCompression] == nullptr) {
    compressors[kXpressCompression] = std::make_shared<XpressCompressor>();
    builtin[kXpressCompression] = true;
  }
  if (compressors[kZSTD] == nullptr) {
    compressors[kZSTD] = std::make_shared<ZSTDCompressor>();
    builtin[kZSTD] = true;
  }
  if (compressors[kZSTDNotFinalCompression] == nullptr) {
    compressors[kZSTDNotFinalCompression] =
        std::make_shared<ZSTDNotFinalCompressor>();
    builtin[kZSTDNotFinalCompression] = true;
  }
}

unsigned char CompressorRegistry::AddCompressor(
    std::shared_ptr<Compressor> compressor, unsigned char type) {
  // Check if the type matches reserved values
  if (type == kNoCompression || type == kCustomCompression ||
      type == kDisableCompressionOption) {
    return kDisableCompressionOption;
  }
  // If a compressor with the same name already exists with a different type,
  // unassign previous type
  unsigned char prev_type = GetCompressorType(compressor->Name());
  if (prev_type != kDisableCompressionOption && prev_type != type) {
    compressors[prev_type].reset();
  }
  compressors[type] = compressor;
  return type;
}

unsigned char CompressorRegistry::AddCompressor(
    std::shared_ptr<Compressor> compressor) {
  // If a compressor with the same name already exists, overwrite and keep the
  // same type Otherwise, assign new type
  unsigned char type = kDisableCompressionOption;
  type = GetCompressorType(compressor->Name());
  if (type == kDisableCompressionOption) {
    for (unsigned char i = firstCustomType; i <= maxCompressorType; i++) {
      if (i == kNoCompression || i == kCustomCompression ||
          i == kDisableCompressionOption || compressors[i] != nullptr) {
        continue;
      } else {
        type = i;
        break;
      }
    }
  }

  if (type != kDisableCompressionOption) {
    compressors[type] = compressor;
    return type;
  } else {
    return kDisableCompressionOption;
  }
}

}  // namespace ROCKSDB_NAMESPACE

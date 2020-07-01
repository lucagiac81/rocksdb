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
// There are two categories of compressors
// - built-in: implemented in RocksDB
// - custom: provided by the user
// Built-in compressors are added to CompressorRegistry upon instatiation.
// CompressorRegistry assigns a numeric type to each compressor
// - for built-in compressors, the type follows the CompressionType enum
// - for custom compressors, the type is assigned dynamically
class CompressorRegistry {
 public:
  // Constructor used in NewInstance.
  CompressorRegistry(Env* env, const std::string& lib_path,
                     const std::string& lib_filter);

  // Get an instance of CompressorRegistry (singleton).
  // @param env Environment for dynamic loading.
  // @param lib_path Path to directory containing compressors in shared
  // libraries. Those compressors are loaded upon instantiation of
  // CompressorRegistry.
  // @param lib_filter Regex to filter file names in the lib_path directory.
  // Only file whose names match the regex are considered for loading.
  static std::shared_ptr<CompressorRegistry> NewInstance(
      Env* env = Env::Default(), std::string lib_path = "./compressors",
      const std::string& lib_filter = ".*");

  static void ReleaseInstance();

  // Get a compressor by the associated numeric type.
  // @param type Numeric type of the compressor in the registry.
  std::shared_ptr<Compressor> GetCompressor(unsigned char type);

  // Get a compressor by its name.
  // @param name Name of the compressor (as returned by its Name() method).
  std::shared_ptr<Compressor> GetCompressor(const std::string& name);

  // Get compressors currently in the registry.
  // @param include_builtin If true, the compressors provided directly by
  // RocksDB are included in the returned vector.
  // @param include_custom If true, custom compressors are included in the
  // returned vector.
  std::vector<std::shared_ptr<Compressor>> GetCompressors(
      bool include_builtin = true, bool include_custom = true);

  // Get the types of the compressors currently in the registry.
  // @see CompressorRegistry::GetCompressors for more details.
  std::vector<unsigned char> GetCompressorTypes(bool include_builtin = true,
                                                bool include_custom = true);

  // Get the numeric type associated to the compressor with the specified name.
  // @param name Name of the compressor (as returned by its Name() method).
  unsigned char GetCompressorType(const std::string& name);

  // For built-in compressors, return their type.
  // For custom compressors, return kCustomCompression.
  // @param type Numeric type of the compressor in the registry.
  CompressionType GetCustomCompressorType(unsigned char type);

  // Add a compressor to the registry and let the registry assign a numeric type
  // to it.
  // @param compressor Compressor to be added.
  // Returns the assigned type, or kDisableCompressionOption in case of
  // error.
  unsigned char AddCompressor(std::shared_ptr<Compressor> compressor);

  // Add a compressor to the registry with a specified type. It can be used to
  // override built-in compressors.
  // @param compressor Compressor to be added.
  // @param type Numeric type to be assigned to the compressor.
  // Returns the assigned type (same as provided parameter), or
  // kDisableCompressionOption in case of error.
  unsigned char AddCompressor(std::shared_ptr<Compressor> compressor,
                              unsigned char type);

  // Load a compressor from a shared library.
  // @param lib_name Library path
  // @param lib_path Library path
  std::shared_ptr<Compressor> LoadCompressor(const std::string& lib_name,
                                             const std::string& lib_path);

  // Load compressors from shared libraries in a directory.
  // @param lib_path Directory path.
  // @param lib_filter Regex to filter file names in the directory. Only file
  // whose names match the regex are considered for loading.
  std::vector<std::shared_ptr<Compressor>> LoadCompressors(
      const std::string& lib_path, const std::string& lib_filter = ".*");

  // Load compressor from a shared library and add it to the registry.
  // @see CompressorRegistry::LoadCompressor for more details.
  unsigned char LoadAndAddCompressor(const std::string& lib_name,
                                     const std::string& lib_path);

  // Load compressors from shared libraries in a directory and add them to the
  // registry.
  // @see CompressorRegistry::LoadCompressors for more details.
  std::vector<unsigned char> LoadAndAddCompressors(
      const std::string& lib_path, const std::string& lib_filter = ".*");

  // Returns whether the environment supports dynamic loading.
  bool LoadCompressorSupported();

  // Max type that a compressor can have.
  static const unsigned char maxCompressorType = 0xfe;
  // First type that can be assigned to custom compressor.
  static const unsigned char firstCustomType = 0x41;

 private:
  // Initialize built-in and custom compressors (from lib_path_ directory) and
  // add them to the registry.
  void InitializeCompressors();

  // Part of InitializeCompressors to initialize built-in compressors and add
  // them to the registry.
  void InitializeBuiltInCompressors();

  // Singleton instance of CompressorRegistry.
  static std::shared_ptr<CompressorRegistry> instance;

  // Environment for dynamic loading.
  // @see CompressorRegistry::NewInstance for more details.
  Env* env_;

  // Path to directory containing compressors in shared libraries.
  // @see CompressorRegistry::NewInstance for more details.
  std::string lib_path_;

  // Regex to filter file names in the lib_path directory.
  // @see CompressorRegistry::NewInstance for more details.
  std::string lib_filter_;

  // Dynamic_libraries must be destroyed after compressors.
  std::vector<std::shared_ptr<DynamicLibrary>> dynamic_libraries;

  // Array of compressor instances, indexing by their numeric types.
  std::shared_ptr<Compressor> compressors[maxCompressorType + 1] = {nullptr};

  // Whether a compressor is built-in or custom.
  bool builtin[maxCompressorType + 1] = {false};
};

}  // namespace ROCKSDB_NAMESPACE

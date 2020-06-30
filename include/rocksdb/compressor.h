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

#include "memory/memory_allocator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
class Compressor;
// Holds dictionary and related data, like ZSTD's digested compression
// dictionary.
class CompressionDict {
 private:
  // Raw dictionary
  std::string dict_;

  // Processed dictionary, if the compressor supports the functionality, nullptr
  // otherwise.
  // The underlying format is compressor-specific. A Compressor is
  // responsible for allocating the underlying memory in the
  // ProcessCompressionDict method and deallocating it in the
  // DestroyCompressionDict method.
  // ZSTD digested dictionary is an example of processed dictionary.
  void* processed_dict_ = nullptr;

  // Compressor for the selected compression type
  std::shared_ptr<Compressor> compressor_ = nullptr;

 public:
  CompressionDict(const std::string& dict, CompressionType type, int level);
  ~CompressionDict();
  Slice GetRawDict() const { return dict_; }
  void* GetProcessedDict() const { return processed_dict_; }
  static const CompressionDict& GetEmptyDict() {
    static CompressionDict empty_dict{};
    return empty_dict;
  }

  CompressionDict() = default;
  // Disable copy/move
  CompressionDict(const CompressionDict&) = delete;
  CompressionDict& operator=(const CompressionDict&) = delete;
  CompressionDict(CompressionDict&&) = delete;
  CompressionDict& operator=(CompressionDict&&) = delete;
};

// Holds dictionary and related data, like ZSTD's digested uncompression
// dictionary.
class UncompressionDict {
 private:
  // Block containing the data for the compression dictionary in case the
  // constructor that takes a string parameter is used.
  std::string dict_;

  // Block containing the data for the compression dictionary in case the
  // constructor that takes a Slice parameter is used and the passed in
  // CacheAllocationPtr is not nullptr.
  CacheAllocationPtr allocation_;

  // Slice pointing to the compression dictionary data. Can point to
  // dict_, allocation_, or some other memory location, depending on how
  // the object was constructed.
  Slice slice_;

  // Processed dictionary, if the compressor supports the functionality, nullptr
  // otherwise.
  // The underlying format is compressor-specific. A Compressor is
  // responsible for allocating the underlying memory in the
  // ProcessUncompressionDict method and deallocating it in the
  // DestroyUncompressionDict method.
  // ZSTD digested dictionary is an example of processed dictionary.
  void* processed_dict_ = nullptr;

  // Compressor for the selected compression type
  std::shared_ptr<Compressor> compressor_ = nullptr;

 public:
  UncompressionDict(const std::string& dict, CompressionType type);
  UncompressionDict(const Slice& slice, CacheAllocationPtr&& allocation,
                    CompressionType type);
  UncompressionDict(UncompressionDict&& rhs);
  ~UncompressionDict();
  UncompressionDict& operator=(UncompressionDict&& rhs);

  // The object is self-contained if the string constructor is used, or the
  // Slice constructor is invoked with a non-null allocation. Otherwise, it
  // is the caller's responsibility to ensure that the underlying storage
  // outlives this object.
  bool own_bytes() const { return !dict_.empty() || allocation_; }
  const Slice& GetRawDict() const { return slice_; }
  const void* GetProcessedDict() const { return processed_dict_; }
  static const UncompressionDict& GetEmptyDict() {
    static UncompressionDict empty_dict{};
    return empty_dict;
  }
  size_t ApproximateMemoryUsage() const;

  UncompressionDict() = default;
  // Disable copy
  UncompressionDict(const CompressionDict&) = delete;
  UncompressionDict& operator=(const CompressionDict&) = delete;
};

class CompressionContext {
 private:
  // Compressor-specific context, if the compressor supports the functionality,
  // nullptr otherwise. A Compressor is responsible for allocating the
  // underlying memory in the CreateCompressionContext method and deallocating
  // it in the DestroyCompressionContext method.
  void* context_ = nullptr;

  // Compressor for the selected compression type
  std::shared_ptr<Compressor> compressor_ = nullptr;

 public:
  explicit CompressionContext(CompressionType type);
  ~CompressionContext();
  CompressionContext(const CompressionContext&) = delete;
  CompressionContext& operator=(const CompressionContext&) = delete;
  void* GetContext() const { return context_; };
};

class CompressionInfo {
  const CompressionOptions& opts_;
  const CompressionContext& context_;
  const CompressionDict& dict_;
  const CompressionType type_;
  const uint64_t sample_for_compression_;

 public:
  CompressionInfo(const CompressionOptions& _opts,
                  const CompressionContext& _context,
                  const CompressionDict& _dict, CompressionType _type,
                  uint64_t _sample_for_compression)
      : opts_(_opts),
        context_(_context),
        dict_(_dict),
        type_(_type),
        sample_for_compression_(_sample_for_compression) {}

  const CompressionOptions& options() const { return opts_; }
  const CompressionContext& context() const { return context_; }
  const CompressionDict& dict() const { return dict_; }
  CompressionType type() const { return type_; }
  uint64_t SampleForCompression() const { return sample_for_compression_; }
};

class UncompressionContext {
 private:
  // Compressor-specific context, if the compressor supports the functionality,
  // nullptr otherwise. A Compressor is responsible for allocating the
  // underlying memory in the CreateUncompressionContext method and deallocating
  // it in the DestroyUncompressionContext method.
  void* context_ = nullptr;

  // Compressor for the selected compression type
  std::shared_ptr<Compressor> compressor_ = nullptr;

 public:
  explicit UncompressionContext(CompressionType type);
  ~UncompressionContext();
  UncompressionContext(const UncompressionContext&) = delete;
  UncompressionContext& operator=(const UncompressionContext&) = delete;
  void* GetContext() const { return context_; };
};

class UncompressionInfo {
  const UncompressionContext& context_;
  const UncompressionDict& dict_;
  const CompressionType type_;

 public:
  UncompressionInfo(const UncompressionContext& _context,
                    const UncompressionDict& _dict, CompressionType _type)
      : context_(_context), dict_(_dict), type_(_type) {}

  const UncompressionContext& context() const { return context_; }
  const UncompressionDict& dict() const { return dict_; }
  CompressionType type() const { return type_; }
};

// Interface for each compression algorithm to implement.
class Compressor {
 public:
  virtual ~Compressor() = default;

  // Unique name for the compressor.
  // The name is used to specify the compressor in the options string and for
  // querying CompressorRegistry.
  virtual const char* Name() const = 0;

  // Whether the compressor is supported.
  // For example, a compressor can implement this method to verify its
  // dependencies or environment settings.
  virtual bool Supported() const { return true; }

  // Whether the compressor supports dictionary compression.
  virtual bool DictCompressionSupported() const { return false; }

  // Compress data.
  // @param info Pointer to CompressionInfo object (containing context,
  // dictionary, options).
  // @param compress_format_version Compressed block format.
  // @param input Buffer containing data to compress.
  // @param input_length Length of the input data.
  // @param output Compressed data.
  // Returns OK if compression completed correctly.
  // Returns other status in case of error (e.g., Corruption).
  virtual Status Compress(const CompressionInfo* info,
                          uint32_t compress_format_version, const char* input,
                          size_t input_length, std::string* output) = 0;

  // Uncompress data.
  // @param info Pointer to UnompressionInfo object (containing context,
  // dictionary).
  // @param compress_format_version Compressed block format.
  // @param input Buffer containing data to uncompress.
  // @param input_length Length of the input data.
  // @param output Buffer containing uncompressed data.
  // @param output_length Length of the output data.
  // @param allocator MemoryAllocator to allocate output buffer.
  // Returns OK if uncompression completed correctly.
  // Returns other status in case of error (e.g., Corruption).
  virtual Status Uncompress(const UncompressionInfo* info,
                            uint32_t compress_format_version, const char* input,
                            size_t input_length, char** output,
                            size_t* output_length,
                            MemoryAllocator* allocator = nullptr) = 0;

  // Process the raw compression dictionary.
  // The format of the processed dictionary is compressor-specific.
  // The method is responsible for allocating the memory for the processed
  // dictionary. If the compressor does not support processing the dictionary,
  // use the default implementation of the method.
  // @param dict Raw dictionary.
  // @param level Compression level.
  // Returns a pointer to the processed dictionary.
  virtual void* ProcessCompressionDict(std::string& dict, int level) {
    (void)dict;
    (void)level;
    return nullptr;
  }

  // Destroy the processed compression dictionary.
  // Any memory allocated in ProcessCompressionDict should be freed here.
  // If the compressor does not support processing the dictionary, use the
  // default implementation of the method.
  // @param processed_dict Pointer to processed dictionary.
  virtual void DestroyCompressionDict(void* processed_dict) {
    (void)processed_dict;
  }

  // Similar to ProcessCompressionDict, but for the uncompression dictionary.
  // @see Compressor:ProcessCompressionDict for more details.
  virtual void* ProcessUncompressionDict(Slice& dict) {
    (void)dict;
    return nullptr;
  }

  // Get the memory usage for the processed uncompression dictionary.
  // @param processed_dict Pointer to processed dictionary.
  virtual size_t GetUncompressionDictMemoryUsage(void* processed_dict) {
    (void)processed_dict;
    return 0;
  }

  // Similar to DestroyCompressionDict, but for the uncompression dictionary.
  // @see Compressor:DestroyCompressionDict for more details.
  virtual void DestroyUncompressionDict(void* processed_dict) {
    (void)processed_dict;
  }

  // Prepare any compressor-specific context needed for compression.
  // The method is responsible for allocating the memory for the context.
  // If the compressor does not require a context, use the default
  // implementation of the method. Returns a pointer to the context.
  virtual void* CreateCompressionContext() { return nullptr; }

  // Destroy the compression context.
  // Any memory allocated in CreateCompressionContext should be freed here.
  // If the compressor does not require a context, use the default
  // implementation of the method.
  // @param context Pointer to the context.
  virtual void DestroyCompressionContext(void* context) { (void)context; }

  // Similar to CreateCompressionContext, but for uncompression.
  // @see Compressor:CreateCompressionContext for more details.
  virtual void* CreateUncompressionContext() { return nullptr; }

  // Similar to DestroyCompressionContext, but for uncompression.
  // @see Compressor:DestroyCompressionContext for more details.
  virtual void DestroyUncompressionContext(void* context) { (void)context; }
};

}  // namespace ROCKSDB_NAMESPACE

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

#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

Status BZip2Compressor::Compress(const CompressionInfo* info,
                                 uint32_t compress_format_version,
                                 const char* input, size_t input_length,
                                 std::string* output) {
  bool success = BZip2_Compress(*info, compress_format_version, input,
                                input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status BZip2Compressor::Uncompress(const UncompressionInfo* /*info*/,
                                   uint32_t compress_format_version,
                                   const char* input, size_t input_length,
                                   char** output, size_t* output_length,
                                   MemoryAllocator* allocator) {
  *output = BZip2_Uncompress(input, input_length, output_length,
                             compress_format_version, allocator);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

bool LZ4Compressor::DictCompressionSupported() const {
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  return true;
#else
  return false;
#endif
}

Status LZ4Compressor::Compress(const CompressionInfo* info,
                               uint32_t compress_format_version,
                               const char* input, size_t input_length,
                               std::string* output) {
  bool success =
      LZ4_Compress(*info, compress_format_version, input, input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status LZ4Compressor::Uncompress(const UncompressionInfo* info,
                                 uint32_t compress_format_version,
                                 const char* input, size_t input_length,
                                 char** output, size_t* output_length,
                                 MemoryAllocator* allocator) {
  *output = LZ4_Uncompress(*info, input, input_length, output_length,
                           compress_format_version, allocator);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status LZ4HCCompressor::Compress(const CompressionInfo* info,
                                 uint32_t compress_format_version,
                                 const char* input, size_t input_length,
                                 std::string* output) {
  bool success = LZ4HC_Compress(*info, compress_format_version, input,
                                input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status SnappyCompressor::Compress(const CompressionInfo* info,
                                  uint32_t /*compress_format_version*/,
                                  const char* input, size_t input_length,
                                  std::string* output) {
  bool success = Snappy_Compress(*info, input, input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status SnappyCompressor::Uncompress(const UncompressionInfo* /*info*/,
                                    uint32_t /*compress_format_version*/,
                                    const char* input, size_t input_length,
                                    char** output, size_t* output_length,
                                    MemoryAllocator* allocator) {
  *output = Snappy_Uncompress(input, input_length, output_length, allocator);
  if (!*output) {
    return Status::Corruption();
  } else {
    return Status::OK();
  }
}

Status XpressCompressor::Compress(const CompressionInfo* /*info*/,
                                  uint32_t /*compress_format_version*/,
                                  const char* input, size_t input_length,
                                  std::string* output) {
  bool success = XPRESS_Compress(input, input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status XpressCompressor::Uncompress(const UncompressionInfo* /*info*/,
                                    uint32_t /*compress_format_version*/,
                                    const char* input, size_t input_length,
                                    char** output, size_t* output_length,
                                    MemoryAllocator* /*allocator*/) {
  // XPRESS allocates memory internally, thus no support for custom allocator.
  *output = XPRESS_Uncompress(input, input_length, output_length);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status ZlibCompressor::Compress(const CompressionInfo* info,
                                uint32_t compress_format_version,
                                const char* input, size_t input_length,
                                std::string* output) {
  bool success = Zlib_Compress(*info, compress_format_version, input,
                               input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status ZlibCompressor::Uncompress(const UncompressionInfo* info,
                                  uint32_t compress_format_version,
                                  const char* input, size_t input_length,
                                  char** output, size_t* output_length,
                                  MemoryAllocator* allocator) {
  *output = Zlib_Uncompress(*info, input, input_length, output_length,
                            compress_format_version, allocator);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

bool ZSTDCompressor::DictCompressionSupported() const {
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  return true;
#else
  return false;
#endif
}

Status ZSTDCompressor::Compress(const CompressionInfo* info,
                                uint32_t /*compress_format_version*/,
                                const char* input, size_t input_length,
                                std::string* output) {
  bool success = ZSTD_Compress(*info, input, input_length, output);
  if (!success) {
    return Status::Corruption();
  }
  return Status::OK();
}

Status ZSTDCompressor::Uncompress(const UncompressionInfo* info,
                                  uint32_t /*compress_format_version*/,
                                  const char* input, size_t input_length,
                                  char** output, size_t* output_length,
                                  MemoryAllocator* allocator) {
  *output =
      ZSTD_Uncompress(*info, input, input_length, output_length, allocator);
  if (!*output) {
    return Status::Corruption();
  }
  return Status::OK();
}

#if ZSTD_VERSION_NUMBER >= 700
void* ZSTDCompressor::ProcessCompressionDict(std::string& dict, int level) {
  ZSTD_CDict* zstd_cdict = nullptr;
  if (!dict.empty()) {
    if (level == CompressionOptions::kDefaultCompressionLevel) {
      // 3 is the value of ZSTD_CLEVEL_DEFAULT (not exposed publicly), see
      // https://github.com/facebook/zstd/issues/1148
      level = 3;
    }
    // Should be safe (but slower) if below call fails as we'll use the
    // raw dictionary to compress.
    zstd_cdict = ZSTD_createCDict(dict.data(), dict.size(), level);
    assert(zstd_cdict != nullptr);
  }
  return zstd_cdict;
}

void ZSTDCompressor::DestroyCompressionDict(void* processed_dict) {
  size_t res = 0;
  if (processed_dict != nullptr) {
    res = ZSTD_freeCDict((ZSTD_CDict*)processed_dict);
  }
  assert(res == 0);  // Last I checked they can't fail
  (void)res;         // prevent unused var warning
}
#endif  // ZSTD_VERSION_NUMBER >= 700

#ifdef ROCKSDB_ZSTD_DDICT
void* ZSTDCompressor::ProcessUncompressionDict(Slice& slice) {
  ZSTD_DDict* zstd_ddict = nullptr;
  if (!slice.empty()) {
    zstd_ddict = ZSTD_createDDict_byReference(slice.data(), slice.size());
    assert(zstd_ddict != nullptr);
  }
  return zstd_ddict;
}

size_t ZSTDCompressor::GetUncompressionDictMemoryUsage(void* processed_dict) {
  return ZSTD_sizeof_DDict((ZSTD_DDict*)processed_dict);
}

void ZSTDCompressor::DestroyUncompressionDict(void* processed_dict) {
  size_t res = 0;
  if (processed_dict != nullptr) {
    res = ZSTD_freeDDict((ZSTD_DDict*)processed_dict);
  }
  assert(res == 0);  // Last I checked they can't fail
  (void)res;         // prevent unused var warning
}
#endif  // ROCKSDB_ZSTD_DDICT

#if defined(ZSTD) && (ZSTD_VERSION_NUMBER >= 500)
void* ZSTDCompressor::CreateCompressionContext() {
  ZSTD_CCtx* zstd_ctx_ = nullptr;
#ifdef ROCKSDB_ZSTD_CUSTOM_MEM
  zstd_ctx_ = ZSTD_createCCtx_advanced(port::GetJeZstdAllocationOverrides());
#else   // ROCKSDB_ZSTD_CUSTOM_MEM
  zstd_ctx_ = ZSTD_createCCtx();
#endif  // ROCKSDB_ZSTD_CUSTOM_MEM
  return zstd_ctx_;
}

void ZSTDCompressor::DestroyCompressionContext(void* context) {
  if (context != nullptr) {
    ZSTD_freeCCtx((ZSTD_CCtx*)context);
  }
}
#endif

void* ZSTDCompressor::CreateUncompressionContext() {
  ZSTDUncompressionContext* context = new ZSTDUncompressionContext;
  context->ctx_cache_ = CompressionContextCache::Instance();
  context->uncomp_cached_data_ =
      context->ctx_cache_->GetCachedZSTDUncompressData();
  return context;
}

void ZSTDCompressor::DestroyUncompressionContext(void* context) {
  ZSTDUncompressionContext* zstd_context = (ZSTDUncompressionContext*)context;
  if (zstd_context->uncomp_cached_data_.GetCacheIndex() != -1) {
    assert(zstd_context->ctx_cache_ != nullptr);
    zstd_context->ctx_cache_->ReturnCachedZSTDUncompressData(
        zstd_context->uncomp_cached_data_.GetCacheIndex());
  }
  delete zstd_context;
}

}  // namespace ROCKSDB_NAMESPACE

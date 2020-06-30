// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#include "rocksdb/compressor.h"

#include "rocksdb/compressor_registry.h"

namespace ROCKSDB_NAMESPACE {

CompressionDict::CompressionDict(const std::string& dict, CompressionType type,
                                 int level) {
  dict_ = std::move(dict);
  processed_dict_ = nullptr;
  compressor_ = CompressorRegistry::NewInstance()->GetCompressor(type);
  if (compressor_ != nullptr) {
    processed_dict_ = compressor_->ProcessCompressionDict(dict_, level);
  }
}

CompressionDict::~CompressionDict() {
  if (compressor_ != nullptr) {
    compressor_->DestroyCompressionDict(processed_dict_);
  }
}

UncompressionDict::UncompressionDict(const std::string& dict,
                                     CompressionType type)
    : dict_(std::move(dict)), slice_(dict_) {
  compressor_ = CompressorRegistry::NewInstance()->GetCompressor(type);
  if (compressor_ != nullptr) {
    processed_dict_ = compressor_->ProcessUncompressionDict(slice_);
  }
}

UncompressionDict::UncompressionDict(const Slice& slice,
                                     CacheAllocationPtr&& allocation,
                                     CompressionType type)
    : allocation_(std::move(allocation)), slice_(std::move(slice)) {
  compressor_ = CompressorRegistry::NewInstance()->GetCompressor(type);
  if (compressor_ != nullptr) {
    processed_dict_ = compressor_->ProcessUncompressionDict(slice_);
  }
}

UncompressionDict::UncompressionDict(UncompressionDict&& rhs)
    : dict_(std::move(rhs.dict_)),
      allocation_(std::move(rhs.allocation_)),
      slice_(std::move(rhs.slice_)),
      processed_dict_(rhs.processed_dict_) {
  rhs.processed_dict_ = nullptr;
}

UncompressionDict::~UncompressionDict() {
  if (compressor_ != nullptr) {
    compressor_->DestroyUncompressionDict(processed_dict_);
  }
}

UncompressionDict& UncompressionDict::operator=(UncompressionDict&& rhs) {
  if (this == &rhs) {
    return *this;
  }

  dict_ = std::move(rhs.dict_);
  allocation_ = std::move(rhs.allocation_);
  slice_ = std::move(rhs.slice_);
  processed_dict_ = rhs.processed_dict_;
  rhs.processed_dict_ = nullptr;

  return *this;
}

size_t UncompressionDict::ApproximateMemoryUsage() const {
  size_t usage = sizeof(UncompressionDict);
  usage += dict_.size();
  if (allocation_) {
    auto allocator = allocation_.get_deleter().allocator;
    if (allocator) {
      usage += allocator->UsableSize(allocation_.get(), slice_.size());
    } else {
      usage += slice_.size();
    }
  }
  if (compressor_ != nullptr) {
    usage += compressor_->GetUncompressionDictMemoryUsage(processed_dict_);
  }
  return usage;
}

CompressionContext::CompressionContext(CompressionType type) {
  compressor_ = CompressorRegistry::NewInstance()->GetCompressor(type);
  if (compressor_ != nullptr) {
    context_ = compressor_->CreateCompressionContext();
  }
}

CompressionContext::~CompressionContext() {
  if (compressor_ != nullptr) {
    compressor_->DestroyCompressionContext(context_);
  }
}

UncompressionContext::UncompressionContext(CompressionType type) {
  compressor_ = CompressorRegistry::NewInstance()->GetCompressor(type);
  if (compressor_ != nullptr) {
    context_ = compressor_->CreateUncompressionContext();
  }
}

UncompressionContext::~UncompressionContext() {
  if (compressor_ != nullptr) {
    compressor_->DestroyUncompressionContext(context_);
  }
}

}  // namespace ROCKSDB_NAMESPACE

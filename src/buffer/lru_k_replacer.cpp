//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : node_store_(num_frames), replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  bool curr_less_k = false;
  size_t min_value = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < replacer_size_; ++i) {
    auto &node = node_store_[i];
    if (!node.is_evictable_ || node.fid_ == INVALID_TXN_ID) {
      continue;
    }
    size_t value = node.history_.back();
    if (node.k_ < k_) {
      if (!curr_less_k || value < min_value) {
        min_value = value;
        *frame_id = i;
        curr_less_k = true;
      }
    }
    if (!curr_less_k && value < min_value) {
      min_value = value;
      *frame_id = i;
    }
  }

  if (min_value != std::numeric_limits<size_t>::max()) {
    node_store_[*frame_id].fid_ = INVALID_TXN_ID;
    --curr_size_;
  }
  return min_value != std::numeric_limits<size_t>::max();
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(true, "frame_id is invalid");
  }
  std::lock_guard lock(latch_);
  ++current_timestamp_;
  // if (node_store_.find(frame_id) == node_store_.end()) {
  //   node_store_.emplace(frame_id, LRUKNode{.history_{}, .k_{0}, .fid_{frame_id}, .is_evictable_{false}});
  // }
  auto &node = node_store_[frame_id];
  if (node.fid_ == INVALID_TXN_ID) {
    node.is_evictable_ = false;
    node.fid_ = frame_id;
    node.history_.clear();
    node.k_ = 0;
  }

  ++node.k_;
  node.history_.push_front(current_timestamp_);
  if (node.k_ > k_) {
    node.history_.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(true, "frame_id is invalid");
  }
  std::lock_guard lock(latch_);
  auto &node = node_store_[frame_id];
  if (node.fid_ == INVALID_TXN_ID) {
    return;
  }
  if (node.is_evictable_ && !set_evictable) {
    --curr_size_;
  } else if (!node.is_evictable_ && set_evictable) {
    ++curr_size_;
  }
  node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  std::lock_guard lock(latch_);
  auto &node = node_store_[frame_id];
  if (node.fid_ == INVALID_TXN_ID) {
    return;
  }
  if (!node.is_evictable_) {
    BUSTUB_ASSERT(true, "frame_id is non-evicting");
  }
  node_store_[frame_id].fid_ = INVALID_TXN_ID;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard lock(latch_);
  return curr_size_;
}

}  // namespace bustub

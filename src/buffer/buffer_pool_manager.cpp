//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  // std::cout << "init set" << pool_size << " " << replacer_k << "\n";
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard lock(latch_);
  frame_id_t id;
  if (!free_list_.empty()) {
    id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&id)) {
    return nullptr;
  }
  *page_id = AllocatePage();
  auto &page = pages_[id];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  page_table_.erase(page.GetPageId());
  page_table_.emplace(*page_id, id);

  page.ResetMemory();
  page.page_id_ = *page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  // std::cout << "New page_id = " << page.GetPageId() << " frame_id = " << id << "\n";
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard lock(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    auto id = page_table_[page_id];
    auto &page = pages_[id];
    ++page.pin_count_;
    replacer_->RecordAccess(id);
    replacer_->SetEvictable(id, false);
    return &page;
  }

  frame_id_t id;
  if (!free_list_.empty()) {
    id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&id)) {
    return nullptr;
  }
  auto &page = pages_[id];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  page_table_.erase(page.GetPageId());
  page_table_.emplace(page_id, id);

  page.ResetMemory();
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  disk_manager_->ReadPage(page.GetPageId(), page.GetData());

  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  // std::cout << "fetch page_id = " << page.GetPageId() << " frame_id = " << id << "pin count = " << page.GetPinCount()
  // << "\n";
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    // std::cout << "unpin page page_id = " << page_id << " don't find\n";
    return false;
  }
  auto id = page_table_[page_id];
  auto &page = pages_[id];
  if (page.GetPinCount() == 0) {
    // std::cout << "unpin page page_id = " << page_id << " already pinCount= 0\n";
    return false;
  }
  --page.pin_count_;
  if (page.GetPinCount() <= 0) {
    replacer_->SetEvictable(id, true);
  }
  // std::cout << "unpin page page_id = " << page_id << " current pin count = " << page.GetPinCount() << "\n";
  if (is_dirty) {
    page.is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto id = page_table_[page_id];
  auto &page = pages_[id];

  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lock(latch_);
  for (auto &[_, id] : page_table_) {
    auto &page = pages_[id];
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto id = page_table_[page_id];
  auto &page = pages_[id];
  if (page.GetPinCount() > 0) {
    return false;
  }
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  page.ResetMemory();
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  replacer_->Remove(id);
  free_list_.push_back(id);
  // std::cout << "remove page page_id = " << page_id;
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub

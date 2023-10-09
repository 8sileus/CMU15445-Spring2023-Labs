//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexByKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&comparator](const MappingType &lhs, const MappingType &rhs) -> bool {
    return comparator(lhs.first, rhs.first) < 0;
  };
  return std::lower_bound(array_, array_ + GetSize(), MappingType(key, ValueType{}), cmp) - array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValueByKey(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  auto index = GetIndexByKey(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    return array_[index].second;
  }
  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto index = GetIndexByKey(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    return false;
  }
  std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  auto index = GetIndexByKey(key, comparator);
  if (index >= GetSize() || comparator(array_[index].first, key) != 0) {
    return false;
  }
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BufferPoolManager *bpm) -> std::pair<KeyType, page_id_t> {
  page_id_t new_page_id;
  bpm->NewPage(&new_page_id);
  // 配置new page
  auto guard = bpm->FetchPageWrite(new_page_id);
  bpm->UnpinPage(new_page_id, true);
  auto new_leaf_page = guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  new_leaf_page->Init(GetMaxSize());
  new_leaf_page->SetNextPageId(GetNextPageId());
  SetNextPageId(new_page_id);
  // migrate data
  std::copy(array_ + GetMinSize(), array_ + GetLimitSize(), new_leaf_page->array_);

  // 修改size
  new_leaf_page->SetSize(GetLimitSize() - GetMinSize());
  SetSize(GetMinSize());
  return {new_leaf_page->KeyAt(0), new_page_id};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitToPrev(BufferPoolManager *bpm, page_id_t prev_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(prev_page_id);
  auto prev_page = guard.template AsMut<BPlusTreeLeafPage>();
  if (prev_page->GetSize() >= prev_page->GetMaxSize()) {
    return std::nullopt;
  }
  auto old_key = KeyAt(0);
  prev_page->array_[prev_page->GetSize()] = array_[0];
  std::copy(array_ + 1, array_ + GetSize(), array_);
  prev_page->IncreaseSize(1);
  IncreaseSize(-1);
  return std::make_pair(old_key, KeyAt(0));
}

// return next_page old key
INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitToNext(BufferPoolManager *bpm, page_id_t next_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(next_page_id);
  auto next_page = guard.template AsMut<BPlusTreeLeafPage>();
  if (next_page->GetSize() >= next_page->GetMaxSize()) {
    return std::nullopt;
  }
  auto old_key = next_page->KeyAt(0);
  std::copy_backward(next_page->array_, next_page->array_ + next_page->GetSize(),
                     next_page->array_ + next_page->GetSize() + 1);
  next_page->array_[0] = array_[GetSize() - 1];
  next_page->IncreaseSize(1);
  IncreaseSize(-1);
  return std::make_pair(old_key, next_page->KeyAt(0));
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BufferPoolManager *bpm, page_id_t prev_page_id, page_id_t next_page_id)
    -> KeyType {
  if (prev_page_id != INVALID_PAGE_ID) {
    auto prev_guard = bpm->FetchPageWrite(prev_page_id);
    auto prev_leaf_page = prev_guard.template AsMut<BPlusTreeLeafPage>();
    prev_leaf_page->SetNextPageId(GetNextPageId());
    std::copy(array_, array_ + GetSize(), prev_leaf_page->array_ + prev_leaf_page->GetSize());
    prev_leaf_page->IncreaseSize(GetSize());
    return KeyAt(0);
  }
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_guard = bpm->FetchPageWrite(next_page_id);
    auto next_leaf_page = next_guard.template AsMut<BPlusTreeLeafPage>();
    SetNextPageId(next_leaf_page->GetNextPageId());
    std::copy(next_leaf_page->array_, next_leaf_page->array_ + next_leaf_page->GetSize(), array_ + GetSize());
    IncreaseSize(next_leaf_page->GetSize());
    return next_leaf_page->KeyAt(0);
  }
  return KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FetchFromPrev(BufferPoolManager *bpm, page_id_t prev_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(prev_page_id);
  auto prev_page = guard.template AsMut<BPlusTreeLeafPage>();
  if (prev_page->GetSize() <= prev_page->GetMinSize()) {
    return std::nullopt;
  }
  auto old_key = KeyAt(0);
  std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  array_[0] = prev_page->array_[prev_page->GetSize() - 1];
  prev_page->IncreaseSize(-1);
  IncreaseSize(1);
  return std::make_pair(old_key, KeyAt(0));
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FetchFromNext(BufferPoolManager *bpm, page_id_t next_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(next_page_id);
  auto next_page = guard.template AsMut<BPlusTreeLeafPage>();
  if (next_page->GetSize() <= next_page->GetMinSize()) {
    return std::nullopt;
  }
  auto old_key = next_page->KeyAt(0);
  array_[GetSize()] = next_page->array_[0];
  std::copy(next_page->array_ + 1, next_page->array_ + next_page->GetSize(), next_page->array_);
  next_page->IncreaseSize(-1);
  IncreaseSize(1);
  return std::make_pair(old_key, next_page->KeyAt(0));
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

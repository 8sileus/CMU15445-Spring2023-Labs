//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexByKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&comparator](const auto &lhs, const auto &rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; };
  return std::upper_bound(array_ + 1, array_ + GetSize(), MappingType(key, ValueType{}), cmp) - array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueByKey(const KeyType &key, const KeyComparator &comparator) const
    -> ValueType {
  return array_[GetIndexByKey(key, comparator) - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &left_value,
                                            const ValueType &right_value, const KeyComparator &comparator) {
  if (GetSize() == 0) {
    array_[0] = MappingType(key, left_value);
    array_[1] = MappingType(key, right_value);
    IncreaseSize(2);
    return;
  }
  auto index = GetIndexByKey(key, comparator);
  if (index < GetSize()) {
    std::copy_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  }
  array_[index] = MappingType(key, right_value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Update(const KeyType &old_key, const KeyType &new_key,
                                            const KeyComparator &comparator) {
  auto index = GetIndexByKey(old_key, comparator) - 1;
  array_[index].first = new_key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) {
  auto index = GetIndexByKey(key, comparator) - 1;
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BufferPoolManager *bpm) -> std::pair<KeyType, ValueType> {
  page_id_t new_page_id;
  auto guard = bpm->NewPageGuarded(&new_page_id);
  auto new_leaf_page = guard.template AsMut<BPlusTreeInternalPage>();
  new_leaf_page->Init(GetMaxSize());
  // migrate data
  std::copy(array_ + GetMinSize(), array_ + GetLimitSize(), new_leaf_page->array_);

  // 修改size
  new_leaf_page->SetSize(GetLimitSize() - GetMinSize());
  SetSize(GetMinSize());
  return {new_leaf_page->KeyAt(0), new_page_id};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitToPrev(BufferPoolManager *bpm, page_id_t prev_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(prev_page_id);
  auto prev_page = guard.template AsMut<BPlusTreeInternalPage>();
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitToNext(BufferPoolManager *bpm, page_id_t next_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(next_page_id);
  auto next_page = guard.template AsMut<BPlusTreeInternalPage>();
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BufferPoolManager *bpm, page_id_t prev_page_id, page_id_t next_page_id)
    -> KeyType {
  if (prev_page_id != INVALID_PAGE_ID) {
    auto prev_guard = bpm->FetchPageWrite(prev_page_id);
    auto prev_page = prev_guard.template AsMut<BPlusTreeInternalPage>();
    // prev_leaf_page->SetNextPageId(GetNextPageId());
    std::copy(array_, array_ + GetSize(), prev_page->array_ + prev_page->GetSize());
    prev_page->IncreaseSize(GetSize());
    SetSize(0);
    return KeyAt(0);
  }
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_guard = bpm->FetchPageWrite(next_page_id);
    auto next_page = next_guard.template AsMut<BPlusTreeInternalPage>();
    // SetNextPageId(next_leaf_page->GetNextPageId());
    std::copy(next_page->array_, next_page->array_ + next_page->GetSize(), array_ + GetSize());
    IncreaseSize(next_page->GetSize());
    next_page->SetSize(0);
    return next_page->KeyAt(0);
  }
  return KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FetchFromPrev(BufferPoolManager *bpm, page_id_t prev_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(prev_page_id);
  auto prev_page = guard.template AsMut<BPlusTreeInternalPage>();
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FetchFromNext(BufferPoolManager *bpm, page_id_t next_page_id)
    -> std::optional<std::pair<KeyType, KeyType>> {
  auto guard = bpm->FetchPageWrite(next_page_id);
  auto next_page = guard.template AsMut<BPlusTreeInternalPage>();
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

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

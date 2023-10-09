//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  explicit IndexIterator(BufferPoolManager *bpm, page_id_t page_id = INVALID_PAGE_ID, int index = 0)
      : bpm_(bpm), page_id_(page_id), index_(index) {}

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return bpm_ == itr.bpm_ && page_id_ == itr.page_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  page_id_t page_id_;
  int index_;
};

}  // namespace bustub

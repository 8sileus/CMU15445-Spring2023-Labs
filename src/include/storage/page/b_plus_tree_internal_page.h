//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE (((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / sizeof(MappingType)) - 1)
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Deleted to disallow initialization
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  void Init(int max_size = INTERNAL_PAGE_SIZE) {
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::INTERNAL_PAGE);
  }

  auto KeyAt(int index) const -> KeyType { return array_[index].first; }
  auto ValueAt(int index) const -> ValueType { return array_[index].second; }
  auto GetIndexByKey(const KeyType &key, const KeyComparator &comparator) const -> int;
  auto GetValueByKey(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  void Insert(const KeyType &key, const ValueType &left_value, const ValueType &right_value,
              const KeyComparator &comparator);
  void Update(const KeyType &old_key, const KeyType &new_key, const KeyComparator &comparator);
  void Delete(const KeyType &key, const KeyComparator &comparator);

  auto Split(BufferPoolManager *bpm) -> std::pair<KeyType, ValueType>;
  auto SplitToPrev(BufferPoolManager *bpm, page_id_t prev_page_id) -> std::optional<std::pair<KeyType, KeyType>>;
  auto SplitToNext(BufferPoolManager *bpm, page_id_t next_page_id) -> std::optional<std::pair<KeyType, KeyType>>;

  auto Merge(BufferPoolManager *bpm, page_id_t prev_page_id, page_id_t next_page_id) -> KeyType;
  auto FetchFromPrev(BufferPoolManager *bpm, page_id_t prev_page_id) -> std::optional<std::pair<KeyType, KeyType>>;
  auto FetchFromNext(BufferPoolManager *bpm, page_id_t next_page_id) -> std::optional<std::pair<KeyType, KeyType>>;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub

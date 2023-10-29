//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
  std::lock_guard lock(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_records = txn->GetWriteSet();
  while (!table_write_records->empty()) {
    auto &record = table_write_records->back();
    auto tuple_meta = record.table_heap_->GetTupleMeta(record.rid_);
    switch (record.wtype_) {
      case WType::INSERT:
        tuple_meta.is_deleted_ = true;
        record.table_heap_->UpdateTupleMeta(tuple_meta, record.rid_);
        break;
      case WType::DELETE:
        tuple_meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(tuple_meta, record.rid_);
        break;
      case WType::UPDATE:
        // tuple_meta.is_deleted_ = true;
        // record.table_heap_->UpdateTupleMeta(tuple_meta, record.rid_);
        break;
    }
    table_write_records->pop_back();
  }
  auto index_write_records = txn->GetIndexWriteSet();
  while (!index_write_records->empty()) {
    auto &record = index_write_records->back();
    switch (record.wtype_) {
      case WType::INSERT:
        record.catalog_->GetIndex(record.index_oid_)->index_->DeleteEntry(record.tuple_, record.rid_, txn);
        break;
      case WType::DELETE:
        record.catalog_->GetIndex(record.index_oid_)->index_->InsertEntry(record.tuple_, record.rid_, txn);
        break;
      case WType::UPDATE:
        break;
    }
    index_write_records->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
  std::lock_guard lock(txn_map_mutex_);
  txn_map_.erase(txn->GetTransactionId());
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub

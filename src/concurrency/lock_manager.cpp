//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }

    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 获取锁队列
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      // IS -> [S, X, IX, SIX]
      // S -> [X, SIX]
      // IX -> [X, SIX]
      // SIX -> [X]
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(request);
      DeleteTableLockSet(txn, request);
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

      auto it = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                             [](const std::shared_ptr<LockRequest> &request) { return !request->granted_; });

      lock_request_queue->request_queue_.insert(it, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      while (!CanGrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertTableLockSet(txn, upgrade_lock_request);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  while (!CanGrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertTableLockSet(txn, lock_request);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock.unlock();

      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      DeleteTableLockSet(txn, request);
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  std::unique_lock lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->request_queue_.remove(request);
      DeleteRowLockSet(txn, request);

      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      auto it = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                             [](const auto &request) { return !request->granted_; });
      lock_request_queue->request_queue_.insert(it, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      while (!CanGrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertRowLockSet(txn, upgrade_lock_request);

      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  while (!CanGrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertRowLockSet(txn, lock_request);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  std::unique_lock lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock.unlock();
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      DeleteRowLockSet(txn, request);
      return true;
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2); it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::function<bool(txn_id_t)> dfs = [&](txn_id_t cur_txn_id) -> bool {
    if (this->visited_txn_ids_.find(cur_txn_id) != this->visited_txn_ids_.end()) {
      return false;
    }
    if (this->visiting_txn_ids_.find(cur_txn_id)!=this->visiting_txn_ids_.end()) {
      return true;
    }
    this->visiting_txn_ids_.insert(cur_txn_id);
    for (auto next_txn_id : this->waits_for_[cur_txn_id]) {
      if (dfs(next_txn_id)) {
        return true;
      }
    }
    this->visiting_txn_ids_.erase(cur_txn_id);
    this->visited_txn_ids_.insert(cur_txn_id);
    return false;
  };

  for (const auto &cur_txn_id : txn_set_) {
    if (dfs(cur_txn_id)) {
      *txn_id = *visiting_txn_ids_.begin();
      for (const auto &visiting_txn_id : visiting_txn_ids_) {
        *txn_id = std::max(*txn_id, visiting_txn_id);
      }
      visiting_txn_ids_.clear();
      return true;
    }
    BUSTUB_ASSERT(visiting_txn_ids_.empty(), "why visiting_txn_ids_ is not empty");
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::lock_guard lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[t1, t2s] : waits_for_) {
    for (const auto &t2 : t2s) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  std::unordered_map<txn_id_t, RID> txn_rid_map;
  std::unordered_map<txn_id_t, table_oid_t> txn_oid_map;
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::scoped_lock lock(table_lock_map_latch_, row_lock_map_latch_);
      for (const auto &[_, lock_queue] : table_lock_map_) {
        std::lock_guard lock(lock_queue->latch_);
        std::set<txn_id_t> granted_txn_ids;
        for (const auto &lock_request : lock_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_txn_ids.insert(lock_request->txn_id_);
          } else {
            for (const auto &txn_id : granted_txn_ids) {
              txn_oid_map.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
      }
      for (const auto &[_, lock_queue] : row_lock_map_) {
        std::lock_guard lock(lock_queue->latch_);
        std::set<txn_id_t> granted_txn_ids;
        for (const auto &lock_request : lock_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_txn_ids.insert(lock_request->txn_id_);
          } else {
            for (const auto &txn_id : granted_txn_ids) {
              txn_rid_map.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id); 
            }
          }
        }
      }
    }
    txn_id_t txn_id;
    while (HasCycle(&txn_id)) {
      auto txn = txn_manager_->GetTransaction(txn_id);
      txn->SetState(TransactionState::ABORTED);

      waits_for_.erase(txn_id);
      for (const auto &cur_txn_id : txn_set_) {
        if (cur_txn_id != txn_id) {
          RemoveEdge(cur_txn_id, txn_id);
        }
      }

      if (txn_oid_map.find(txn_id) != txn_oid_map.end()) {
        std::lock_guard lock(table_lock_map_[txn_oid_map[txn_id]]->latch_);
        table_lock_map_[txn_oid_map[txn_id]]->cv_.notify_all();
      }

      if (txn_rid_map.find(txn_id) != txn_rid_map.end()) {
        std::lock_guard lock(row_lock_map_[txn_rid_map[txn_id]]->latch_);
        row_lock_map_[txn_rid_map[txn_id]]->cv_.notify_all();
      }
    }
    txn_rid_map.clear();
    txn_oid_map.clear();
    visiting_txn_ids_.clear();
    visited_txn_ids_.clear();
  }
}

auto LockManager::CanGrantLock(const std::shared_ptr<LockRequest> &lock_request,
                               const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto &request : lock_request_queue->request_queue_) {
    if (request->granted_) {
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (request->lock_mode_ == LockMode::EXCLUSIVE || request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE) {
            return false;
          }
          break;
      }
    } else {
      return lock_request.get() == request.get();
    }
  }
  return false;
}

void LockManager::InsertTableLockSet(Transaction *txn, const std ::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
  }
}

void LockManager::DeleteTableLockSet(Transaction *txn, const std ::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
  }
}

void LockManager::InsertRowLockSet(Transaction *txn, const std ::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[lock_request->oid_].emplace(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[lock_request->oid_].emplace(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::DeleteRowLockSet(Transaction *txn, const std ::shared_ptr<LockRequest> &lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      (*txn->GetSharedRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      (*txn->GetExclusiveRowLockSet())[lock_request->oid_].erase(lock_request->rid_);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::UnlockAll() {}

}  // namespace bustub

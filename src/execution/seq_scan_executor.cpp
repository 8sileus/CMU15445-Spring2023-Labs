//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())) {}

void SeqScanExecutor::Init() {
  try {
    if (exec_ctx_->IsDelete()) {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                  LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)) {
        throw ExecutionException("Lock Table FAILED");
      }
    } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
               !exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  table_info_->oid_)) {
        throw ExecutionException("Lock Table FAILED");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("SeqExecutor::Init " + e.GetInfo());
  }
  it_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_->IsEnd()) {
    *rid = it_->GetRID();
    try {
      if (exec_ctx_->IsDelete()) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  plan_->table_oid_, *rid)) {
          throw ExecutionException("Lock Row Failed");
        }
      } else if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
                 !exec_ctx_->GetTransaction()->IsRowExclusiveLocked(table_info_->oid_, *rid)) {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                  plan_->table_oid_, *rid)) {
          throw ExecutionException("Lock Row Failed");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
    }
    auto [tuple_meta, tmp_tuple] = it_->GetTuple();
    ++(*it_);

    bool ok = !tuple_meta.is_deleted_;
    if (ok && plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&tmp_tuple, GetOutputSchema());
      ok = !value.IsNull() && value.GetAs<bool>();
    }

    if (ok) {
      *tuple = tmp_tuple;
      if (!exec_ctx_->IsDelete() &&
          exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        try {
          if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid)) {
            throw ExecutionException("UnLock Row Failed");
          }
        } catch (TransactionAbortException &e) {
          throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
        }
      }
      return true;
    }

    if (exec_ctx_->IsDelete() || exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, *rid, true)) {
          throw ExecutionException("UnLock Row Failed");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
      }
    }
  }

  if (!exec_ctx_->IsDelete() && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_)) {
        throw ExecutionException("Unlock Table Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqExecutor::Next " + e.GetInfo());
    }
  }
  return false;
}

}  // namespace bustub

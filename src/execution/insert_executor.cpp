//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  auto count = 0;
  TupleMeta tuple_meta{.insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = false};
  while (child_executor_->Next(tuple, rid)) {
    auto tuple_rid = table_info_->table_->InsertTuple(tuple_meta, *tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!tuple_rid.has_value()) {
      continue;
    }
    for (auto &index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, tuple_rid.value(), exec_ctx_->GetTransaction());
    }
    ++count;
  }
  std::vector<Value> values;
  values.emplace_back(INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  finished_ = true;
  return true;
}

}  // namespace bustub

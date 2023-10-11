//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  auto count = 0;
  TupleMeta tuple_meta{.insert_txn_id_ = INVALID_TXN_ID, .delete_txn_id_ = INVALID_TXN_ID, .is_deleted_ = false};
  while (child_executor_->Next(tuple, rid)) {
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    for (auto &index_info : index_infos_) {
      auto key = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &child_executor_->GetOutputSchema());
    tuple_meta.is_deleted_ = false;
    auto tuple_rid = table_info_->table_->InsertTuple(tuple_meta, new_tuple, exec_ctx_->GetLockManager(),
                                                      exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!tuple_rid.has_value()) {
      continue;
    }
    for (auto &index_info : index_infos_) {
      auto key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
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

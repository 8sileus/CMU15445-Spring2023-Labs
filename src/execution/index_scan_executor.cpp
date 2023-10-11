//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  it_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get())->GetBeginIterator();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_.IsEnd()) {
    *rid = (*it_).second;
    ++it_;
    auto [tuple_meta, tmp_tuple] = table_info_->table_->GetTuple(*rid);
    if (!tuple_meta.is_deleted_) {
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub

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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      it_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_.IsEnd()) {
    auto [tuple_meta, tmp_tuple] = it_.GetTuple();
    *rid = it_.GetRID();
    ++it_;
    if (!tuple_meta.is_deleted_) {
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub

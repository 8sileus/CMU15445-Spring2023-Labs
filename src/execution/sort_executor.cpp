#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    for (const auto &[type, expr] : this->plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&left_tuple, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&right_tuple, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    BUSTUB_ASSERT(false, "duplicated tuple key");
    return true;
  });
  it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.end()) {
    *tuple = *(it_++);
    return true;
  }
  return false;
}

}  // namespace bustub

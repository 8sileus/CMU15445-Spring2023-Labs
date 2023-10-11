#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [this](const Tuple &left_tuple, const Tuple &right_tuple) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap.emplace(tuple);
    if (heap.size() > plan_->GetN()) {
      heap.pop();
    }
  }
  while (!heap.empty()) {
    tuples_.emplace_back(heap.top());
    heap.pop();
  }
  it_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.rend()) {
    *tuple = *(it_++);
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.rend() - it_; };

}  // namespace bustub

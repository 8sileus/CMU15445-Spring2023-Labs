//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ != 0) {
    if (LoopJoin(tuple)) {
      return true;
    }
  }
  while (left_executor_->Next(&left_tuple_, rid)) {
    right_executor_->Init();
    if (LoopJoin(tuple)) {
      return true;
    }
  }
  return false;
}

auto NestedLoopJoinExecutor::LoopJoin(Tuple *tuple) -> bool {
  while (index_ < right_tuples_.size()) {
    if (plan_->Predicate()) {
      auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                    &right_tuples_[index_], right_executor_->GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++index_;
        continue;
      }
    }
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(right_tuples_[index_].GetValue(&right_executor_->GetOutputSchema(), i));
    }
    *tuple = Tuple(values, &GetOutputSchema());
    index_ = (index_ + 1) % right_tuples_.size();
    is_matched_ = index_ != 0;
    return true;
  }
  index_ = 0;
  if (plan_->GetJoinType() == JoinType::LEFT && !is_matched_) {
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  is_matched_ = false;
  return false;
}

}  // namespace bustub

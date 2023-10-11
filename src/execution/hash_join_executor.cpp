//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  while (right_child_->Next(&tuple, &rid)) {
    HashJoinKey key;
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      key.values_.emplace_back(expr->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    map_[key].push_back(tuple);
  }
  while (left_child_->Next(&tuple, &rid)) {
    HashJoinKey key;
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      key.values_.emplace_back(expr->Evaluate(&tuple, left_child_->GetOutputSchema()));
    }
    if (map_.count(key) != 0) {
      for (const auto &right_tuple : map_[key]) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        tuples_.emplace_back(values, &GetOutputSchema());
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      tuples_.emplace_back(values, &GetOutputSchema());
    }
  }
  it_ = tuples_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.end()) {
    *tuple = *(it_++);
    return true;
  }
  return false;
}

}  // namespace bustub

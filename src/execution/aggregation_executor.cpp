//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // empty table && empty groupby && not finished
  if (aht_.Begin() == aht_.End() && plan_->group_bys_.empty() && !finished_) {
    std::vector<Value> values;
    for (auto aggregate : plan_->agg_types_) {
      switch (aggregate) {
        case AggregationType::CountStarAggregate:
          values.push_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    *tuple = Tuple(values, &GetOutputSchema());
    finished_ = true;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values(aht_iterator_.Key().group_bys_);
  for (auto &aggregate : aht_iterator_.Val().aggregates_) {
    values.emplace_back(aggregate);
  }
  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

#include "execution/executors/filter_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizerTooJoins(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizerTooJoins(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter) {
  }
  return optimized_plan;
}

}  // namespace bustub

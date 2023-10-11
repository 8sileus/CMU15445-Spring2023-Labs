#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void FillKeyExpressions(std::vector<AbstractExpressionRef> &left_key_expressions,
                        std::vector<AbstractExpressionRef> &right_key_expressions,
                        const ColumnValueExpression *left_expr, const ColumnValueExpression *right_expr) {
  if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
  } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
  } else {
    BUSTUB_ASSERT(false, "left_expr or right_expr error");
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            std::vector<AbstractExpressionRef> left_key_expressions;
            std::vector<AbstractExpressionRef> right_key_expressions;
            FillKeyExpressions(left_key_expressions, right_key_expressions, left_expr, right_expr);
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                      std::move(right_key_expressions), nlj_plan.GetJoinType());
          }
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        if (auto *expr1 = dynamic_cast<ComparisonExpression *>(expr->children_[0].get()); expr1 != nullptr) {
          if (auto *expr2 = dynamic_cast<ComparisonExpression *>(expr->children_[1].get()); expr2 != nullptr) {
            if (expr1->comp_type_ == ComparisonType::Equal && expr2->comp_type_ == ComparisonType::Equal) {
              std::vector<AbstractExpressionRef> left_key_expressions;
              std::vector<AbstractExpressionRef> right_key_expressions;
              if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr1->children_[0].get());
                  left_expr != nullptr) {
                if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr1->children_[1].get());
                    right_expr != nullptr) {
                  FillKeyExpressions(left_key_expressions, right_key_expressions, left_expr, right_expr);
                }
              }
              if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr2->children_[0].get());
                  left_expr != nullptr) {
                if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr2->children_[1].get());
                    right_expr != nullptr) {
                  FillKeyExpressions(left_key_expressions, right_key_expressions, left_expr, right_expr);
                }
              }
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                        std::move(right_key_expressions), nlj_plan.GetJoinType());
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub

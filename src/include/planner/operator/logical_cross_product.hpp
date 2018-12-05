//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
public:
	LogicalCrossProduct() : LogicalOperator(LogicalOperatorType::CROSS_PRODUCT) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	vector<string> GetNames() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb

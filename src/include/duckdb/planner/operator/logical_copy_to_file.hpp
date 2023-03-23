//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/planner/filename_format_creator.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	LogicalCopyToFile(CopyFunction function, unique_ptr<FunctionData> bind_data)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_TO_FILE), function(function),
	      bind_data(std::move(bind_data)) {
	}
	CopyFunction function;
	unique_ptr<FunctionData> bind_data;
	std::string file_path;
	bool use_tmp_file;
	FilenameFormatCreator fmt;
	bool overwrite_or_ignore;
	bool per_thread_output;

	bool partition_output;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb

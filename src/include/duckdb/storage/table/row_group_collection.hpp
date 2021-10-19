//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {
struct ParallelTableScanState;

class RowGroupCollection {
public:
	RowGroupCollection(shared_ptr<DataTableInfo> info, vector<LogicalType> types, idx_t row_start, idx_t total_rows = 0);

public:
	idx_t GetTotalRows();

	void Initialize(PersistentTableData &data);
	void InitializeEmpty();

	void AppendRowGroup(idx_t start_row);
	void Verify();

	void InitializeScan(TableScanState &state, const vector<column_t> &column_ids, TableFilterSet *table_filters);
	void InitializeCreateIndexScan(CreateIndexScanState &state);
	void InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
	                              idx_t end_row);
	static bool InitializeScanInRowGroup(TableScanState &state, const vector<column_t> &column_ids,
	                                     TableFilterSet *table_filters, RowGroup *row_group, idx_t vector_index,
	                                     idx_t max_row);
	void InitializeParallelScan(ClientContext &context, ParallelTableScanState &state);
	bool NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
	                      const vector<column_t> &column_ids);

	void Fetch(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids, Vector &row_identifiers,
	           idx_t fetch_count, ColumnFetchState &state);

	void InitializeAppend(Transaction &transaction, TableAppendState &state, idx_t append_count);
	void Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state, TableStatistics &stats);
	void CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count);
	void RevertAppendInternal(idx_t start_row, idx_t count);

	void RemoveFromIndexes(Vector &row_identifiers, idx_t count);

	idx_t Delete(Transaction &transaction, DataTable *table, row_t *ids, idx_t count);
	void Update(Transaction &transaction, row_t *ids, const vector<column_t> &column_ids, DataChunk &updates,
	            TableStatistics &stats);
	void UpdateColumn(Transaction &transaction, Vector &row_ids, const vector<column_t> &column_path,
	                  DataChunk &updates, TableStatistics &stats);

	void Checkpoint(TableDataWriter &writer, vector<RowGroupPointer> &row_group_pointers,
	                vector<unique_ptr<BaseStatistics>> &global_stats);

	void CommitDropColumn(idx_t index);
	void CommitDropTable();

	vector<vector<Value>> GetStorageInfo();

	shared_ptr<RowGroupCollection> AddColumn(ColumnDefinition &new_column, Expression *default_value,
	                                         BaseStatistics &stats);
	shared_ptr<RowGroupCollection> RemoveColumn(idx_t col_idx);
	shared_ptr<RowGroupCollection> AlterType(idx_t changed_idx, const LogicalType &target_type,
	                                         vector<column_t> bound_columns, Expression &cast_expr,
	                                         BaseStatistics &stats);

private:
	//! The number of rows in the table
	atomic<idx_t> total_rows;
	shared_ptr<DataTableInfo> info;
	vector<LogicalType> types;
	idx_t row_start;
	//! The segment trees holding the various row_groups of the table
	shared_ptr<SegmentTree> row_groups;
};

} // namespace duckdb

#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/row/row_data_collection_scanner.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

//	Global sink state
class WindowGlobalSinkState : public GlobalSinkState {
public:
	WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context)
	    : mode(DBConfig::GetConfig(context).options.window_mode) {

		D_ASSERT(op.select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[0]->Cast<BoundWindowExpression>();

		global_partition =
		    make_uniq<PartitionGlobalSinkState>(context, wexpr.partitions, wexpr.orders, op.children[0]->types,
		                                        wexpr.partitions_stats, op.estimated_cardinality);
	}

	unique_ptr<PartitionGlobalSinkState> global_partition;
	WindowAggregationMode mode;
};

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ClientContext &context, const WindowGlobalSinkState &gstate)
	    : local_partition(context, *gstate.global_partition) {
	}

	void Sink(DataChunk &input_chunk) {
		local_partition.Sink(input_chunk);
	}

	void Combine() {
		local_partition.Combine();
	}

	PartitionLocalSinkState local_partition;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list_p,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list_p)) {
	is_order_dependent = false;
	for (auto &expr : select_list) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_WINDOW);
		auto &bound_window = expr->Cast<BoundWindowExpression>();
		if (bound_window.partitions.empty() && bound_window.orders.empty()) {
			is_order_dependent = true;
		}
	}
}

static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = MinValue(l + n - 1, r);
		n -= MinValue(n, r - l);
		return start;
	}

	while (l < r) {
		//	If l is aligned with the start of a block, and the block is blank, then skip forward one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(l, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && !shift) {
			l += ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop over the block
		for (; shift < ValidityMask::BITS_PER_VALUE && l < r; ++shift, ++l) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MinValue(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = (r <= l + n) ? l : r - n;
		n -= r - start;
		return start;
	}

	while (l < r) {
		// If r is aligned with the start of a block, and the previous block is blank,
		// then skip backwards one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(r - 1, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && (shift + 1 == ValidityMask::BITS_PER_VALUE)) {
			// r is nonzero (> l) and word aligned, so this will not underflow.
			r -= ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop backwards over the block
		// shift is probing r-1 >= l >= 0
		for (++shift; shift-- > 0; --r) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
}

static void PrepareInputExpressions(vector<unique_ptr<Expression>> &exprs, ExpressionExecutor &executor,
                                    DataChunk &chunk) {
	if (exprs.empty()) {
		return;
	}

	vector<LogicalType> types;
	for (idx_t expr_idx = 0; expr_idx < exprs.size(); ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	if (!types.empty()) {
		auto &allocator = executor.GetAllocator();
		chunk.Initialize(allocator, types);
	}
}

static void PrepareInputExpression(Expression &expr, ExpressionExecutor &executor, DataChunk &chunk) {
	vector<LogicalType> types;
	types.push_back(expr.return_type);
	executor.AddExpression(expr);

	auto &allocator = executor.GetAllocator();
	chunk.Initialize(allocator, types);
}

struct WindowInputExpression {
	WindowInputExpression(optional_ptr<Expression> expr_p, ClientContext &context)
	    : expr(expr_p), ptype(PhysicalType::INVALID), scalar(true), executor(context) {
		if (expr) {
			PrepareInputExpression(*expr, executor, chunk);
			ptype = expr->return_type.InternalType();
			scalar = expr->IsScalar();
		}
	}

	void Execute(DataChunk &input_chunk) {
		if (expr) {
			chunk.Reset();
			executor.Execute(input_chunk, chunk);
			chunk.Verify();
		}
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		const auto data = FlatVector::GetData<T>(chunk.data[0]);
		return data[scalar ? 0 : i];
	}

	inline bool CellIsNull(idx_t i) const {
		D_ASSERT(!chunk.data.empty());
		if (chunk.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return ConstantVector::IsNull(chunk.data[0]);
		}
		return FlatVector::IsNull(chunk.data[0], i);
	}

	inline void CopyCell(Vector &target, idx_t target_offset) const {
		D_ASSERT(!chunk.data.empty());
		auto &source = chunk.data[0];
		auto source_offset = scalar ? 0 : target_offset;
		VectorOperations::Copy(source, target, source_offset + 1, source_offset, target_offset);
	}

	optional_ptr<Expression> expr;
	PhysicalType ptype;
	bool scalar;
	ExpressionExecutor executor;
	DataChunk chunk;
};

struct WindowInputColumn {
	WindowInputColumn(Expression *expr_p, ClientContext &context, idx_t capacity_p)
	    : input_expr(expr_p, context), count(0), capacity(capacity_p) {
		if (input_expr.expr) {
			target = make_uniq<Vector>(input_expr.chunk.data[0].GetType(), capacity);
		}
	}

	void Append(DataChunk &input_chunk) {
		if (input_expr.expr) {
			const auto source_count = input_chunk.size();
			D_ASSERT(count + source_count <= capacity);
			if (!input_expr.scalar || !count) {
				input_expr.Execute(input_chunk);
				auto &source = input_expr.chunk.data[0];
				VectorOperations::Copy(source, *target, source_count, 0, count);
			}
			count += source_count;
		}
	}

	inline bool CellIsNull(idx_t i) {
		D_ASSERT(target);
		D_ASSERT(i < count);
		return FlatVector::IsNull(*target, input_expr.scalar ? 0 : i);
	}

	template <typename T>
	inline T GetCell(idx_t i) const {
		D_ASSERT(target);
		D_ASSERT(i < count);
		const auto data = FlatVector::GetData<T>(*target);
		return data[input_expr.scalar ? 0 : i];
	}

	WindowInputExpression input_expr;

private:
	unique_ptr<Vector> target;
	idx_t count;
	idx_t capacity;
};

static inline bool BoundaryNeedsPeer(const WindowBoundary &boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return true;
	default:
		return false;
	}
}

enum WindowBounds : uint8_t { PARTITION_BEGIN, PARTITION_END, PEER_BEGIN, PEER_END, WINDOW_BEGIN, WINDOW_END };

struct WindowBoundariesState {
	static inline bool IsScalar(const unique_ptr<Expression> &expr) {
		return expr ? expr->IsScalar() : true;
	}

	WindowBoundariesState(BoundWindowExpression &wexpr, const idx_t input_size)
	    : type(wexpr.type), input_size(input_size), start_boundary(wexpr.start), end_boundary(wexpr.end),
	      partition_count(wexpr.partitions.size()), order_count(wexpr.orders.size()),
	      range_sense(wexpr.orders.empty() ? OrderType::INVALID : wexpr.orders[0].type),
	      has_preceding_range(wexpr.start == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                          wexpr.end == WindowBoundary::EXPR_PRECEDING_RANGE),
	      has_following_range(wexpr.start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                          wexpr.end == WindowBoundary::EXPR_FOLLOWING_RANGE),
	      needs_peer(BoundaryNeedsPeer(wexpr.end) || wexpr.type == ExpressionType::WINDOW_CUME_DIST) {
	}

	void Update(const idx_t row_idx, WindowInputColumn &range_collection, const idx_t chunk_idx,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	void Bounds(DataChunk &bounds, idx_t row_idx, WindowInputColumn &range, const idx_t count,
	            WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
	            const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// Cached lookups
	const ExpressionType type;
	const idx_t input_size;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const size_t partition_count;
	const size_t order_count;
	const OrderType range_sense;
	const bool has_preceding_range;
	const bool has_following_range;
	const bool needs_peer;

	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	FrameBounds prev;
};

template <typename T>
static T GetCell(DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	const auto data = FlatVector::GetData<T>(source);
	return data[index];
}

static bool CellIsNull(DataChunk &chunk, idx_t column, idx_t index) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	return FlatVector::IsNull(source, index);
}

static void CopyCell(DataChunk &chunk, idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	D_ASSERT(chunk.ColumnCount() > column);
	auto &source = chunk.data[column];
	VectorOperations::Copy(source, target, index + 1, index, target_offset);
}

template <typename T>
struct WindowColumnIterator {
	using iterator = WindowColumnIterator<T>;
	using iterator_category = std::random_access_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	explicit WindowColumnIterator(WindowInputColumn &coll_p, pointer pos_p = 0) : coll(&coll_p), pos(pos_p) {
	}

	//	Forward iterator
	inline reference operator*() const {
		return coll->GetCell<T>(pos);
	}
	inline explicit operator pointer() const {
		return pos;
	}

	inline iterator &operator++() {
		++pos;
		return *this;
	}
	inline iterator operator++(int) {
		auto result = *this;
		++(*this);
		return result;
	}

	//	Bidirectional iterator
	inline iterator &operator--() {
		--pos;
		return *this;
	}
	inline iterator operator--(int) {
		auto result = *this;
		--(*this);
		return result;
	}

	//	Random Access
	inline iterator &operator+=(difference_type n) {
		pos += n;
		return *this;
	}
	inline iterator &operator-=(difference_type n) {
		pos -= n;
		return *this;
	}

	inline reference operator[](difference_type m) const {
		return coll->GetCell<T>(pos + m);
	}

	friend inline iterator operator+(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos + n);
	}

	friend inline iterator operator-(const iterator &a, difference_type n) {
		return iterator(a.coll, a.pos - n);
	}

	friend inline iterator operator+(difference_type n, const iterator &a) {
		return a + n;
	}
	friend inline difference_type operator-(const iterator &a, const iterator &b) {
		return difference_type(a.pos - b.pos);
	}

	friend inline bool operator==(const iterator &a, const iterator &b) {
		return a.pos == b.pos;
	}
	friend inline bool operator!=(const iterator &a, const iterator &b) {
		return a.pos != b.pos;
	}
	friend inline bool operator<(const iterator &a, const iterator &b) {
		return a.pos < b.pos;
	}
	friend inline bool operator<=(const iterator &a, const iterator &b) {
		return a.pos <= b.pos;
	}
	friend inline bool operator>(const iterator &a, const iterator &b) {
		return a.pos > b.pos;
	}
	friend inline bool operator>=(const iterator &a, const iterator &b) {
		return a.pos >= b.pos;
	}

private:
	optional_ptr<WindowInputColumn> coll;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                                 WindowInputExpression &boundary, const idx_t chunk_idx, const FrameBounds &prev) {
	D_ASSERT(!boundary.CellIsNull(chunk_idx));
	const auto val = boundary.GetCell<T>(chunk_idx);

	OperationCompare<T, OP> comp;
	WindowColumnIterator<T> begin(over, order_begin);
	WindowColumnIterator<T> end(over, order_end);

	if (order_begin < prev.first && prev.first < order_end) {
		const auto first = over.GetCell<T>(prev.first);
		if (!comp(val, first)) {
			//	prev.first <= val, so we can start further forward
			begin += (prev.first - order_begin);
		}
	}
	if (order_begin <= prev.second && prev.second < order_end) {
		const auto second = over.GetCell<T>(prev.second);
		if (!comp(second, val)) {
			//	val <= prev.second, so we can end further back
			// (prev.second is the largest peer)
			end -= (order_end - prev.second - 1);
		}
	}

	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(WindowInputColumn &over, const idx_t order_begin, const idx_t order_end,
                            WindowInputExpression &boundary, const idx_t chunk_idx, const FrameBounds &prev) {
	D_ASSERT(boundary.chunk.ColumnCount() == 1);
	D_ASSERT(boundary.chunk.data[0].GetType().InternalType() == over.input_expr.ptype);

	switch (over.input_expr.ptype) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(WindowInputColumn &over, const OrderType range_sense, const idx_t order_begin,
                                   const idx_t order_end, WindowInputExpression &boundary, const idx_t chunk_idx,
                                   const FrameBounds &prev) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_begin, order_end, boundary, chunk_idx, prev);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

void WindowBoundariesState::Update(const idx_t row_idx, WindowInputColumn &range_collection, const idx_t chunk_idx,
                                   WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
                                   const ValidityMask &partition_mask, const ValidityMask &order_mask) {

	if (partition_count + order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		const auto is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		const auto is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!is_same_partition) {
			partition_start = row_idx;
			peer_start = row_idx;

			// find end of partition
			partition_end = input_size;
			if (partition_count) {
				idx_t n = 1;
				partition_end = FindNextStart(partition_mask, partition_start + 1, input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			valid_start = partition_start;
			valid_end = partition_end;

			if ((valid_start < valid_end) && has_preceding_range) {
				// Exclude any leading NULLs
				if (range_collection.CellIsNull(valid_start)) {
					idx_t n = 1;
					valid_start = FindNextStart(order_mask, valid_start + 1, valid_end, n);
				}
			}

			if ((valid_start < valid_end) && has_following_range) {
				// Exclude any trailing NULLs
				if (range_collection.CellIsNull(valid_end - 1)) {
					idx_t n = 1;
					valid_end = FindPrevStart(order_mask, valid_start, valid_end, n);
				}

				//	Reset range hints
				prev.first = valid_start;
				prev.second = valid_end;
			}
		} else if (!is_peer) {
			peer_start = row_idx;
		}

		if (needs_peer) {
			peer_end = partition_end;
			if (order_count) {
				idx_t n = 1;
				peer_end = FindNextStart(order_mask, peer_start + 1, partition_end, n);
			}
		}

	} else {
		//	OVER()
		partition_end = input_size;
		peer_end = partition_end;
	}

	// determine window boundaries depending on the type of expression
	window_start = -1;
	window_end = -1;

	switch (start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		window_start = partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		window_start = peer_start;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		if (!TrySubtractOperator::Operation(int64_t(row_idx), boundary_start.GetCell<int64_t>(chunk_idx),
		                                    window_start)) {
			throw OutOfRangeException("Overflow computing ROWS PRECEDING start");
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		if (!TryAddOperator::Operation(int64_t(row_idx), boundary_start.GetCell<int64_t>(chunk_idx), window_start)) {
			throw OutOfRangeException("Overflow computing ROWS FOLLOWING start");
		}
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.first = FindOrderedRangeBound<true>(range_collection, range_sense, valid_start, row_idx,
			                                         boundary_start, chunk_idx, prev);
			window_start = prev.first;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_start.CellIsNull(chunk_idx)) {
			window_start = peer_start;
		} else {
			prev.first = FindOrderedRangeBound<true>(range_collection, range_sense, row_idx, valid_end, boundary_start,
			                                         chunk_idx, prev);
			window_start = prev.first;
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		window_end = peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		window_end = partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		if (!TrySubtractOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx),
		                                    window_end)) {
			throw OutOfRangeException("Overflow computing ROWS PRECEDING end");
		}
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		if (!TryAddOperator::Operation(int64_t(row_idx + 1), boundary_end.GetCell<int64_t>(chunk_idx), window_end)) {
			throw OutOfRangeException("Overflow computing ROWS FOLLOWING end");
		}
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.second = FindOrderedRangeBound<false>(range_collection, range_sense, valid_start, row_idx,
			                                           boundary_end, chunk_idx, prev);
			window_end = prev.second;
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		if (boundary_end.CellIsNull(chunk_idx)) {
			window_end = peer_end;
		} else {
			prev.second = FindOrderedRangeBound<false>(range_collection, range_sense, row_idx, valid_end, boundary_end,
			                                           chunk_idx, prev);
			window_end = prev.second;
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (window_start < (int64_t)partition_start) {
		window_start = partition_start;
	}
	if (window_start > (int64_t)partition_end) {
		window_start = partition_end;
	}
	if (window_end < (int64_t)partition_start) {
		window_end = partition_start;
	}
	if (window_end > (int64_t)partition_end) {
		window_end = partition_end;
	}

	if (window_start < 0 || window_end < 0) {
		throw InternalException("Failed to compute window boundaries");
	}
}

void WindowBoundariesState::Bounds(DataChunk &bounds, idx_t row_idx, WindowInputColumn &range, const idx_t count,
                                   WindowInputExpression &boundary_start, WindowInputExpression &boundary_end,
                                   const ValidityMask &partition_mask, const ValidityMask &order_mask) {
	bounds.Reset();
	D_ASSERT(bounds.ColumnCount() == 6);
	auto partition_begin_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end_data = FlatVector::GetData<idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin_data = FlatVector::GetData<idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end_data = FlatVector::GetData<idx_t>(bounds.data[PEER_END]);
	auto window_begin_data = FlatVector::GetData<int64_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end_data = FlatVector::GetData<int64_t>(bounds.data[WINDOW_END]);
	for (idx_t chunk_idx = 0; chunk_idx < count; ++chunk_idx, ++row_idx) {
		Update(row_idx, range, chunk_idx, boundary_start, boundary_end, partition_mask, order_mask);
		*partition_begin_data++ = partition_start;
		*partition_end_data++ = partition_end;
		if (needs_peer) {
			*peer_begin_data++ = peer_start;
			*peer_end_data++ = peer_end;
		}
		*window_begin_data++ = window_start;
		*window_end_data++ = window_end;
	}
	bounds.SetCardinality(count);
}

struct WindowExecutor {
	bool IsConstantAggregate();
	bool IsCustomAggregate();

	WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, const ValidityMask &partition_mask,
	               const idx_t count, WindowAggregationMode mode);

	void Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count);
	void Finalize();

	void Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, const ValidityMask &partition_mask,
	              const ValidityMask &order_mask);

	// The function
	BoundWindowExpression &wexpr;
	const WindowAggregationMode mode;

	// Frame management
	WindowBoundariesState state;
	DataChunk bounds;
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	// Expression collections
	DataChunk payload_collection;
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;

	ExpressionExecutor filter_executor;
	SelectionVector filter_sel;

	// LEAD/LAG Evaluation
	WindowInputExpression leadlag_offset;
	WindowInputExpression leadlag_default;

	// evaluate boundaries if present. Parser has checked boundary types.
	WindowInputExpression boundary_start;
	WindowInputExpression boundary_end;

	// evaluate RANGE expressions, if needed
	WindowInputColumn range;

	// IGNORE NULLS
	ValidityMask ignore_nulls;

	// aggregate computation algorithm
	unique_ptr<WindowAggregateState> aggregate_state = nullptr;

protected:
	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);
	void Aggregate(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void RowNumber(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void Rank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void DenseRank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void PercentRank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void CumeDist(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void Ntile(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void LeadLag(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void FirstValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void LastValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
	void NthValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx);
};

bool WindowExecutor::IsConstantAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	//	COUNT(*) is already handled efficiently by segment trees.
	if (wexpr.children.empty()) {
		return false;
	}

	/*
	    The default framing option is RANGE UNBOUNDED PRECEDING, which
	    is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
	    ROW; it sets the frame to be all rows from the partition start
	    up through the current row's last peer (a row that the window's
	    ORDER BY clause considers equivalent to the current row; all
	    rows are peers if there is no ORDER BY). In general, UNBOUNDED
	    PRECEDING means that the frame starts with the first row of the
	    partition, and similarly UNBOUNDED FOLLOWING means that the
	    frame ends with the last row of the partition, regardless of
	    RANGE, ROWS or GROUPS mode. In ROWS mode, CURRENT ROW means that
	    the frame starts or ends with the current row; but in RANGE or
	    GROUPS mode it means that the frame starts or ends with the
	    current row's first or last peer in the ORDER BY ordering. The
	    offset PRECEDING and offset FOLLOWING options vary in meaning
	    depending on the frame mode.
	*/
	switch (wexpr.start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	switch (wexpr.end) {
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	return true;
}

bool WindowExecutor::IsCustomAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	if (!AggregateObject(wexpr).function.window) {
		return false;
	}

	return (mode < WindowAggregationMode::COMBINE);
}

WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, const ValidityMask &partition_mask,
                               const idx_t count, WindowAggregationMode mode)
    : wexpr(wexpr), mode(mode), state(wexpr, count), payload_collection(), payload_executor(context),
      filter_executor(context), leadlag_offset(wexpr.offset_expr.get(), context),
      leadlag_default(wexpr.default_expr.get(), context), boundary_start(wexpr.start_expr.get(), context),
      boundary_end(wexpr.end_expr.get(), context),
      range((state.has_preceding_range || state.has_following_range) ? wexpr.orders[0].expression.get() : nullptr,
            context, count)

{
	// TODO we could evaluate those expressions in parallel

	//	Check for constant aggregate
	if (IsConstantAggregate()) {
		aggregate_state =
		    make_uniq<WindowConstantAggregate>(AggregateObject(wexpr), wexpr.return_type, partition_mask, count);
	} else if (IsCustomAggregate()) {
		aggregate_state = make_uniq<WindowCustomAggregate>(AggregateObject(wexpr), wexpr.return_type, count);
	} else if (wexpr.aggregate) {
		// build a segment tree for frame-adhering aggregates
		// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
		aggregate_state = make_uniq<WindowSegmentTree>(AggregateObject(wexpr), wexpr.return_type, count, mode);
	}

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	if (wexpr.filter_expr) {
		filter_executor.AddExpression(*wexpr.filter_expr);
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	// TODO: child may be a scalar, don't need to materialize the whole collection then

	// evaluate inner expressions of window functions, could be more complex
	PrepareInputExpressions(wexpr.children, payload_executor, payload_chunk);

	auto types = payload_chunk.GetTypes();
	if (!types.empty()) {
		payload_collection.Initialize(Allocator::Get(context), types);
	}

	vector<LogicalType> bounds_types(6, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(context), bounds_types);
}

void WindowExecutor::Sink(DataChunk &input_chunk, const idx_t input_idx, const idx_t total_count) {
	// Single pass over the input to produce the global data.
	// Vectorisation for the win...

	// Set up a validity mask for IGNORE NULLS
	bool check_nulls = false;
	if (wexpr.ignore_nulls) {
		switch (wexpr.type) {
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_NTH_VALUE:
			check_nulls = true;
			break;
		default:
			break;
		}
	}

	const auto count = input_chunk.size();

	idx_t filtered = 0;
	SelectionVector *filtering = nullptr;
	if (wexpr.filter_expr) {
		filtering = &filter_sel;
		filtered = filter_executor.SelectExpression(input_chunk, filter_sel);
	}

	if (!wexpr.children.empty()) {
		payload_chunk.Reset();
		payload_executor.Execute(input_chunk, payload_chunk);
		payload_chunk.Verify();
		if (aggregate_state) {
			aggregate_state->Sink(payload_chunk, filtering, filtered);
		} else {
			payload_collection.Append(payload_chunk, true);
		}

		// process payload chunks while they are still piping hot
		if (check_nulls) {
			UnifiedVectorFormat vdata;
			payload_chunk.data[0].ToUnifiedFormat(count, vdata);
			if (!vdata.validity.AllValid()) {
				//	Lazily materialise the contents when we find the first NULL
				if (ignore_nulls.AllValid()) {
					ignore_nulls.Initialize(total_count);
				}
				// Write to the current position
				if (input_idx % ValidityMask::BITS_PER_VALUE == 0) {
					// If we are at the edge of an output entry, just copy the entries
					auto dst = ignore_nulls.GetData() + ignore_nulls.EntryCount(input_idx);
					auto src = vdata.validity.GetData();
					for (auto entry_count = vdata.validity.EntryCount(count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				} else {
					// If not, we have ragged data and need to copy one bit at a time.
					for (idx_t i = 0; i < count; ++i) {
						ignore_nulls.Set(input_idx + i, vdata.validity.RowIsValid(i));
					}
				}
			}
		}
	} else if (aggregate_state) {
		//	Zero-argument aggregate (e.g., COUNT(*)
		payload_chunk.SetCardinality(input_chunk);
		aggregate_state->Sink(payload_chunk, filtering, filtered);
	}

	range.Append(input_chunk);
}

void WindowExecutor::Finalize() {
	if (aggregate_state) {
		aggregate_state->Finalize();
	}
}

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &input_chunk, Vector &result, const ValidityMask &partition_mask,
                              const ValidityMask &order_mask) {
	// Evaluate the row-level arguments
	boundary_start.Execute(input_chunk);
	boundary_end.Execute(input_chunk);

	leadlag_offset.Execute(input_chunk);
	leadlag_default.Execute(input_chunk);

	const auto count = input_chunk.size();
	bounds.Reset();
	state.Bounds(bounds, row_idx, range, input_chunk.size(), boundary_start, boundary_end, partition_mask, order_mask);

	switch (wexpr.type) {
	case ExpressionType::WINDOW_AGGREGATE:
		Aggregate(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_ROW_NUMBER:
		RowNumber(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_RANK_DENSE:
		DenseRank(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_RANK:
		Rank(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_PERCENT_RANK:
		PercentRank(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_CUME_DIST:
		CumeDist(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_NTILE:
		Ntile(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		LeadLag(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_FIRST_VALUE:
		FirstValue(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_LAST_VALUE:
		LastValue(bounds, result, count, row_idx);
		break;
	case ExpressionType::WINDOW_NTH_VALUE:
		NthValue(bounds, result, count, row_idx);
		break;
	default:
		throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr.type));
	}

	result.Verify(count);
}

void WindowExecutor::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
	if (partition_begin == row_idx) {
		dense_rank = 1;
		rank = 1;
		rank_equal = 0;
	} else if (peer_begin == row_idx) {
		dense_rank++;
		rank += rank_equal;
		rank_equal = 0;
	}
	rank_equal++;
}

void WindowExecutor::Aggregate(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(aggregate_state);
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	aggregate_state->Evaluate(window_begin, window_end, result, count);
}

void WindowExecutor::RowNumber(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = row_idx - partition_begin[i] + 1;
	}
}

void WindowExecutor::Rank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = rank;
	}
}

void WindowExecutor::DenseRank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = dense_rank;
	}
}

void WindowExecutor::PercentRank(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		int64_t denom = partition_end[i] - partition_begin[i] - 1;
		double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

void WindowExecutor::CumeDist(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		NextRank(partition_begin[i], peer_begin[i], row_idx);
		int64_t denom = partition_end[i] - partition_begin[i];
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
	}
}

void WindowExecutor::Ntile(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(payload_collection.ColumnCount() == 1);
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (CellIsNull(payload_collection, 0, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
			if (n_param < 1) {
				throw InvalidInputException("Argument for ntile must be greater than zero");
			}
			// With thanks from SQLite's ntileValueFunc()
			int64_t n_total = partition_end[i] - partition_begin[i];
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= partition_begin[i]);
			int64_t adjusted_row_idx = row_idx - partition_begin[i];
			// now compute the ntile
			int64_t n_large = n_total - n_param * n_size;
			int64_t i_small = n_large * (n_size + 1);
			int64_t result_ntile;

			D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

			if (adjusted_row_idx < i_small) {
				result_ntile = 1 + adjusted_row_idx / (n_size + 1);
			} else {
				result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
			}
			// result has to be between [1, NTILE]
			D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
			rdata[i] = result_ntile;
		}
	}
}

void WindowExecutor::LeadLag(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto partition_begin = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(bounds.data[PARTITION_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		int64_t offset = 1;
		if (wexpr.offset_expr) {
			offset = leadlag_offset.GetCell<int64_t>(i);
		}
		int64_t val_idx = (int64_t)row_idx;
		if (wexpr.type == ExpressionType::WINDOW_LEAD) {
			val_idx += offset;
		} else {
			val_idx -= offset;
		}

		idx_t delta = 0;
		if (val_idx < (int64_t)row_idx) {
			// Count backwards
			delta = idx_t(row_idx - val_idx);
			val_idx = FindPrevStart(ignore_nulls, partition_begin[i], row_idx, delta);
		} else if (val_idx > (int64_t)row_idx) {
			delta = idx_t(val_idx - row_idx);
			val_idx = FindNextStart(ignore_nulls, row_idx + 1, partition_end[i], delta);
		}
		// else offset is zero, so don't move.

		if (!delta) {
			CopyCell(payload_collection, 0, val_idx, result, i);
		} else if (wexpr.default_expr) {
			leadlag_default.CopyCell(result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowExecutor::FirstValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
			continue;
		}
		//	Same as NTH_VALUE(..., 1)
		idx_t n = 1;
		const auto first_idx = FindNextStart(ignore_nulls, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, first_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowExecutor::LastValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
			continue;
		}
		idx_t n = 1;
		const auto last_idx = FindPrevStart(ignore_nulls, window_begin[i], window_end[i], n);
		if (!n) {
			CopyCell(payload_collection, 0, last_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void WindowExecutor::NthValue(DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	D_ASSERT(payload_collection.ColumnCount() == 2);

	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto &rmask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (window_begin[i] >= window_end[i]) {
			rmask.SetInvalid(i);
			continue;
		}
		// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
		// returns NULL if there is no such row.
		if (CellIsNull(payload_collection, 1, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
			if (n_param < 1) {
				FlatVector::SetNull(result, i, true);
			} else {
				auto n = idx_t(n_param);
				const auto nth_index = FindNextStart(ignore_nulls, window_begin[i], window_end[i], n);
				if (!n) {
					CopyCell(payload_collection, 0, nth_index, result, i);
				} else {
					FlatVector::SetNull(result, i, true);
				}
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();

	lstate.Sink(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalWindow::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();
	lstate.Combine();

	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowLocalSinkState>(context.client, gstate);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<WindowGlobalSinkState>(*this, context);
}

SinkFinalizeType PhysicalWindow::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<WindowGlobalSinkState>();

	//	Did we get any data?
	if (!state.global_partition->count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Do we have any sorting to schedule?
	if (state.global_partition->rows) {
		D_ASSERT(!state.global_partition->grouping_data);
		return state.global_partition->rows->count ? SinkFinalizeType::READY : SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Find the first group to sort
	auto &groups = state.global_partition->grouping_data->GetPartitions();
	if (groups.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared<PartitionMergeEvent>(*state.global_partition, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	explicit WindowGlobalSourceState(WindowGlobalSinkState &gsink) : gsink(*gsink.global_partition), next_bin(0) {
	}

	PartitionGlobalSinkState &gsink;
	//! The output read position.
	atomic<idx_t> next_bin;

public:
	idx_t MaxThreads() override {
		// If there is only one partition, we have to process it on one thread.
		if (!gsink.grouping_data) {
			return 1;
		}

		// If there is not a lot of data, process serially.
		if (gsink.count < STANDARD_ROW_GROUPS_SIZE) {
			return 1;
		}

		return gsink.hash_groups.size();
	}
};

// Per-thread read state
class WindowLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;
	using WindowExecutorPtr = unique_ptr<WindowExecutor>;
	using WindowExecutors = vector<WindowExecutorPtr>;

	WindowLocalSourceState(const PhysicalWindow &op_p, ExecutionContext &context, WindowGlobalSourceState &gsource)
	    : context(context.client), op(op_p), gsink(gsource.gsink) {

		vector<LogicalType> output_types;
		for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
			D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
			output_types.emplace_back(wexpr.return_type);
		}
		output_chunk.Initialize(Allocator::Get(context.client), output_types);

		const auto &input_types = gsink.payload_types;
		layout.Initialize(input_types);
		input_chunk.Initialize(gsink.allocator, input_types);
	}

	void MaterializeSortedData();
	void GeneratePartition(WindowGlobalSinkState &gstate, const idx_t hash_bin);
	void Scan(DataChunk &chunk);

	HashGroupPtr hash_group;
	ClientContext &context;
	const PhysicalWindow &op;

	PartitionGlobalSinkState &gsink;

	//! The generated input chunks
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> heap;
	RowLayout layout;
	//! The partition boundary mask
	vector<validity_t> partition_bits;
	ValidityMask partition_mask;
	//! The order boundary mask
	vector<validity_t> order_bits;
	ValidityMask order_mask;
	//! The current execution functions
	WindowExecutors window_execs;

	//! The read partition
	idx_t hash_bin;
	//! The read cursor
	unique_ptr<RowDataCollectionScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Buffer for window results
	DataChunk output_chunk;
};

void WindowLocalSourceState::MaterializeSortedData() {
	auto &global_sort_state = *hash_group->global_sort;
	if (global_sort_state.sorted_blocks.empty()) {
		return;
	}

	// scan the sorted row data
	D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
	auto &sb = *global_sort_state.sorted_blocks[0];

	// Free up some memory before allocating more
	sb.radix_sorting_data.clear();
	sb.blob_sorting_data = nullptr;

	// Move the sorting row blocks into our RDCs
	auto &buffer_manager = global_sort_state.buffer_manager;
	auto &sd = *sb.payload_data;

	// Data blocks are required
	D_ASSERT(!sd.data_blocks.empty());
	auto &block = sd.data_blocks[0];
	rows = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
	rows->blocks = std::move(sd.data_blocks);
	rows->count = std::accumulate(rows->blocks.begin(), rows->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });

	// Heap blocks are optional, but we want both for iteration.
	if (!sd.heap_blocks.empty()) {
		auto &block = sd.heap_blocks[0];
		heap = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
		heap->blocks = std::move(sd.heap_blocks);
		hash_group.reset();
	} else {
		heap = make_uniq<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

void WindowLocalSourceState::GeneratePartition(WindowGlobalSinkState &gstate, const idx_t hash_bin_p) {
	//	Get rid of any stale data
	hash_bin = hash_bin_p;

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	idx_t count = 0;
	if (hash_bin < gsink.hash_groups.size() && gsink.hash_groups[hash_bin]) {
		count = gsink.hash_groups[hash_bin]->count;
	} else if (gsink.rows && !hash_bin) {
		count = gsink.count;
	} else {
		return;
	}

	//	Initialise masks to false
	const auto bit_count = ValidityMask::ValidityMaskSize(count);
	partition_bits.clear();
	partition_bits.resize(bit_count, 0);
	partition_mask.Initialize(partition_bits.data());

	order_bits.clear();
	order_bits.resize(bit_count, 0);
	order_mask.Initialize(order_bits.data());

	// Scan the sorted data into new Collections
	auto external = gsink.external;
	if (gsink.rows && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		order_mask.SetValidUnsafe(0);
		//	No partition - align the heap blocks with the row blocks
		rows = gsink.rows->CloneEmpty(gsink.rows->keep_pinned);
		heap = gsink.strings->CloneEmpty(gsink.strings->keep_pinned);
		RowDataCollectionScanner::AlignHeapBlocks(*rows, *heap, *gsink.rows, *gsink.strings, layout);
		external = true;
	} else if (hash_bin < gsink.hash_groups.size() && gsink.hash_groups[hash_bin]) {
		// Overwrite the collections with the sorted data
		hash_group = std::move(gsink.hash_groups[hash_bin]);
		hash_group->ComputeMasks(partition_mask, order_mask);
		external = hash_group->global_sort->external;
		MaterializeSortedData();
	} else {
		return;
	}

	// Create the executors for each function
	window_execs.clear();
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto wexec = make_uniq<WindowExecutor>(wexpr, context, partition_mask, count, gstate.mode);
		window_execs.emplace_back(std::move(wexec));
	}

	//	First pass over the input without flushing
	//	TODO: Factor out the constructor data as global state
	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, false);
	idx_t input_idx = 0;
	while (true) {
		input_chunk.Reset();
		scanner->Scan(input_chunk);
		if (input_chunk.size() == 0) {
			break;
		}

		//	TODO: Parallelization opportunity
		for (auto &wexec : window_execs) {
			wexec->Sink(input_chunk, input_idx, scanner->Count());
		}
		input_idx += input_chunk.size();
	}

	//	TODO: Parallelization opportunity
	for (auto &wexec : window_execs) {
		wexec->Finalize();
	}

	// External scanning assumes all blocks are swizzled.
	scanner->ReSwizzle();

	//	Second pass can flush
	scanner->Reset(true);
}

void WindowLocalSourceState::Scan(DataChunk &result) {
	D_ASSERT(scanner);
	if (!scanner->Remaining()) {
		return;
	}

	const auto position = scanner->Scanned();
	input_chunk.Reset();
	scanner->Scan(input_chunk);

	output_chunk.Reset();
	for (idx_t expr_idx = 0; expr_idx < window_execs.size(); ++expr_idx) {
		auto &executor = *window_execs[expr_idx];
		executor.Evaluate(position, input_chunk, output_chunk.data[expr_idx], partition_mask, order_mask);
	}
	output_chunk.SetCardinality(input_chunk);
	output_chunk.Verify();

	idx_t out_idx = 0;
	result.SetCardinality(input_chunk);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(input_chunk.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < output_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(output_chunk.data[col_idx]);
	}
	result.Verify();
}

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<WindowGlobalSourceState>();
	return make_uniq<WindowLocalSourceState>(*this, context, gstate);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowGlobalSourceState>(gsink);
}

SourceResultType PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &lsource = input.local_state.Cast<WindowLocalSourceState>();
	auto &gsource = input.global_state.Cast<WindowGlobalSourceState>();
	auto &gsink = sink_state->Cast<WindowGlobalSinkState>();

	auto &hash_groups = gsink.global_partition->hash_groups;
	const auto bin_count = hash_groups.empty() ? 1 : hash_groups.size();

	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner || !lsource.scanner->Remaining()) {
			lsource.scanner.reset();
			lsource.rows.reset();
			lsource.heap.reset();
			lsource.hash_group.reset();
			auto hash_bin = gsource.next_bin++;
			if (hash_bin >= bin_count) {
				return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
			}

			for (; hash_bin < hash_groups.size(); hash_bin = gsource.next_bin++) {
				if (hash_groups[hash_bin]) {
					break;
				}
			}
			lsource.GeneratePartition(gsink, hash_bin);
		}

		lsource.Scan(chunk);
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb

#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/array.hpp"
#include "duckdb/common/arrow.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace duckdb {

struct DuckDBArrowArrayChildHolder {
	ArrowArray array;
	//! need max three pointers for strings
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	unique_ptr<Vector> vector;
	unique_ptr<data_t[]> offsets;
	unique_ptr<data_t[]> data;
	//! Children of nested structures
	::duckdb::vector<DuckDBArrowArrayChildHolder> children;
	::duckdb::vector<ArrowArray *> children_ptrs;
};

struct DuckDBArrowArrayHolder {
	vector<DuckDBArrowArrayChildHolder> children = {};
	vector<ArrowArray *> children_ptrs = {};
	array<const void *, 1> buffers = {{nullptr}};
	vector<shared_ptr<ArrowArrayWrapper>> arrow_original_array;
};

static void ReleaseDuckDBArrowArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = static_cast<DuckDBArrowArrayHolder *>(array->private_data);
	delete holder;
}

struct ArrowUUIDConversion {
	using internal_type_t = hugeint_t;

	static unique_ptr<Vector> InitializeVector(Vector &data, idx_t size) {
		return make_unique<Vector>(LogicalType::VARCHAR, size);
	}

	static idx_t GetStringLength(hugeint_t value) {
		return UUID::STRING_SIZE;
	}

	static string_t ConvertValue(Vector &tgt_vec, string_t *tgt_ptr, internal_type_t *src_ptr, idx_t row) {
		auto str_value = UUID::ToString(src_ptr[row]);
		// Have to store this string
		tgt_ptr[row] = StringVector::AddStringOrBlob(tgt_vec, str_value);
		return tgt_ptr[row];
	}
};

struct ArrowVarcharConversion {
	using internal_type_t = string_t;

	static unique_ptr<Vector> InitializeVector(Vector &data, idx_t size) {
		return make_unique<Vector>(data);
	}
	static idx_t GetStringLength(string_t value) {
		return value.GetSize();
	}

	static string_t ConvertValue(Vector &tgt_vec, string_t *tgt_ptr, internal_type_t *src_ptr, idx_t row) {
		return src_ptr[row];
	}
};


struct ArrowChunkConverter {
	void InitializeChild(DuckDBArrowArrayChildHolder &child_holder, idx_t size) {
		auto &child = child_holder.array;
		child.private_data = nullptr;
		child.release = ReleaseDuckDBArrowArray;
		child.n_children = 0;
		child.null_count = 0;
		child.offset = 0;
		child.dictionary = nullptr;
		child.buffers = child_holder.buffers.data();

		child.length = size;
	}

	void ConvertChildValidityMask(Vector &vector, ArrowArray &child) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto &mask = FlatVector::Validity(vector);
		if (!mask.AllValid()) {
			//! any bits are set: might have nulls
			child.null_count = -1;
		} else {
			//! no bits are set; we know there are no nulls
			child.null_count = 0;
		}
		child.buffers[0] = (void *)mask.GetData();
	}

	void ForceContiguousList(Vector &v, idx_t size) {
		switch(v.GetType().InternalType()) {
		case PhysicalType::LIST:
			break;
		case PhysicalType::STRUCT: {
			for(auto &entry : StructVector::GetEntries(v)) {
				ForceContiguousList(*entry, size);
			}
			return;
		}
		default:
			return;
		}
		D_ASSERT(v.GetType().id() == LogicalTypeId::LIST);
		bool list_is_contiguous;

		auto list_data = FlatVector::GetData<list_entry_t>(v);
		auto list_mask = FlatVector::Validity(v);
		idx_t current_offset = 0;
		list_is_contiguous = true;
		for (idx_t i = 0; i < size; i++) {
			if (!list_mask.RowIsValid(i)) {
				continue;
			}
			auto &le = list_data[i];
			if (le.offset != current_offset) {
				// arrow requires lists to be contiguous
				// if our list is not contiguous, we need to create a copy
				list_is_contiguous = false;
			}
			current_offset += le.length;
		}
		if (!list_is_contiguous) {
			// the list is not contiguous! flatten it
			idx_t total_child_count = current_offset;
			auto &current_child_vector = ListVector::GetEntry(v);
			auto &child_type = ListType::GetChildType(v.GetType());
			// create a selection vector for the slice
			SelectionVector sel(total_child_count);
			idx_t current_offset = 0;
			for (idx_t i = 0; i < size; i++) {
				if (!list_mask.RowIsValid(i)) {
					continue;
				}
				auto &le = list_data[i];
				for(idx_t k = 0; k < le.length; k++) {
					sel.set_index(current_offset++, le.offset + k);
				}
			}
			// slice + flatten the child
			auto new_child_vector = make_unique<Vector>(child_type);
			new_child_vector->Slice(current_child_vector, sel, total_child_count);
			new_child_vector->Flatten(total_child_count);
			ListVector::GetEntry(v).Reference(*new_child_vector);
		}
		// recurse into child
		ForceContiguousList(ListVector::GetEntry(v), ListVector::GetListSize(v));
	}

	void ConvertList(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		child_holder.vector = make_unique<Vector>(data);

		//! Lists have two buffers
		child.n_buffers = 2;
		//! Second Buffer is the list offsets
		child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
		child.buffers[1] = child_holder.offsets.get();
		auto offset_ptr = (uint32_t *)child.buffers[1];
		auto list_data = FlatVector::GetData<list_entry_t>(data);
		auto list_mask = FlatVector::Validity(data);
		idx_t offset = 0;
		offset_ptr[0] = 0;
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			if (list_mask.RowIsValid(i)) {
				offset += le.length;
			}
			offset_ptr[i + 1] = offset;
		}
		auto list_size = ListVector::GetListSize(data);
		child_holder.children.resize(1);
		InitializeChild(child_holder.children[0], list_size);
		child.n_children = 1;
		child_holder.children_ptrs.push_back(&child_holder.children[0].array);
		child.children = &child_holder.children_ptrs[0];
		auto &child_vector = ListVector::GetEntry(data);
		auto &child_type = ListType::GetChildType(type);
		ConvertArrowChild(child_holder.children[0], child_type, child_vector, list_size);
		ConvertChildValidityMask(child_vector, child_holder.children[0].array);

	}

	void ConvertStruct(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		child_holder.vector = make_unique<Vector>(data);

		//! Structs only have validity buffers
		child.n_buffers = 1;
		auto &children = StructVector::GetEntries(*child_holder.vector);
		child.n_children = children.size();
		child_holder.children.resize(child.n_children);
		for (auto &struct_child : child_holder.children) {
			InitializeChild(struct_child, size);
			child_holder.children_ptrs.push_back(&struct_child.array);
		}
		child.children = &child_holder.children_ptrs[0];
		for (idx_t child_idx = 0; child_idx < child_holder.children.size(); child_idx++) {
			ConvertArrowChild(child_holder.children[child_idx], StructType::GetChildType(type, child_idx), *children[child_idx],
						  size);
			ConvertChildValidityMask(*children[child_idx], child_holder.children[child_idx].array);
		}
	}

	void ConvertStructMap(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		child_holder.vector = make_unique<Vector>(data);

		//! Structs only have validity buffers
		child.n_buffers = 1;
		auto &children = StructVector::GetEntries(*child_holder.vector);
		child.n_children = children.size();
		child_holder.children.resize(child.n_children);
		auto list_size = ListVector::GetListSize(*children[0]);
		child.length = list_size;
		for (auto &struct_child : child_holder.children) {
			InitializeChild(struct_child, list_size);
			child_holder.children_ptrs.push_back(&struct_child.array);
		}
		child.children = &child_holder.children_ptrs[0];
		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t child_idx = 0; child_idx < child_holder.children.size(); child_idx++) {
			auto &list_vector_child = ListVector::GetEntry(*children[child_idx]);
			if (child_idx == 0) {
				UnifiedVectorFormat list_data;
				children[child_idx]->ToUnifiedFormat(size, list_data);
				auto list_child_validity = FlatVector::Validity(list_vector_child);
				if (!list_child_validity.AllValid()) {
					//! Get the offsets to check from the selection vector
					auto list_offsets = FlatVector::GetData<list_entry_t>(*children[child_idx]);
					for (idx_t list_idx = 0; list_idx < size; list_idx++) {
						auto idx = list_data.sel->get_index(list_idx);
						if (!list_data.validity.RowIsValid(idx)) {
							continue;
						}
						auto offset = list_offsets[idx];
						if (!list_child_validity.CheckAllValid(offset.length + offset.offset, offset.offset)) {
							throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
						}
					}
				}
			} else {
				ConvertChildValidityMask(list_vector_child, child_holder.children[child_idx].array);
			}
			ConvertArrowChild(child_holder.children[child_idx], ListType::GetChildType(child_types[child_idx].second),
						  list_vector_child, list_size);
		}
	}

	template <class CONVERT, class VECTOR_TYPE>
	void ConvertVarchar(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		child_holder.vector = CONVERT::InitializeVector(data, size);
		auto target_data_ptr = FlatVector::GetData<string_t>(data);
		child.n_buffers = 3;
		child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
		child.buffers[1] = child_holder.offsets.get();
		D_ASSERT(child.buffers[1]);
		//! step 1: figure out total string length:
		idx_t total_string_length = 0;
		auto source_ptr = FlatVector::GetData<VECTOR_TYPE>(data);
		auto &mask = FlatVector::Validity(data);
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (!mask.RowIsValid(row_idx)) {
				continue;
			}
			total_string_length += CONVERT::GetStringLength(source_ptr[row_idx]);
		}
		//! step 2: allocate this much
		child_holder.data = unique_ptr<data_t[]>(new data_t[total_string_length]);
		child.buffers[2] = child_holder.data.get();
		D_ASSERT(child.buffers[2]);
		//! step 3: assign buffers
		idx_t current_heap_offset = 0;
		auto target_ptr = (uint32_t *)child.buffers[1];

		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			target_ptr[row_idx] = current_heap_offset;
			if (!mask.RowIsValid(row_idx)) {
				continue;
			}
			string_t str = CONVERT::ConvertValue(*child_holder.vector, target_data_ptr, source_ptr, row_idx);
			memcpy((void *)((uint8_t *)child.buffers[2] + current_heap_offset), str.GetDataUnsafe(), str.GetSize());
			current_heap_offset += str.GetSize();
		}
		target_ptr[size] = current_heap_offset; //! need to terminate last string!
	}

	void ConvertBoolean(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		//! Gotta bitpack these booleans
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		idx_t num_bytes = (size + 8 - 1) / 8;
		child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(uint8_t) * num_bytes]);
		child.buffers[1] = child_holder.data.get();
		auto source_ptr = FlatVector::GetData<uint8_t>(*child_holder.vector);
		auto target_ptr = (uint8_t *)child.buffers[1];
		idx_t target_pos = 0;
		idx_t cur_bit = 0;
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (cur_bit == 8) {
				target_pos++;
				cur_bit = 0;
			}
			if (source_ptr[row_idx] == 0) {
				//! We set the bit to 0
				target_ptr[target_pos] &= ~(1 << cur_bit);
			} else {
				//! We set the bit to 1
				target_ptr[target_pos] |= 1 << cur_bit;
			}
			cur_bit++;
		}
	}

	void ConvertDecimal(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		child.n_buffers = 2;
		child_holder.vector = make_unique<Vector>(data);

		//! We have to convert to INT128
		switch (type.InternalType()) {
		case PhysicalType::INT16: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int16_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT32: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int32_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT64: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int64_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT128: {
			child.buffers[1] = (void *)FlatVector::GetData(*child_holder.vector);
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" + TypeIdToString(type.InternalType()));
		}
	}

	void ConvertInterval(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		//! convert interval from month/days/ucs to milliseconds
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(int64_t) * (size)]);
		child.buffers[1] = child_holder.data.get();
		auto source_ptr = FlatVector::GetData<interval_t>(*child_holder.vector);
		auto target_ptr = (int64_t *)child.buffers[1];
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			target_ptr[row_idx] = Interval::GetMilli(source_ptr[row_idx]);
		}
	}

	void ConvertEnum(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		// We need to initialize our dictionary
		child_holder.children.resize(1);
		idx_t dict_size = EnumType::GetSize(type);
		InitializeChild(child_holder.children[0], dict_size);
		Vector dictionary(EnumType::GetValuesInsertOrder(type));
		ConvertArrowChild(child_holder.children[0], dictionary.GetType(), dictionary, dict_size);
		child_holder.children_ptrs.push_back(&child_holder.children[0].array);

		// now we set the data
		child.dictionary = child_holder.children_ptrs[0];
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child.buffers[1] = (void *) FlatVector::GetData(*child_holder.vector);
	}

	void ConvertZeroCopy(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data) {
		auto &child = child_holder.array;

		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child.buffers[1] = (void *)FlatVector::GetData(*child_holder.vector);
	}

	void ConvertArrowChild(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
		auto &child = child_holder.array;
		switch (type.id()) {
		case LogicalTypeId::BOOLEAN: {
			ConvertBoolean(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIME_TZ:
			ConvertZeroCopy(child_holder, type, data);
			break;
		case LogicalTypeId::SQLNULL:
			child.n_buffers = 1;
			break;
		case LogicalTypeId::DECIMAL: {
			ConvertDecimal(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::BLOB:
		case LogicalTypeId::JSON:
		case LogicalTypeId::VARCHAR: {
			ConvertVarchar<ArrowVarcharConversion, string_t>(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::UUID: {
			ConvertVarchar<ArrowUUIDConversion, hugeint_t>(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::LIST: {
			ConvertList(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::STRUCT: {
			ConvertStruct(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::MAP: {
			child_holder.vector = make_unique<Vector>(data);

			auto &map_mask = FlatVector::Validity(*child_holder.vector);
			child.n_buffers = 2;
			//! Maps have one child
			child.n_children = 1;
			child_holder.children.resize(1);
			InitializeChild(child_holder.children[0], size);
			child_holder.children_ptrs.push_back(&child_holder.children[0].array);
			//! Second Buffer is the offsets
			child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
			child.buffers[1] = child_holder.offsets.get();
			auto &struct_children = StructVector::GetEntries(data);
			auto offset_ptr = (uint32_t *)child.buffers[1];
			auto list_data = FlatVector::GetData<list_entry_t>(*struct_children[0]);
			idx_t offset = 0;
			offset_ptr[0] = 0;
			for (idx_t i = 0; i < size; i++) {
				auto &le = list_data[i];
				if (map_mask.RowIsValid(i)) {
					offset += le.length;
				}
				offset_ptr[i + 1] = offset;
			}
			child.children = &child_holder.children_ptrs[0];
			//! We need to set up a struct
			auto struct_type = LogicalType::STRUCT(StructType::GetChildTypes(type));

			ConvertStructMap(child_holder.children[0], struct_type, *child_holder.vector, size);
			break;
		}
		case LogicalTypeId::INTERVAL: {
			ConvertInterval(child_holder, type, data, size);
			break;
		}
		case LogicalTypeId::ENUM: {
			ConvertEnum(child_holder, type, data, size);
			break;
		}
		default:
			throw std::runtime_error("Unsupported type " + type.ToString());
		}
	}
};

void DataChunk::ToArrowArray(ArrowArray *out_array) {
	Flatten();
	D_ASSERT(out_array);

	// Allocate as unique_ptr first to cleanup properly on error
	auto root_holder = make_unique<DuckDBArrowArrayHolder>();

	// Allocate the children
	root_holder->children.resize(ColumnCount());
	root_holder->children_ptrs.resize(ColumnCount(), nullptr);
	for (size_t i = 0; i < ColumnCount(); ++i) {
		root_holder->children_ptrs[i] = &root_holder->children[i].array;
	}
	out_array->children = root_holder->children_ptrs.data();
	out_array->n_children = ColumnCount();

	// Configure root array
	out_array->length = size();
	out_array->n_children = ColumnCount();
	out_array->n_buffers = 1;
	out_array->buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	out_array->offset = 0;
	out_array->null_count = 0; // needs to be 0
	out_array->dictionary = nullptr;

	//! Configure child arrays
	ArrowChunkConverter converter;
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		converter.ForceContiguousList(data[col_idx], size());

		auto &child_holder = root_holder->children[col_idx];
		converter.InitializeChild(child_holder, size());
		auto &vector = child_holder.vector;
		auto &child = child_holder.array;
		auto vec_buffer = data[col_idx].GetBuffer();
		if (vec_buffer->GetAuxiliaryData() &&
		    vec_buffer->GetAuxiliaryDataType() == VectorAuxiliaryDataType::ARROW_AUXILIARY) {
			auto arrow_aux_data = (ArrowAuxiliaryData *)vec_buffer->GetAuxiliaryData();
			root_holder->arrow_original_array.push_back(arrow_aux_data->arrow_array);
		}
		//! We could, in theory, output other types of vectors here, currently only FLAT Vectors
		converter.ConvertArrowChild(child_holder, GetTypes()[col_idx], data[col_idx], size());
		converter.ConvertChildValidityMask(*vector, child);
		out_array->children[col_idx] = &child;
	}

	// Release ownership to caller
	out_array->private_data = root_holder.release();
	out_array->release = ReleaseDuckDBArrowArray;
}

}

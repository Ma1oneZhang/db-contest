/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "common/common.h"
#include "defs.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_defs.h"
#include "record/rm_file_handle.h"
#include "system/sm.h"
#include "system/sm_meta.h"
#include <cstring>
#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
	std::unique_ptr<AbstractExecutor> left_;           // 左儿子节点（需要join的表）
	std::unique_ptr<AbstractExecutor> right_;          // 右儿子节点（需要join的表）
	size_t len_;                                       // join后获得的每条记录的长度
	std::vector<ColMeta> cols_;                        // join后获得的记录的字段
	std::vector<std::unique_ptr<RmRecord>> buffer_vec_;// block join buffer
	static constexpr int MAX_BUFFER_SIZE = 2048;          // block join buffer size
	int64_t current_buffer_pos;                        // pos in the vector -1 means not exist
	std::map<std::pair<std::string, std::string>,
					 std::pair<bool, std::vector<ColMeta>::const_iterator>>
		table_col_table;// 记录对应col的表名和指针

	bool is_join;
	std::vector<Condition> fed_conds_;// join条件

	std::unique_ptr<RmRecord> current_;
	std::unordered_map<CompOp, CompOp> swapOp = {
		{OP_GE, OP_LE},
		{OP_LE, OP_GE},
		{OP_EQ, OP_EQ},
		{OP_NE, OP_NE},
		{OP_GT, OP_LT},
		{OP_LT, OP_GT}};

	__inline__ std::pair<bool, std::vector<ColMeta>::const_iterator> findByColMeta(const ColMeta &meta) {
		return table_col_table[{meta.tab_name, meta.name}];
	}

	void getNextBlockBuffer() {
		buffer_vec_.clear();
		for (size_t i = 0; i < MAX_BUFFER_SIZE && !left_->is_end(); i++, left_->nextTuple()) {
			buffer_vec_.emplace_back(left_->Next());
		}
	}

	bool checkCondition(const std::unique_ptr<RmRecord> &left, const std::unique_ptr<RmRecord> &right) {
		for (auto &cond: fed_conds_) {
			auto col = get_col(cols_, cond.lhs_col);
			auto lhs = left->data + col->offset;
			char *rhs;
			ColType rhs_type;
			if (cond.is_rhs_val) {
				rhs_type = cond.rhs_val.type;
				rhs = cond.rhs_val.raw->data;
			} else {
				auto [_, rhs_col] = findByColMeta({cond.rhs_col.tab_name, cond.rhs_col.col_name});
				rhs_type = rhs_col->type;
				rhs = right->data + rhs_col->offset;
			}
			int cmp;
			if (col->type == TYPE_INT) {
				// handle int type
				if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((int *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					if (col->type != rhs_type) {
						throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
					}
					cmp = ix_compare((int *) lhs, (int *) rhs, rhs_type, col->len);
				}
			} else if (col->type == TYPE_FLOAT) {
				// handle float type
				if (rhs_type == TYPE_INT) {
					cmp = ix_compare((double *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_FLOAT) {
					cmp = ix_compare((double *) lhs, (double *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((double *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
			} else if (col->type == TYPE_STRING) {
				// handle string type
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare(lhs, rhs, rhs_type, col->len);
			} else if (col->type == TYPE_DATETIME) {
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare(lhs, rhs, rhs_type, col->len);
			} else if (col->type == TYPE_BIGINT) {
				if (rhs_type == TYPE_INT) {
					cmp = ix_compare((int64_t *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((int64_t *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
			} else {
				// somewhere unkonwn
				throw std::logic_error("somewhere unkonwn");
			}
			switch (cond.op) {
				case OP_EQ:
					if (cmp != 0)
						return false;
					break;
				case OP_NE:
					if (cmp == 0)
						return false;
					break;
				case OP_GE:
					if (cmp < 0)
						return false;
					break;
				case OP_LE:
					if (cmp > 0)
						return false;
					break;
				case OP_GT:
					if (cmp <= 0)
						return false;
					break;
				case OP_LT:
					if (cmp >= 0)
						return false;
					break;
				default:
					assert(false);
			}
		}
		return true;
	}

public:
	NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
												 std::vector<Condition> conds) {
		left_ = std::move(left);
		right_ = std::move(right);
		len_ = left_->tupleLen() + right_->tupleLen();
		cols_ = left_->cols();
		auto right_cols = right_->cols();
		for (auto &col: right_cols) {
			col.offset += left_->tupleLen();
		}
		cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
		fed_conds_ = std::move(conds);
		is_join = (fed_conds_.size() > 0);
		for (auto &cond: fed_conds_) {
			if (!cond.is_rhs_val) {
				if (left_->cols()[0].tab_name != cond.lhs_col.tab_name) {
					std::swap(cond.lhs_col, cond.rhs_col);
					cond.op = swapOp[cond.op];
				}
			}
		}
		for (size_t i = 0; i < left_->cols().size(); i++) {
			const auto &vec = left_->cols();
			table_col_table[{vec[i].tab_name, vec[i].name}] = {true, left_->cols().begin() + i};
		}
		for (size_t i = 0; i < right_->cols().size(); i++) {
			const auto &vec = right_->cols();
			table_col_table[{vec[i].tab_name, vec[i].name}] = {false, right_->cols().begin() + i};
		}
		current_buffer_pos = -2;
	}

	const std::vector<ColMeta> &cols() override {
		return cols_;
	}

	void beginTuple() override {
		left_->beginTuple();
		if (left_->is_end()) {
			return;
		}
		right_->beginTuple();
		if (right_->is_end()) {
			return;
		}
		getNextBlockBuffer();
		nextTuple();
	}

	void nextTuple() override {
		while (!(left_->is_end() && right_->is_end())) {
			bool next = 0;
			if (current_buffer_pos < 0) {
				if (current_buffer_pos == -1) {
					right_->nextTuple();
					if (right_->is_end() && left_->is_end()) {
						return;
					}
				}
				if (right_->is_end()) {
					right_->beginTuple();
					getNextBlockBuffer();
				}
				current_buffer_pos = 0;
			}
			for (; current_buffer_pos < buffer_vec_.size(); current_buffer_pos++) {
				auto &left_rec = buffer_vec_[current_buffer_pos];
				auto right_rec = right_->Next();
				if (checkCondition(left_rec, right_rec)) {
					current_ = std::make_unique<RmRecord>(len_);
					for (auto col: cols_) {
						auto [is_left, it] = table_col_table[{col.tab_name, col.name}];
						if (is_left) {
							memcpy(current_->data + col.offset, left_rec->data + it->offset, it->len);
						} else {
							memcpy(current_->data + col.offset, right_rec->data + it->offset, it->len);
						}
					}
					next = true;
					break;
				}
			}
			if (next) {
				current_buffer_pos++;
				break;
			} else {
				current_buffer_pos = -1;
			}
		}
	}

	bool is_end() const override {
		return left_->is_end() && right_->is_end();
	}

	std::unique_ptr<RmRecord> Next() override {
		return std::make_unique<RmRecord>(*current_);
	}

	size_t tupleLen() const override {
		return len_;
	}

	std::string getType() override { return "NestedLoopJoinExecutor"; };


	Rid &rid() override { return _abstract_rid; }
};

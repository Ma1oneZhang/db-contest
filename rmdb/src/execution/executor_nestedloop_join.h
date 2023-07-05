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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_defs.h"
#include "record/rm_file_handle.h"
#include "system/sm.h"
#include "system/sm_meta.h"
#include <cstring>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
	std::unique_ptr<AbstractExecutor> left_;                                            // 左儿子节点（需要join的表）
	std::unique_ptr<AbstractExecutor> right_;                                           // 右儿子节点（需要join的表）
	size_t len_;                                                                        // join后获得的每条记录的长度
	std::vector<ColMeta> cols_;                                                         // join后获得的记录的字段
	std::unordered_map<std::string, std::vector<std::unique_ptr<RmRecord>>> hash_table_;// hash join table(we start join from left)
	std::unordered_map<std::string, std::vector<std::unique_ptr<RmRecord>>>::const_iterator join_current_vec_it;
	long long current_vec_pos;// pos in the vector -1 means not exist
	std::map<std::pair<std::string, std::string>,
					 std::pair<bool, std::vector<ColMeta>::const_iterator>>
		table_col_table;// 记录对应col的表名和指针

	bool is_join;
	std::vector<Condition> fed_conds_;// join条件

	std::unique_ptr<RmRecord> current_;

	__inline__ std::pair<bool, std::vector<ColMeta>::const_iterator> findByColMeta(const ColMeta &meta) {
		return table_col_table[{meta.tab_name, meta.name}];
	}

	void preProcessJoinHashTable() {
		// init hash_table
		for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
			auto right_rec = right_->Next();
			std::string bin_data = "prefix:";
			bin_data.reserve(right_->tupleLen());
			for (auto fed_cond: fed_conds_) {
				assert(fed_cond.op == OP_EQ);
				auto [is_left, it] = findByColMeta({fed_cond.rhs_col.tab_name, fed_cond.rhs_col.col_name});
				if (is_left) {
					std::swap(fed_cond.lhs_col, fed_cond.rhs_col);
					it = findByColMeta({fed_cond.rhs_col.tab_name, fed_cond.rhs_col.col_name}).second;
				}
				bin_data += std::string(right_rec->data + it->offset, it->len);
			}
			hash_table_[bin_data].emplace_back(std::make_unique<RmRecord>(*right_rec.get()));
		}
		// init the iterator
		std::string bin_data = "notprefix:";
		while (!left_->is_end() && hash_table_.find(bin_data) == hash_table_.end()) {
			bin_data = "prefix:";
			auto left_rec = left_->Next();
			for (auto fed_cond: fed_conds_) {
				auto [is_left, it] = findByColMeta({fed_cond.lhs_col.tab_name, fed_cond.lhs_col.col_name});
				bin_data += std::string(left_rec->data + it->offset, it->len);
			}
			if (hash_table_.find(bin_data) == hash_table_.end())
				left_->nextTuple();
			else
				break;
		}
		current_vec_pos = 0;
		join_current_vec_it = hash_table_.find(bin_data);
		current_ = std::make_unique<RmRecord>(len_);
		auto left_rec = left_->Next();
		auto &right_rec = join_current_vec_it->second[current_vec_pos];
		for (auto col: cols_) {
			auto [is_left, it] = table_col_table[{col.tab_name, col.name}];
			if (is_left) {
				memcpy(current_->data + col.offset, left_rec->data + it->offset, it->len);
			} else {
				memcpy(current_->data + col.offset, right_rec->data + it->offset, it->len);
			}
		}
		current_vec_pos++;
		if (current_vec_pos == (long long) join_current_vec_it->second.size()) {
			current_vec_pos = -1;
		}
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
		for (size_t i = 0; i < left_->cols().size(); i++) {
			const auto &vec = left_->cols();
			table_col_table[{vec[i].tab_name, vec[i].name}] = {true, left_->cols().begin() + i};
		}
		for (size_t i = 0; i < right_->cols().size(); i++) {
			const auto &vec = right_->cols();
			table_col_table[{vec[i].tab_name, vec[i].name}] = {false, right_->cols().begin() + i};
		}
		current_vec_pos = -1;
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
		current_ = std::make_unique<RmRecord>(len_);
		preProcessJoinHashTable();
	}

	void nextTuple() override {
		if (current_vec_pos == -1) {
			// -1 means EndOfVec
			// move to next tuple try to match the comparsion
			std::string bin_data{};
			left_->nextTuple();
			while (!left_->is_end()) {
				bin_data = "prefix:";
				auto left_rec = left_->Next();
				for (auto fed_cond: fed_conds_) {
					auto [is_left, it] = findByColMeta({fed_cond.lhs_col.tab_name, fed_cond.lhs_col.col_name});
					bin_data += std::string(left_rec->data + it->offset, it->len);
				}
				if (hash_table_.find(bin_data) == hash_table_.end())
					left_->nextTuple();
				else
					break;
			}
			current_vec_pos = 0;
			join_current_vec_it = hash_table_.find(bin_data);
		}
		if (left_->is_end()) {
			return;
		}
		// not equal with -1
		current_ = std::make_unique<RmRecord>(len_);
		auto left_rec = left_->Next();
		auto &right_rec = join_current_vec_it->second[current_vec_pos];
		for (auto col: cols_) {
			auto [is_left, it] = table_col_table[{col.tab_name, col.name}];
			if (is_left) {
				memcpy(current_->data + col.offset, left_rec->data + it->offset, it->len);
			} else {
				memcpy(current_->data + col.offset, right_rec->data + it->offset, it->len);
			}
		}
		current_vec_pos++;
		if (current_vec_pos == (long long) join_current_vec_it->second.size()) {
			current_vec_pos = -1;
		}
	}

	bool is_end() const override {
		return left_->is_end();
	}

	std::unique_ptr<RmRecord> Next() override {
		return std::make_unique<RmRecord>(*current_.get());
	}

	size_t tupleLen() const override {
		return len_;
	}

	Rid &rid() override { return _abstract_rid; }
};

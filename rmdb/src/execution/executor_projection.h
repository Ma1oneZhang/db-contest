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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "record/rm_defs.h"
#include "system/sm.h"
#include <cstring>
#include <utils/ThreadPool.h>
#include <memory>

class ProjectionExecutor : public AbstractExecutor {
private:
	std::unique_ptr<AbstractExecutor> prev_;// 投影节点的儿子节点
	std::vector<ColMeta> cols_;             // 需要投影的字段
	size_t len_;                            // 字段总长度
	std::vector<size_t> sel_idxs_;


	std::vector<std::unique_ptr<RmRecord>> buffer_vec_;// block join buffer
	static constexpr int MAX_BUFFER_SIZE = 2048;       // block join buffer size
	int64_t current_buffer_pos;                        // pos in the vector -1 means not exist
	std::map<std::pair<std::string, std::string>,
					 std::pair<bool, std::vector<ColMeta>::const_iterator>>
		table_col_table;// 记录对应col的表名和指针

	std::vector<std::unique_ptr<RmRecord>> match_tuples;
	std::mutex mu_of_match_tuple;
	std::unique_ptr<ThreadPool> tp;

	void inline getNextBlockBuffer() {
		buffer_vec_.reserve(MAX_BUFFER_SIZE);
		size_t i; 
		for (i = 0; i < MAX_BUFFER_SIZE && !prev_->is_end(); i++, prev_->nextTuple()) {
			buffer_vec_.emplace_back(prev_->Next());
		}
		if (i != MAX_BUFFER_SIZE) buffer_vec_.resize(i); 
	}

	void getNextMatchVector() {
		while (match_tuples.size() == 0 && !prev_->is_end()) {
			getNextBlockBuffer();
			// means scan over
			if (buffer_vec_.size() == 0) {
				continue;
			}
			match_tuples.reserve(buffer_vec_.size()); 
			for (auto &prev_rec : buffer_vec_) {
					auto &prev_cols = prev_->cols();
					auto &project_cols = cols_;
					auto project_rec = std::make_unique<RmRecord>(len_);
					for (size_t i = 0; i < project_cols.size(); i++) {
						auto &prev_idx = sel_idxs_[i];
						auto &prev_col = prev_cols[prev_idx];
						auto &proj_col = project_cols[i];
						memcpy(project_rec->data + proj_col.offset, prev_rec->data + prev_col.offset, prev_col.len);
					}
				
				match_tuples.emplace_back(std::move(project_rec)); 
			}
		}
	}

public:
	ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
		prev_ = std::move(prev);
		size_t curr_offset = 0;
		auto &prev_cols = prev_->cols();

		for (auto &sel_col: sel_cols) {
			auto pos = get_col(prev_cols, sel_col);
			sel_idxs_.push_back(pos - prev_cols.begin());
			auto col = *pos;
			col.offset = curr_offset;
			curr_offset += col.len;
			cols_.push_back(col);
		}

		// 5 thread
		tp = std::make_unique<ThreadPool>(5);
		tp->init();
		current_buffer_pos = -1;
		len_ = curr_offset;
	}

	const std::vector<ColMeta> &cols() override {
		return cols_;
	}

	void beginTuple() override {
		prev_->beginTuple();
		if (prev_->is_end()) {
			return;
		}
		nextTuple();
	}

	void nextTuple() override {
		current_buffer_pos++;
		if (current_buffer_pos == (int64_t)match_tuples.size()) {
			buffer_vec_.clear();
			match_tuples.clear(); 
			getNextMatchVector();
			current_buffer_pos = 0;
		}
	}


	bool is_end() const override {
		return buffer_vec_.size() == 0 && prev_->is_end();
	}
	std::unique_ptr<RmRecord> Next() override {
		assert(!is_end());
		return std::make_unique<RmRecord>(*match_tuples[current_buffer_pos]);
	}

	Rid &rid() override { return _abstract_rid; }
};
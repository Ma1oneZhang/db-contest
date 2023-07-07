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
#include "index/ix_index_handle.h"
#include "record/rm_defs.h"
#include "record/rm_scan.h"
#include "system/sm.h"
#include <memory>
#include <type_traits>
#include <unordered_map>

class SeqScanExecutor : public AbstractExecutor {
private:
	std::string tab_name_;            // 表的名称
	std::vector<Condition> conds_;    // scan的条件
	RmFileHandle *fh_;                // 表的数据文件句柄
	std::vector<ColMeta> cols_;       // scan后生成的记录的字段
	size_t len_;                      // scan后生成的每条记录的长度
	std::vector<Condition> fed_conds_;// 同conds_，两个字段相同
	std::unordered_map<CompOp, CompOp> swapOp = {
		{OP_GE, OP_LE},
		{OP_LE, OP_GE},
		{OP_EQ, OP_EQ},
		{OP_NE, OP_NE},
		{OP_GT, OP_LT},
		{OP_LT, OP_GT}};
	Rid rid_;
	std::unique_ptr<RecScan> scan_;// table_iterator

	SmManager *sm_manager_;

	bool checkCondition(const std::unique_ptr<RmRecord> &rec) {
		for (auto &cond: conds_) {
			auto col = get_col(cols_, cond.lhs_col);
			auto lhs = rec->data + col->offset;
			char *rhs;
			ColType rhs_type;
			if (cond.is_rhs_val) {
				rhs_type = cond.rhs_val.type;
				rhs = cond.rhs_val.raw->data;
			} else {
				auto rhs_col = get_col(cols_, cond.rhs_col);
				rhs_type = rhs_col->type;
				rhs = rec->data + rhs_col->offset;
			}
			int cmp;
			if (col->type == TYPE_INT) {
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare((int *) lhs, (int *) rhs, rhs_type, col->len);
			} else if (col->type == TYPE_FLOAT) {
				if (rhs_type == TYPE_INT) {
					cmp = ix_compare((double *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_FLOAT) {
					cmp = ix_compare((double *) lhs, (double *) rhs, rhs_type, col->len);
				} else {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
			} else if (col->type == TYPE_STRING) {
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare(lhs, rhs, rhs_type, col->len);
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
	SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
		sm_manager_ = sm_manager;
		tab_name_ = std::move(tab_name);
		conds_ = std::move(conds);
		TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
		fh_ = sm_manager_->fhs_.at(tab_name_).get();
		cols_ = tab.cols;
		len_ = cols_.back().offset + cols_.back().len;

		context_ = context;
		for (auto &cond: conds) {
			if (tab_name_ != cond.lhs_col.tab_name) {
				std::swap(cond.lhs_col, cond.rhs_col);
				cond.op = swapOp[cond.op];
			}
		}
		fed_conds_ = conds_;
	}
	const std::vector<ColMeta> &cols() override {
		return cols_;
	}

	size_t tupleLen() const override {
		return len_;
	}
	// Init
	void beginTuple() override {
		scan_ = std::make_unique<RmScan>(fh_);
		rid_ = scan_->rid();
		if (!is_end() && !checkCondition(fh_->get_record(rid_, nullptr))) {
			nextTuple();
		}
	}
	// iterate
	void nextTuple() override {
		for (scan_->next(); !scan_->is_end(); scan_->next()) {
			rid_ = scan_->rid();
			if (checkCondition(fh_->get_record(rid_, nullptr))) {
				break;
			}
		}
	}

	bool is_end() const override {
		return scan_->is_end();
	}

	std::unique_ptr<RmRecord> Next() override {
		return fh_->get_record(rid_, nullptr);
	}

	Rid &rid() override { return rid_; }
};
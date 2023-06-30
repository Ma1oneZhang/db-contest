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
#include "system/sm.h"
#include "system/sm_meta.h"
#include <memory>
#include <vector>

class DeleteExecutor : public AbstractExecutor {
private:
	TabMeta tab_;                 // 表的元数据
	std::vector<Condition> conds_;// delete的条件
	RmFileHandle *fh_;            // 表的数据文件句柄
	std::vector<Rid> rids_;       // 需要删除的记录的位置
	std::string tab_name_;        // 表名称
	SmManager *sm_manager_;

	bool is_executed;
	std::unordered_map<CompOp, CompOp> swapOp = {
		{OP_GE, OP_LE},
		{OP_LE, OP_GE},
		{OP_EQ, OP_EQ},
		{OP_NE, OP_NE},
		{OP_GT, OP_LT},
		{OP_LT, OP_GT}};

	bool checkCondition(const std::unique_ptr<RmRecord> &rec) {
		auto &cols_ = tab_.cols;
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
			if (rhs_type == TYPE_FLOAT) {
				if (col->type == TYPE_INT) {
					cmp = ix_compare((int *) lhs, (double *) rhs, rhs_type, col->len);
				} else if (col->type == TYPE_FLOAT) {
					cmp = ix_compare((double *) lhs, (double *) rhs, rhs_type, col->len);
				}
			} else if (rhs_type == TYPE_INT) {
				if (col->type == TYPE_INT) {
					cmp = ix_compare((int *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (col->type == TYPE_FLOAT) {
					cmp = ix_compare((double *) lhs, (int *) rhs, rhs_type, col->len);
				}
			} else {
				assert(col->type == col->type);
				// force it to compare by bytes
				cmp = ix_compare(lhs, rhs, TYPE_STRING, col->len);
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
					if (cmp > 0)
						return false;
					break;
				default:
					assert(false);
			}
		}
		return true;
	}

public:
	DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
								 std::vector<Rid> rids, Context *context) {
		sm_manager_ = sm_manager;
		tab_name_ = tab_name;
		tab_ = sm_manager_->db_.get_table(tab_name);
		fh_ = sm_manager_->fhs_.at(tab_name).get();
		conds_ = conds;
		rids_ = rids;
		context_ = context;

		is_executed = false;

		for (auto &cond: conds) {
			if (tab_name_ != cond.lhs_col.tab_name) {
				std::swap(cond.lhs_col, cond.rhs_col);
				cond.op = swapOp[cond.op];
			}
		}
	}
	const std::vector<ColMeta> &cols() override {
		std::vector<ColMeta> *cols = nullptr;
		return *cols;
	}
	std::unique_ptr<RmRecord> Next() override {
		for (auto scan = std::make_unique<RmScan>(fh_); !scan->is_end(); scan->next()) {
			if (checkCondition(fh_->get_record(scan->rid(), nullptr))) {
				fh_->delete_record(scan->rid(), nullptr);
			}
		}
		is_executed = true;
		return nullptr;
	}
	bool is_end() const override {
		return is_executed;
	}
	Rid &rid() override { return _abstract_rid; }
};
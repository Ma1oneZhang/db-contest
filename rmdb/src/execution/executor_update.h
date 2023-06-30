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
#include "defs.h"
#include "errors.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include <cstring>

class UpdateExecutor : public AbstractExecutor {
private:
	TabMeta tab_;
	std::vector<Condition> conds_;
	RmFileHandle *fh_;
	std::vector<Rid> rids_;
	std::string tab_name_;
	std::vector<SetClause> set_clauses_;
	SmManager *sm_manager_;

	bool is_executed;
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
	UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
								 std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
		sm_manager_ = sm_manager;
		tab_name_ = tab_name;
		set_clauses_ = set_clauses;
		tab_ = sm_manager_->db_.get_table(tab_name);
		fh_ = sm_manager_->fhs_.at(tab_name).get();
		conds_ = conds;
		rids_ = rids;
		context_ = context;
		is_executed = false;
	}
	std::unique_ptr<RmRecord> Next() override {
		// scan all table
		for (auto scan = std::make_unique<RmScan>(fh_); !scan->is_end(); scan->next()) {
			auto rec = fh_->get_record(scan->rid(), nullptr);
			// check singal record
			if (checkCondition(rec)) {
				// make set_clause apply on it
				for (auto set_clause: set_clauses_) {
					auto col = tab_.get_col(set_clause.lhs.col_name);
					auto &offset = col->offset;
					if (col->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_INT) {
						double *data = (double *) (rec->data + offset);
						*data = set_clause.rhs.int_val;
					} else {
						if (col->type != set_clause.rhs.type) {
							throw IncompatibleTypeError("", "");
						}
						if (col->type == TYPE_STRING && set_clause.rhs.str_val.size() > col->len) {
							throw StringOverflowError();
						}
						memcpy(rec->data + offset, set_clause.rhs.raw->data, col->len);
					}
				}
				fh_->insert_record(scan->rid(), rec->data);
			}
		}
		is_executed = true;
		return nullptr;
	}
	bool is_end() const override {
		return is_executed;
	}
	Rid &rid() override { return _abstract_rid; }
	const std::vector<ColMeta> &cols() override {
		std::vector<ColMeta> *cols = nullptr;
		return *cols;
	}
};
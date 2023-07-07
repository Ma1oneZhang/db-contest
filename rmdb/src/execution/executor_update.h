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
#include "errors.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include <cstring>
#include <stdexcept>

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
				cond.rhs_val.str_resize(col->len);
				rhs = cond.rhs_val.raw->data;
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
	template<typename lhs, typename rhs>
	void doOperation(lhs *l, rhs *r, SetClauseOp op) {
		switch (op) {
			case SetClauseOp::SELF_ADD:
				*l += *r;
				break;
			case SetClauseOp::SELF_SUB:
				*l -= *r;
				break;
			case SetClauseOp::SELF_MUT:
				*l *= *r;
				break;
			case SetClauseOp::SELF_DIV:
				*l /= *r;
				break;
		}
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
					if (!set_clause.is_self) {
						if (col->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_INT) {
							double *data = (double *) (rec->data + offset);
							// using built_in type transfer
							*data = set_clause.rhs.int_val;
						} else {
							if (col->type != set_clause.rhs.type) {
								throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
							}
							if (col->type == TYPE_STRING && set_clause.rhs.str_val.size() > (size_t) col->len) {
								throw StringOverflowError();
							}
							if (col->type == TYPE_STRING) {
								set_clause.rhs.str_resize(col->len);
							}
							memcpy(rec->data + offset, set_clause.rhs.raw->data, col->len);
						}
					} else {
						if (col->type == TYPE_INT) {
							// as logic and sql standard, there is only int operation
							auto lhs = (int *) (rec->data + offset);
							doOperation(lhs, (int *) set_clause.rhs.raw->data, set_clause.op);
						} else if (col->type == TYPE_FLOAT) {
							auto lhs = (double *) (rec->data + offset);
							if (set_clause.rhs.type == TYPE_INT) {
								doOperation(lhs, (int *) set_clause.rhs.raw->data, set_clause.op);
							} else if (set_clause.rhs.type == TYPE_FLOAT) {
								doOperation(lhs, (double *) set_clause.rhs.raw->data, set_clause.op);
							} else {
								throw std::logic_error("Todo for int64 type");
							}
						} else {
							// somewhere unkonwn
							throw std::logic_error("somewhere unkonwn");
						}
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
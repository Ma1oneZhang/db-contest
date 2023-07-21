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
#include "record/rm_defs.h"
#include "system/sm.h"
#include "transaction/txn_defs.h"
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>

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
		std::vector<Rid> rids;
		std::vector<std::unique_ptr<RmRecord>> original_records_;
		std::vector<std::unique_ptr<RmRecord>> updated_records_;
		for (auto scan = std::make_unique<RmScan>(fh_); !scan->is_end(); scan->next()) {
			auto rec = fh_->get_record(scan->rid(), nullptr);
			// check every record
			if (checkCondition(rec)) {
				auto original_rec = std::make_unique<RmRecord>(*rec);
				original_records_.emplace_back(std::move(original_rec));
				// make set_clause apply on it
				for (auto set_clause: set_clauses_) {
					auto col = tab_.get_col(set_clause.lhs.col_name);
					auto &offset = col->offset;
					if (!set_clause.is_self) {
						if (col->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_INT) {
							double *data = (double *) (rec->data + offset);
							// using built_in type transfer
							*data = set_clause.rhs.int_val;
						} else if (col->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_BIGINT) {
							double *data = (double *) (rec->data + offset);
							// using built_in type transfer
							*data = set_clause.rhs.bigint_val;
						} else if (col->type == TYPE_BIGINT && set_clause.rhs.type == TYPE_INT) {
							int64_t *data = (int64_t *) (rec->data + offset);
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
							} else if (set_clause.rhs.type == TYPE_BIGINT) {
								doOperation(lhs, (int64_t *) set_clause.rhs.raw->data, set_clause.op);
							} else {
								throw std::logic_error("Todo for int64 type");
							}
						} else if (col->type == TYPE_BIGINT) {
							auto lhs = (int64_t *) (rec->data + offset);
							if (set_clause.rhs.type == TYPE_INT) {
								doOperation(lhs, (int *) set_clause.rhs.raw->data, set_clause.op);
							} else if (set_clause.rhs.type == TYPE_BIGINT) {
								doOperation(lhs, (int64_t *) set_clause.rhs.raw->data, set_clause.op);
							}
						} else {
							// somewhere unkonwn
							throw std::logic_error("somewhere unkonwn");
						}
					}
				}
				updated_records_.emplace_back(std::move(rec));
				rids.push_back(scan->rid());
			}
		}
		if (tab_.indexes.size() > 0) {
			// means need unique check
			std::unordered_set<std::string> vailated_records;
			for (auto &updated_rec: updated_records_) {
				for (size_t i = 0; i < tab_.indexes.size(); ++i) {
					auto &index = tab_.indexes[i];
					std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
					int offset = 0;
					for (size_t i = 0; i < index.col_num; ++i) {
						memcpy(key->data + offset, updated_rec->data + index.cols[i].offset, index.cols[i].len);
						offset += index.cols[i].len;
					}
					auto bin_data = std::string(key->data, key->size);
					if (vailated_records.count(bin_data))
						throw DuplicateKeyError("update key duplicate");
					vailated_records.insert(bin_data);
				}
			}
			vailated_records.clear();
			// delete entry of index
			for (auto &original_rec: original_records_) {
				for (size_t i = 0; i < tab_.indexes.size(); ++i) {
					auto &index = tab_.indexes[i];
					auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
					std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
					int offset = 0;
					for (size_t i = 0; i < index.col_num; ++i) {
						memcpy(key->data + offset, original_rec->data + index.cols[i].offset, index.cols[i].len);
						offset += index.cols[i].len;
					}
					ih->delete_entry(key->data, context_->txn_);
				}
			}
			// check if in the index.
			for (auto &rec: updated_records_) {
				for (size_t i = 0; i < tab_.indexes.size(); ++i) {
					auto &index = tab_.indexes[i];
					auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
					std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
					int offset = 0;
					for (size_t i = 0; i < index.col_num; ++i) {
						memcpy(key->data + offset, rec->data + index.cols[i].offset, index.cols[i].len);
						offset += index.cols[i].len;
					}
					std::vector<Rid> res;
					ih->get_value(key->data, &res, context_->txn_);
					if (res.size() != 0) {
						// insert original record back to index
						for (size_t j = 0; j < original_records_.size(); j++) {
							const auto &original_rec = original_records_[j];
							const auto &rid = rids[j];
							for (size_t i = 0; i < tab_.indexes.size(); ++i) {
								auto &index = tab_.indexes[i];
								auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
								std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
								int offset = 0;
								for (size_t i = 0; i < index.col_num; ++i) {
									memcpy(key->data + offset, original_rec->data + index.cols[i].offset, index.cols[i].len);
									offset += index.cols[i].len;
								}
								ih->insert_entry(key->data, rid, context_->txn_);
							}
						}
						throw DuplicateKeyError("update key duplicate");
					}
				}
			}
			// insert updated record to the index
			for (size_t i = 0; i < updated_records_.size(); ++i) {
				const auto &rec = updated_records_[i];
				const auto &rid = rids[i];
				for (size_t i = 0; i < tab_.indexes.size(); ++i) {
					auto &index = tab_.indexes[i];
					auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
					std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
					int offset = 0;
					for (size_t i = 0; i < index.col_num; ++i) {
						memcpy(key->data + offset, rec->data + index.cols[i].offset, index.cols[i].len);
						offset += index.cols[i].len;
					}
					ih->insert_entry(key->data, rid, context_->txn_);
				}
			}
		}
		for (size_t i = 0; i < updated_records_.size(); i++) {
			fh_->insert_record(rids[i], updated_records_[i]->data);
		}
		if (context_->txn_->get_txn_mode()) {
			// add it to the deleted_records
			for (size_t i = 0; i < updated_records_.size(); i++) {
				WriteRecord *wrec = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rids_[i], *original_records_[i]);
				context_->txn_->append_write_record(wrec);
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
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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "index/ix_index_handle.h"
#include "recovery/log_manager.h"
#include "system/sm.h"
#include "system/sm_meta.h"
#include "transaction/txn_defs.h"
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
		std::vector<std::unique_ptr<RmRecord>> deleted_records;
		context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, sm_manager_->fhs_[tab_name_]->GetFd());
		for (auto rid: rids_) {
			auto rec = fh_->get_record(rid, nullptr);
			// delete it from every index
			for (size_t i = 0; i < tab_.indexes.size(); ++i) {
				const auto &index = tab_.indexes[i];
				const auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
				const std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
				int offset = 0;
				for (size_t i = 0; i < index.col_num; ++i) {
					memcpy(key->data + offset, rec->data + index.cols[i].offset, index.cols[i].len);
					offset += index.cols[i].len;
				}
				ih->delete_entry(key->data, context_->txn_);
			}
			// delete from disk
			fh_->delete_record(rid, context_);
			// if the transtions is open
			if (context_->txn_->get_state() == TransactionState::DEFAULT) {
				// add it to the deleted_records
				WriteRecord *deleteRecord = new WriteRecord{WType::DELETE_TUPLE, tab_name_, rid, *rec};
				context_->txn_->append_write_record(deleteRecord);
				auto &txn = context_->txn_; 
				if (context_->log_mgr_->get_enable_logging()) {
					auto log = new DeleteLogRecord(txn->get_transaction_id(), txn->get_prev_lsn(), *rec, rid, tab_name_);
					txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(log)); 
					// log->format_print(); 
					if (context_->txn_->get_txn_mode() == true) { //多条事务才刷, 单条事务会跟上commit, 不需要额外刷一次盘
						context_->log_mgr_->flush_log_to_disk(txn->get_transaction_id());
					}
				}
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
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
#include "common/context.h"
#include "defs.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "index/ix_index_handle.h"
#include "record/rm_defs.h"
#include "recovery/log_manager.h"
#include "system/sm.h"
#include "system/sm_meta.h"
#include <memory>

class InsertExecutor : public AbstractExecutor {
private:
	TabMeta tab_;              // 表的元数据
	std::vector<Value> values_;// 需要插入的数据
	RmFileHandle *fh_;         // 表的数据文件句柄
	std::string tab_name_;     // 表名称
	Rid rid_;                  // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
	SmManager *sm_manager_;

public:
	InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
		sm_manager_ = sm_manager;
		tab_ = sm_manager_->db_.get_table(tab_name);
		values_ = values;
		tab_name_ = tab_name;
		if (values.size() != tab_.cols.size()) {
			throw InvalidValueCountError();
		}
		fh_ = sm_manager_->fhs_.at(tab_name).get();
		context_ = context;
	};

	std::unique_ptr<RmRecord> Next() override {
		// Make record buffer
		RmRecord rec(fh_->get_file_hdr().record_size);
		for (size_t i = 0; i < values_.size(); i++) {
			auto &col = tab_.cols[i];
			auto &val = values_[i];
			if (col.type == val.type) {
				val.init_raw(col.len);
				memcpy(rec.data + col.offset, val.raw->data, col.len);
			} else if (col.type == TYPE_BIGINT && val.type == TYPE_INT) {
				*(int64_t *) (rec.data + col.offset) = val.int_val;
			} else if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
				*(double *) (rec.data + col.offset) = val.int_val;
			} else {//type not equal && val is not TYPE_DATETIME
				throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
			}
		}
		// check if it not unique
		for (size_t i = 0; i < tab_.indexes.size(); ++i) {
			auto &index = tab_.indexes[i];
			auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
			std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
			int offset = 0;
			for (size_t i = 0; i < index.col_num; ++i) {
				memcpy(key->data + offset, rec.data + index.cols[i].offset, index.cols[i].len);
				offset += index.cols[i].len;
			}
			std::vector<Rid> res;
			auto exist = ih->get_value(key->data, &res, context_->txn_);
			if (exist) {
				throw DuplicateKeyError("insert key duplicate");
			}
		}
		// Insert into record file
		rid_ = fh_->insert_record(rec.data, context_); //插入数据的位置

		// Insert into index
		for (size_t i = 0; i < tab_.indexes.size(); ++i) {
			auto &index = tab_.indexes[i];
			auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
			std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
			int offset = 0;
			for (size_t i = 0; i < index.col_num; ++i) {
				memcpy(key->data + offset, rec.data + index.cols[i].offset, index.cols[i].len);
				offset += index.cols[i].len;
			}
			ih->insert_entry(key->data, rid_, context_->txn_);
		}
		if (context_->txn_->get_state() == TransactionState::DEFAULT) {
			// add it to the deleted_records
			WriteRecord *deleteRecord = new WriteRecord{WType::INSERT_TUPLE, tab_name_, rid_};
			context_->txn_->append_write_record(deleteRecord);

			auto &txn = context_->txn_; 
			if (context_->log_mgr_->get_enable_logging()) {
				auto log = new InsertLogRecord(txn->get_transaction_id(), txn->get_prev_lsn(), rec, rid_, tab_name_);
				txn->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(log)); 
				log->format_print(); 
				if (context_->txn_->get_txn_mode() == true) {
					context_->log_mgr_->flush_log_to_disk(txn->get_transaction_id());
				}
			}
		}
		return nullptr;
	}
	Rid &rid() override { return rid_; }

	const std::vector<ColMeta> &cols() override {
		std::vector<ColMeta> *cols = nullptr;
		return *cols;
	}
};
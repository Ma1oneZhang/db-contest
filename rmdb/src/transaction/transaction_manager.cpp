/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "common/config.h"
#include "common/context.h"
#include "errors.h"
#include "execution/executor_delete.h"
#include "record/rm_file_handle.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction.h"
#include "transaction/txn_defs.h"
#include "defs.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager) {
	// Todo:
	// 1. 判断传入事务参数是否为空指针
	// 2. 如果为空指针，创建新事务
	// 3. 把开始事务加入到全局事务表中
	// 4. 返回当前事务指针
	if (txn != nullptr)
		return txn;
	auto txn_id = next_txn_id_++;
	auto new_txn = new Transaction(txn_id);
	new_txn->set_start_ts(next_timestamp_++);
	new_txn->set_state(TransactionState::DEFAULT);
	txn_map[txn_id] = new_txn;

	if (log_manager->get_enable_logging()) {
		// assert(txn->get_prev_lsn() == INVALID_LSN); 
		auto log = new BeginLogRecord(new_txn->get_transaction_id(), new_txn->get_prev_lsn()); 
		new_txn->set_prev_lsn(log_manager->add_log_to_buffer(log));
	}
	return new_txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager) {
	// Todo:
	// 1. 如果存在未提交的写操作，提交所有的写操作
	// 2. 释放所有锁
	// 3. 释放事务相关资源，eg.锁集
	// 4. 把事务日志刷入磁盘中
	// 5. 更新事务状态
	if (txn == nullptr)
		return;
	txn->set_state(TransactionState::COMMITTED);
	// commit all write operations
	auto write_set = txn->get_write_set();
	// release all locks
	auto lock_set = txn->get_lock_set();
	for (auto lock: *lock_set) {
		lock_manager_->unlock(txn, lock);
	}
	// clean related resource
	write_set->clear();
	// todo: flush log file to disk
	// update the txn status

	if (log_manager->get_enable_logging()) {
		auto log = CommitLogRecord(txn->get_transaction_id(), txn->get_prev_lsn());
		txn->set_prev_lsn(log_manager->add_log_to_buffer(&log));
		log_manager->flush_log_to_disk(txn->get_transaction_id()); //刷新磁盘
	}
	lock_set->clear();
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager) {
	// Todo:
	// 1. 回滚所有写操作
	// 2. 释放所有锁
	// 3. 清空事务相关资源，eg.锁集
	// 4. 把事务日志刷入磁盘中
	// 5. 更新事务状态
	if (txn == nullptr)
		return;
	txn->set_state(TransactionState::ABORTED);
	// txn->set_txn_mode(false);
	auto write_set = txn->get_write_set();
	while (!write_set->empty()) {
		switch (write_set->back()->GetWriteType()) {
			case WType::INSERT_TUPLE:
				rollback_insert(write_set->back()->GetTableName(), write_set->back()->GetRid(), txn);
				break;
			case WType::DELETE_TUPLE:
				rollback_delete(write_set->back()->GetTableName(), write_set->back()->GetRid(), write_set->back()->GetRecord(), txn);
				break;
			case WType::UPDATE_TUPLE:
				rollback_update(write_set->back()->GetTableName(), write_set->back()->GetRid(), write_set->back()->GetRecord(), txn);
				break;
		}
		write_set->pop_back();
	}
	auto lock_set = txn->get_lock_set();
	for (auto lock: *lock_set) {
		lock_manager_->unlock(txn, lock);
	}
	// clean related resource
	write_set->clear();
	lock_set->clear();
	// todo: flush log file to disk
	if (log_manager->get_enable_logging()) {
		auto log = AbortLogRecord(txn->get_transaction_id(), txn->get_prev_lsn()); 
		txn->set_prev_lsn(log_manager->add_log_to_buffer(&log)); 
		log_manager->flush_log_to_disk(txn->get_transaction_id()); 
	}
}
// insert rollback is delete
void TransactionManager::rollback_insert(const std::string &tab_name_, const Rid &rid, Transaction *txn) {
	auto tab_ = sm_manager_->db_.get_table(tab_name_);
	auto rec = sm_manager_->fhs_.at(tab_name_).get()->get_record(rid, nullptr);
	auto fh_ = sm_manager_->fhs_.at(tab_name_).get();
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
		ih->delete_entry(key->data, txn);
	}
	// delete from disk
	fh_->delete_record(rid, nullptr);
}
// delete rollback is insert
void TransactionManager::rollback_delete(const std::string &tab_name_, const Rid &rid, const RmRecord &rec, Transaction *txn) {
	auto tab_ = sm_manager_->db_.get_table(tab_name_);
	auto fh_ = sm_manager_->fhs_.at(tab_name_).get();
	// insert it back every index
	for (size_t i = 0; i < tab_.indexes.size(); ++i) {
		const auto &index = tab_.indexes[i];
		const auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
		const std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
		int offset = 0;
		for (size_t i = 0; i < index.col_num; ++i) {
			memcpy(key->data + offset, rec.data + index.cols[i].offset, index.cols[i].len);
			offset += index.cols[i].len;
		}
		ih->insert_entry(key->data, rid, nullptr);
	}
	// write it to disk
	fh_->insert_record(rid, rec.data);
}

// update -> update back (delete then insert)
void TransactionManager::rollback_update(const std::string &tab_name_, const Rid &rid, const RmRecord &record, Transaction *txn) {
	auto tab_ = sm_manager_->db_.get_table(tab_name_);
	auto rec = sm_manager_->fhs_.at(tab_name_).get()->get_record(rid, nullptr);
	auto fh_ = sm_manager_->fhs_.at(tab_name_).get();
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
		ih->delete_entry(key->data, txn);
	}
	// write back to disk
	fh_->insert_record(rid, record.data);
	// insert back to every index
	for (size_t i = 0; i < tab_.indexes.size(); ++i) {
		const auto &index = tab_.indexes[i];
		const auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
		const std::unique_ptr<RmRecord> key = std::make_unique<RmRecord>(index.col_tot_len);
		int offset = 0;
		for (size_t i = 0; i < index.col_num; ++i) {
			memcpy(key->data + offset, record.data + index.cols[i].offset, index.cols[i].len);
			offset += index.cols[i].len;
		}
		ih->insert_entry(key->data, rid, nullptr);
	}
}

/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
#include "common/config.h"
#include <algorithm>
#include <shared_mutex>

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd) {

	return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd) {

	return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd) {
	std::scoped_lock mu{latch_};
	auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
	auto &rwlock = lock_table_[lock_data_id];
	// if not the exclusive lock
	// The rwlock will not be the lock
	if (rwlock.num >= 0) {
		// refresh it and update the first_hold_shared
		auto pos = std::find_if(rwlock.request_queue_.begin(), rwlock.request_queue_.end(), [&](LockRequest &a) {
			return a.txn_id_ == txn->get_transaction_id();
		});
		// never use shared_lock at before
		if (pos == rwlock.request_queue_.end()) {
			// add it to the request queue for check
			rwlock.request_queue_.push_back({txn->get_transaction_id(), LockMode::SHARED});
			// increase the read count
			rwlock.num++;
			// change the lockmode to the Shared
			rwlock.group_lock_mode_ = GroupLockMode::S;
			// change the first_hold_shared transaction id to the min one(happened first)
			rwlock.first_hold_shared =
				rwlock.first_hold_shared == -1 ? txn->get_transaction_id() : std::min(rwlock.first_hold_shared, txn->get_transaction_id());
			// add it to the lock set
			txn->get_lock_set()->insert(lock_data_id);
		}
		return true;
	} else if (rwlock.first_hold_shared == txn->get_transaction_id()) {
		// do not modify anything
		return true;
	} else {
		// throw error for no wait
		throw TransactionAbortException(txn->get_transaction_id(), AbortReason::GET_S);
		return false;
	}
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd) {
	// hold the global lock
	auto lock = std::scoped_lock<std::mutex>{latch_};
	auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
	// acquire the lock in table
	auto &rwlock = lock_table_[lock_data_id];
	// if corrent transaction is not the first hold the lock
	// we throw error for no-wait
	if (rwlock.first_hold_shared != txn->get_transaction_id() && rwlock.first_hold_shared != -1) {
		throw TransactionAbortException(txn->get_transaction_id(), AbortReason::GET_X);
	}
	rwlock.request_queue_.clear();
	rwlock.num = -1;
	rwlock.first_hold_shared = txn->get_transaction_id();
	rwlock.group_lock_mode_ = GroupLockMode::X;
	txn->get_lock_set()->insert(lock_data_id);
	return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd) {

	return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd) {

	return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id) {
	std::scoped_lock latch{latch_};
	auto &rwlock = lock_table_[lock_data_id];
	// rwlock.num ++ ;
	if (rwlock.num < 0) {
		// exclusive lock
		// reset
		rwlock.num = 0;
		rwlock.first_hold_shared = -1;
		rwlock.group_lock_mode_ = GroupLockMode::NON_LOCK;
		return true;
	} else if (rwlock.num > 0) {
		// shared lock
		rwlock.num--;
		if (rwlock.num == 0) {
			rwlock.request_queue_.clear();
			rwlock.first_hold_shared = -1;
		} else {
			txn_id_t next_min_id = 0x3f3f3f3f;
			for (auto it = rwlock.request_queue_.begin(); it != rwlock.request_queue_.end(); it++) {
				if (it->txn_id_ != rwlock.first_hold_shared) {
					next_min_id = std::min(next_min_id, it->txn_id_);
				} else {
					it = rwlock.request_queue_.erase(it);
					it--;
				}
			}
			rwlock.first_hold_shared = next_min_id;
		}
		return true;
	}
	return false;
}
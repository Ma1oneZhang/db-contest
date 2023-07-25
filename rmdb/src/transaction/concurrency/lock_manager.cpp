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
#include <shared_mutex>

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {

    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    auto lock = std::unique_lock<std::mutex>(latch_);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);

    auto &rwlock = lock_table_[lock_data_id]; 
    if (rwlock.num >= 0) {
        rwlock.num ++ ; 
        rwlock.group_lock_mode_ = GroupLockMode::S; 
        return true; 
    }
    else {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::GET_S); 
    }

    return true; 
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    auto lock = std::unique_lock<std::mutex>(latch_);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);

    auto &rwlock = lock_table_[lock_data_id]; 
    if (rwlock.num == 0) {
        rwlock.num -- ; 
        rwlock.group_lock_mode_ = GroupLockMode::X; 
        return true; 
    }
    else {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::GET_X); 
    }
    
    return true; 
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    auto &rwlock = lock_table_[lock_data_id]; 
    rwlock.num ++ ; 
    if ((rwlock.num == -1 && rwlock.group_lock_mode_ == GroupLockMode::X)
        || (rwlock.num >= 0 && rwlock.group_lock_mode_ == GroupLockMode::S)) {
            rwlock.num ++ ; 
            return true; 
        }
    return false; 
}
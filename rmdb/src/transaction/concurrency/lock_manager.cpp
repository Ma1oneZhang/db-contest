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
#include "transaction/transaction.h"
#include "transaction/txn_defs.h"
#include <memory>
#include <mutex>

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    // 2. 检查事务的状态
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    // 1. 获取全局锁变量
    auto lock = std::unique_lock<std::mutex>(latch_);

    // 2. 检查事务的状态
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED); 
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING); 
    }

    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    //查找是否申请国目标哦数据项的锁, 因为是读锁, 如果申请过直接返回
    auto lock_data_id = LockDataId(tab_fd, rid, LockDataType::RECORD); //行级锁
    if (lock_table_.count(lock_data_id)) {
        return true; 
    }

    txn->get_lock_set()->insert(lock_data_id); 
    txn->set_state(TransactionState::GROWING); //设置为扩张封锁阶段

    // 4. 将要申请的锁放入到全局锁表中
    auto request = LockRequest(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_requrest_queue = lock_table_[lock_data_id]; 
    lock_requrest_queue.request_queue_.push_back(std::move(request));

    // 并通过组模式来判断是否可以成功授予锁行级锁不会出现IS，IX，因此行级读锁只在X时阻塞
    while (lock_requrest_queue.group_lock_mode_ != GroupLockMode::NON_LOCK &&
        lock_requrest_queue.group_lock_mode_ != GroupLockMode::S) {
            lock_requrest_queue.cv_.wait(lock); //不能授予锁, 阻塞当前操作
    }

    //可以授予锁
    request.granted_ = true; 
    lock_requrest_queue.group_lock_mode_ = GroupLockMode::S;
    lock_requrest_queue.cv_.notify_all(); //去除所有阻塞信号
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

    // 2. 检查事务的状态
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED); 
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING); 
    }

    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    if (lock_table_.count(lock_data_id)) {
        return true; 
    }

    txn->get_lock_set()->insert(lock_data_id); 
    txn->set_state(TransactionState::GROWING); //设置为扩张封锁阶段
    
    // 4. 将要申请的锁放入到全局锁表中
    auto request = LockRequest(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_requrest_queue = lock_table_[lock_data_id]; 
    lock_requrest_queue.request_queue_.push_back(std::move(request));

    // 表级读锁在IX，SIX，X时阻塞
    auto& mode = lock_requrest_queue.group_lock_mode_; 
    while (mode != GroupLockMode::NON_LOCK
         && mode != GroupLockMode::S
         && mode != GroupLockMode::IS) {
            lock_requrest_queue.cv_.wait(lock); //不能授予锁, 阻塞当前操作
    }

    //可以授予锁
    request.granted_ = true; 

    if (mode == GroupLockMode::NON_LOCK    //NONE, IS, S -> S
        || mode == GroupLockMode::IS) {
        mode = GroupLockMode::S;
    } else if (mode == GroupLockMode::IX) { // IX -> SIX
        mode = GroupLockMode::SIX; 
    }

    lock_requrest_queue.cv_.notify_all(); //去除所有阻塞信号
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

    // 2. 检查事务的状态
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED); 
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING); 
    }

    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    if (lock_table_.count(lock_data_id)) {
        return true; 
    }

    //设置txn的锁和模式
    txn->get_lock_set()->insert(lock_data_id); 
    txn->set_state(TransactionState::GROWING); //设置为扩张封锁阶段
    
    // 4. 将要申请的锁放入到全局锁表中
    auto request = LockRequest(txn->get_transaction_id(), LockMode::SHARED); 
    auto &lock_requrest_queue = lock_table_[lock_data_id]; 
    lock_requrest_queue.request_queue_.push_back(std::move(request));

    // 表级读锁在IX，SIX，X时阻塞
    auto& mode = lock_requrest_queue.group_lock_mode_; 
    while (mode != GroupLockMode::NON_LOCK) {
            lock_requrest_queue.cv_.wait(lock); //不能授予锁, 阻塞当前操作
    }

    //可以授予锁
    request.granted_ = true; 
    mode = GroupLockMode::X; 
    lock_requrest_queue.cv_.notify_all(); //去除所有阻塞信号
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
    std::unique_lock<std::mutex> lock(latch_);

    txn->set_state(TransactionState::SHRINKING); //将事务状态设置为收缩期
    auto txn_lock_set = txn->get_lock_set();
    if (txn_lock_set->find(lock_data_id) == txn_lock_set->end()) {
        return false; //没有找到锁
    }

    //将要释放的锁清楚掉
    std::list<LockRequest> requests; 
    auto &lock_requrest_queue = lock_table_[lock_data_id]; 
    for (auto &request : lock_requrest_queue.request_queue_) {
        if (request.txn_id_ != txn->get_transaction_id()) {
            requests.push_back(std::move(request)); 
        }
    }
    lock_requrest_queue.request_queue_ = requests; 

    auto after_mode = GroupLockMode::NON_LOCK; //之后的模式
    for (auto &request : lock_requrest_queue.request_queue_) {
        if (!request.granted_) continue; 

        if (request.lock_mode_ == LockMode::EXLUCSIVE) {
            after_mode= GroupLockMode::X;
            break;
        } else if (request.lock_mode_ == LockMode::S_IX) {
            after_mode= GroupLockMode::SIX;
        } else if (request.lock_mode_ == LockMode::SHARED && after_mode != GroupLockMode::SIX) {
            if (after_mode == GroupLockMode::IX) {
                after_mode = GroupLockMode::SIX;
            } else {
                after_mode = GroupLockMode::S;
            }
        } else if (request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE && after_mode != GroupLockMode::SIX) {
            if (after_mode == GroupLockMode::S) {
                after_mode = GroupLockMode::SIX;
            } else {
                after_mode = GroupLockMode::IX;
            }
        } else if (request.lock_mode_ == LockMode::INTENTION_SHARED 
                    && (after_mode == GroupLockMode::NON_LOCK || after_mode == GroupLockMode::IS)) {
            after_mode= GroupLockMode::IS;
        }
    }
    lock_requrest_queue.group_lock_mode_ = after_mode; 
    lock_requrest_queue.cv_.notify_all(); 
    return true; 
}
/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include <mutex>
#include "common/config.h"
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    std::unique_lock<std::mutex> latch(latch_); 
    if (log_buffer_.is_full(log_record->log_tot_len_)) {
        flush_log_to_disk(log_record->log_tid_);
    }

    //更新lsn
    log_record->lsn_ = global_lsn_ ++ ; 

    log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_); //写入buffer
    log_buffer_.offset_ += log_record->log_tot_len_; //更新偏移量

    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk(txn_id_t txn) {
    std::unique_lock<std::mutex> latch(latch_); 
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    // set_activa_txn_(txn, log_buffer_.offset_); //更新事件得长度
    // log_buffer_.offset_ = 0; 
    // set_persist_lsn_(global_lsn_); //持久化到lsn
    
    if (txn2log_file.count(txn) && log_buffer_.offset_ > 0) {
        disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_, txn2log_file[txn]);
        set_activa_txn_(txn, log_buffer_.offset_); //更新事件得长度
        // set_persist_lsn_(global_lsn_); //持久化到lsn
    }
    log_buffer_.offset_ = 0; 
}
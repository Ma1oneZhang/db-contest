/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"
#include "common/config.h"
#include "errors.h"
#include "recovery/log_manager.h"
#include <cassert>
#include <iostream>
#include <istream>
#include <memory>
#include <mutex>
#include <fstream>
#include <regex.h>
#include <string>

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    //获取多条事务的最后长度
    if (!disk_manager_->is_file(UNDO_PAIR_FILE_NAME)) return ; 
    txn_id_t k; int v; 
    int len = sizeof(txn_id_t) + sizeof(int); 
    auto buff = new char[8]; 
    int offset = 0; 
    while (disk_manager_->read_undo_pair(buff, len, offset) != -1) {
        k = *(txn_id_t*)buff; 
        v = *(int*)(buff + sizeof(txn_id_t)); 
        offset += len; 

        active_txn[k] = v; 
    }
    
    // for (auto [k, v] : active_txn) {
    //     std::cout << "this is active_txn: " << k << ' ' << v << '\n';
    // }
    disk_manager_->destroy_file(UNDO_PAIR_FILE_NAME); 
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    // LogRecord log_rec;
    // int offset = 0;
    // int tot_len = 20;  
    // int begin_len = 20;  
    // int ret = disk_manager_->read_log(buffer_.buffer_, begin_len, offset);
    // if (ret == 0) return ; 
    // log_rec.deserialize(buffer_.buffer_); //begin;

    // tot_len = log_rec.log_tot_len_; 
    // // offset += log_rec.log_tot_len_; 
    // while (disk_manager_->read_log(buffer_.buffer_, tot_len, offset) != -1) {
    //     int cur_len = 20; //当前record的长度; 
    //     switch (log_rec.log_type_) {
    //         case LogType::INSERT: {
    //             auto x = InsertLogRecord(); 
    //             x.deserialize(buffer_.buffer_); 
    //             txn_manager_->rollback_delete(x.table_name_, x.rid_, x.insert_value_, nullptr);
    //             cur_len = x.log_tot_len_; 
    //         }break; 
    //         case LogType::DELETE: {
    //             auto x = DeleteLogRecord(); 
    //             x.deserialize(buffer_.buffer_);
    //             txn_manager_->rollback_insert(x.table_name_, x.rid_, nullptr); 
    //             cur_len = x.log_tot_len_; 
    //         }break;
    //         case LogType::UPDATE: {
    //             auto x = UpdateLogRecord(); 
    //             x.deserialize(buffer_.buffer_); 
    //             txn_manager_->rollback_update(x.table_name_, x.update_rid_, x.old_value_, nullptr);
    //             cur_len = x.log_tot_len_; 
    //         }break; 
    //         case LogType::begin: {
    //             // cur_len += begin_len; 
    //         }break;
    //         default: { //abort, commit
    //             // throw UndoError("多出的 ABORT COMMIT类型"); 
    //         }break; 
    //     }
    //     offset += cur_len; //更新到下一个偏移量
    //     disk_manager_->read_log(buffer_.buffer_, tot_len, offset); //找到下一个rec的头文件地址
    //     log_rec.deserialize(buffer_.buffer_); //获取完头文件数据
    //     tot_len = log_rec.log_tot_len_;  //获取整体长度
    // }
}


/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    for (auto [txn, log_len]: active_txn) {
        auto file_name = "undo_recovery_" + std::to_string(txn) + ".log"; 
        buffer_.offset_ = 0; 
        bool is_first = true; 
        while (disk_manager_->read_log(buffer_.buffer_, log_len, buffer_.offset_, file_name) != -1)
        {
            LogRecord log_rec; 
            log_rec.deserialize(buffer_.buffer_); 
            if (is_first) {
                is_first = false; 
                if (log_rec.log_type_ == LogType::commit //事务已经完成推送
                    || log_rec.log_type_ == LogType::ABORT //事务已经完成终端
                    || log_rec.log_type_ == LogType::begin) { //当前事务没有东西*/
                    break; 
                }
            }

            //当前需要undo
            switch (log_rec.log_type_) {
                case LogType::INSERT: {
                    auto x = InsertLogRecord(); 
                    x.deserialize(buffer_.buffer_); 
                    txn_manager_->rollback_insert(x.table_name_, x.rid_, nullptr); 
                }break; 
                case LogType::DELETE: {
                    auto x = DeleteLogRecord(); 
                    x.deserialize(buffer_.buffer_);
                    txn_manager_->rollback_delete(x.table_name_, x.rid_, x.delete_value_, nullptr) ;
                }break;
                case LogType::UPDATE: {
                    auto x = UpdateLogRecord(); 
                    x.deserialize(buffer_.buffer_); 
                    txn_manager_->rollback_update(x.table_name_, x.update_rid_, x.old_value_, nullptr);
                }break; 
                case LogType::begin: {

                }break;
                default: { //abort, commit
                    // throw UndoError("多出的 ABORT COMMIT类型"); 
                }break; 
                
                //更新offset
                buffer_.offset_ += log_len; 
                log_len = log_rec.log_tot_len_; 
            }

            //收尾工作, 删除所有的日志文件
            // auto &path2fd = sm_manager_.
            disk_manager_->destroy_file(file_name); 
        }  
    }


    std::cout << "undo compllite\n"; 
}
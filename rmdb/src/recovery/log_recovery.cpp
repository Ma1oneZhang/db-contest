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
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <istream>
#include <memory>
#include <mutex>
#include <fstream>
#include <regex.h>
#include <string>

void RecoveryManager::get_logs(std::string file_name) {
    logs.clear(); 

    LogRecord log_rec;
    int offset = 0;
    int head_len = 20; 
    while (disk_manager_->read_log(buffer_.buffer_, head_len, offset) > 0) {
        log_rec.deserialize(buffer_.buffer_); //获取头文件的数据
        disk_manager_->read_log(buffer_.buffer_, log_rec.log_tot_len_, offset);
        lsn2log_id[log_rec.lsn_] = logs.size(); //对应logs的位置

        switch (log_rec.log_type_) {
            case LogType::INSERT: {
                auto x = InsertLogRecord(); 
                x.deserialize(buffer_.buffer_); 
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_), std::move(x.log_type_), std::move(x.table_name_), std::move(x.rid_), std::move(x.insert_value_)}); 
            }break; 
            case LogType::DELETE: {
                auto x = DeleteLogRecord(); 
                x.deserialize(buffer_.buffer_); 
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_),  std::move(x.log_type_), std::move(x.table_name_), std::move(x.rid_), std::move(x.delete_value_)}); 
            }break;
            case LogType::UPDATE: {
                auto x = UpdateLogRecord(); 
                x.deserialize(buffer_.buffer_); 
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_),  std::move(x.log_type_), std::move(x.table_name_), std::move(x.update_rid_), std::move(x.old_value_), std::move(x.new_value_)}); 

            }break; 
            case LogType::begin: {
                auto x = BeginLogRecord(); 
                x.deserialize(buffer_.buffer_); 
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_),  std::move(x.log_type_)}); 

            }break;
            case LogType::commit: {
                auto x = CommitLogRecord(); 
                x.deserialize(buffer_.buffer_);      
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_),  std::move(x.log_type_)}); 

            }break;
            case LogType::ABORT: {
                auto x = AbortLogRecord(); 
                x.deserialize(buffer_.buffer_); 
                logs.push_back({std::move(x.log_tid_), std::move(x.lsn_), std::move(x.prev_lsn_),  std::move(x.log_type_)}); 
            }break;
            default: { //abort, commit
                // throw UndoError("多出的 ABORT COMMIT类型"); 
            }break; 
        }

        offset += log_rec.log_tot_len_; //更新到下一个偏移量
    }
}


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
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    get_logs(LOG_FILE_NAME);
    for (auto log : logs) {
        last_lsn[log.txn_] = log.lsn_;  //更新最后的lsn
        switch (log.type) {
            case LogType::ABORT:
                undo_txn(log.lsn_); 
                break;
            case LogType::INSERT:
                txn_manager_->insert(log.tab_name_, log.rid_, log.value_, nullptr);
                break;
            case LogType::DELETE:
                txn_manager_->rollback_insert(log.tab_name_, log.rid_, nullptr);
                break;
            case LogType::UPDATE:
                txn_manager_->rollback_update(log.tab_name_, log.rid_, log.new_value_, nullptr);
            default:
                break;
        }
    }
}



void RecoveryManager::undo_txn(lsn_t lsn) {
    auto get_log_by_lsn = [&](lsn_t lsn) {
        return logs[lsn2log_id[lsn]]; 
    };

    auto cur_log = get_log_by_lsn(lsn); 
    
    while (true) {
        if (cur_log.prev_lsn_ == INVALID_LSN) break; 
        switch (cur_log.type) {
            case LogType::INSERT:
                txn_manager_->rollback_insert(cur_log.tab_name_, cur_log.rid_, nullptr);
                break;
            case LogType::DELETE:
                txn_manager_->rollback_delete(cur_log.tab_name_, cur_log.rid_, cur_log.value_, nullptr);
                break;
            case LogType::UPDATE:
                txn_manager_->rollback_update(cur_log.tab_name_, cur_log.rid_, cur_log.value_, nullptr);
            default:
                break;
        }
        cur_log = get_log_by_lsn(cur_log.prev_lsn_); 
    }
    
}


/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    auto get_log_by_lsn = [&](lsn_t lsn) {
        return logs[lsn2log_id[lsn]]; 
    };
    std::vector<std::pair<txn_id_t, lsn_t>> ve(last_lsn.begin(), last_lsn.end()); 
    std::reverse(ve.begin(), ve.end()); 
    for (auto [_, lsn] : ve) {
        auto log = get_log_by_lsn(lsn);
        if (log.type == LogType::begin || log.type == LogType::commit || log.type == LogType::ABORT) {
            continue; 
        }
        else {
            undo_txn(lsn); 
        }
    }
}
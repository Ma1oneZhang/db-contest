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

#include <map>
#include <memory>
#include <unordered_map>
#include "defs.h"
#include "log_manager.h"
#include "common/config.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
#include "transaction/transaction_manager.h"
#include "log_manager.h"

class RedoLogsInPage {
public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_;   // 在该page上需要redo的操作的lsn
};

struct LogNode {
    txn_id_t txn_;
    lsn_t lsn_; 
    lsn_t prev_lsn_;  
    LogType type; 
    char* tab_name_; 
    Rid rid_; 
    RmRecord value_;
    RmRecord new_value_;
};

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager, TransactionManager* txn_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        txn_manager_ = txn_manager; 
    }

    void analyze();
    void redo();
    void undo_txn(lsn_t txn);

    void undo();
    void get_logs(std::string file_name);

private:
    LogBuffer buffer_;                                              // 读入日志
    DiskManager* disk_manager_;                                     // 用来读写文件
    BufferPoolManager* buffer_pool_manager_;                        // 对页面进行读写
    SmManager* sm_manager_;                                         // 访问数据库元数据
    TransactionManager* txn_manager_; 
    std::vector<LogNode> logs; 
    std::map<txn_id_t, int> active_txn; 
    
    std::map<lsn_t, int> lsn2log_id; //lsn对应的LogNode数据位置
    std::map<txn_id_t, lsn_t> last_lsn; //每个事务最后一个对应的lsn
};
/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <atomic>
#include <cstring>
#include <exception>
#include <fstream>
#include <memory>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <signal.h>
#include <string>
#include <string_view>
#include <unistd.h>

#include "analyze/analyze.h"
#include "common/config.h"
#include "errors.h"
#include "optimizer/optimizer.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "recovery/log_recovery.h"
#include "storage/disk_manager.h"
#include "utils/timer.h"
#include "gtest/gtest.h"

#define SOCK_PORT 8765
#define MAX_CONN_LIMIT 8
static jmp_buf jmpbuf;
static bool should_exit;


class Test : public::testing::Test {
public:

	// 构建全局所需的管理器对象
	std::string db_name_ = "test"; //创建的数据库名字
	std::unique_ptr<DiskManager> disk_manager;
	std::unique_ptr<BufferPoolManager> buffer_pool_manager;
	std::unique_ptr<RmManager> rm_manager;
	std::unique_ptr<IxManager> ix_manager;
	std::unique_ptr<SmManager> sm_manager;
	std::unique_ptr<LockManager> lock_manager;
	std::unique_ptr<TransactionManager> txn_manager;
	std::unique_ptr<QlManager> ql_manager;
	std::unique_ptr<LogManager> log_manager;
	std::unique_ptr<RecoveryManager> recovery;
	std::unique_ptr<Planner> planner;
	std::unique_ptr<Optimizer> optimizer;
	std::unique_ptr<Portal> portal;
	std::unique_ptr<Analyze> analyze;
	pthread_mutex_t *buffer_mutex;
	pthread_mutex_t *sockfd_mutex;
	txn_id_t txn_id = INVALID_TXN_ID;
	char *result = new char[BUFFER_LENGTH];
	int offset = 0;

public: 
	void SetUp() override {
		::testing::Test::SetUp(); 
		disk_manager = std::make_unique<DiskManager>();
		buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
		rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
		ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
		sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
		lock_manager = std::make_unique<LockManager>();
		txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
		ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get());
		log_manager = std::make_unique<LogManager>(disk_manager.get());
		recovery = std::make_unique<RecoveryManager>(disk_manager.get(), buffer_pool_manager.get(), sm_manager.get());
		planner = std::make_unique<Planner>(sm_manager.get());
		optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
		portal = std::make_unique<Portal>(sm_manager.get());
		analyze = std::make_unique<Analyze>(sm_manager.get());

		// init mutex
		buffer_mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
		sockfd_mutex = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
		pthread_mutex_init(buffer_mutex, nullptr);
		pthread_mutex_init(sockfd_mutex, nullptr);

		// create db and open db
        if (sm_manager->is_dir(db_name_)) {
            sm_manager->drop_db(db_name_);
        }
        sm_manager->create_db(db_name_);
        sm_manager->open_db(db_name_);
	}

	void TearDown() override {
		sm_manager->close_db(); 
        // sm_manager_->drop_db(db_name_);  // 若不删除数据库文件，则将保留最后一个测试点的数据库
	}



public:
	void sigint_handler(int signo); 
	void SetTransaction(txn_id_t *txn_id, Context *context);
	void exec_sql(const std::string &sql);
	int cnt; 
};


void Test::sigint_handler(int signo) {
	should_exit = true;
	log_manager->flush_log_to_disk();
	std::cout << "The Server receive Crtl+C, will been closed\n";
	longjmp(jmpbuf, 1);
}

// 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
void Test::SetTransaction(txn_id_t *txn_id, Context *context) {
	context->txn_ = txn_manager->get_transaction(*txn_id);
	if (context->txn_ == nullptr ||
			context->txn_->get_state() == TransactionState::COMMITTED ||
			context->txn_->get_state() == TransactionState::ABORTED) {
		context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);
		*txn_id = context->txn_->get_transaction_id();
		context->txn_->set_txn_mode(false);
	}
}


void Test::exec_sql(const std::string &sql) {
	auto data_recv = sql.c_str();
	if ((++ cnt) % 1000 == 0) 
		std::cout << cnt << '\n'; 

	//初始化输出信息
	memset(result, '\0', BUFFER_LENGTH);
	offset = 0;

	// 开启事务，初始化系统所需的上下文信息（包括事务对象指针、锁管理器指针、日志管理器指针、存放结果的buffer、记录结果长度的变量）
	Context *context = new Context(lock_manager.get(), log_manager.get(),
																nullptr, result, &offset);
	SetTransaction(&txn_id, context);

	// 用于判断是否已经调用了yy_delete_buffer来删除buf
	bool finish_analyze = false;
	pthread_mutex_lock(buffer_mutex);
	YY_BUFFER_STATE buf = yy_scan_string(data_recv);
	if (yyparse() == 0) {
		if (ast::parse_tree != nullptr) {
			try {
				// analyze and rewrite
				std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
				yy_delete_buffer(buf);
				finish_analyze = true;
				pthread_mutex_unlock(buffer_mutex);
				// 优化器
				std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
				// portal
				std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
				portal->run(portalStmt, ql_manager.get(), &txn_id, context);
				portal->drop();
			} catch (TransactionAbortException &e) {
				// 事务需要回滚，需要把abort信息返回给客户端并写入output.txt文件中
				std::string str = "abort\n";
				memcpy(result, str.c_str(), str.length());
				result[str.length()] = '\0';
				offset = str.length();

				// 回滚事务
				txn_manager->abort(context->txn_, log_manager.get());
				std::cout << e.GetInfo() << std::endl;

				disk_manager->write_outfile(str);
			} catch (RMDBError &e) {
				// 遇到异常，需要打印failure到output.txt文件中，并发异常信息返回给客户端
				std::cerr << e.what() << std::endl;

				memcpy(result, e.what(), e.get_msg_len());
				result[e.get_msg_len()] = '\n';
				result[e.get_msg_len() + 1] = '\0';
				offset = e.get_msg_len() + 1;

				// 将报错信息写入output.txt
				disk_manager->write_outfile_failure();
			}
		}
	} else {
		disk_manager->write_outfile_failure(); 
	}
	if (finish_analyze == false) {
		yy_delete_buffer(buf);
		pthread_mutex_unlock(buffer_mutex);
		// send client parse error
		std::string_view parse_error = "SQL_parse error recheck the sql";
		memcpy(result, parse_error.data(), parse_error.size());
		result[parse_error.size()] = '\n';
		result[parse_error.size() + 1] = '\0';
		offset = parse_error.size() + 1;
	}
	// 如果是单条语句，需要按照一个完整的事务来执行，所以执行完当前语句后，自动提交事务
	if (context->txn_->get_txn_mode() == false) {
		txn_manager->commit(context->txn_, context->log_mgr_);
	}
}

// test one
TEST_F(Test, ONE) {
	// init sql test file	
	if (access(SQL_TEST_FILE_PATH.c_str(), F_OK) == -1) {
		std::cerr << "测试的SQL文件不存在\n";
		exit(1);
	}
 
	std::ifstream file(SQL_TEST_FILE_PATH);
	std::string sql; 
	while (std::getline(file, sql)) {
		exec_sql(sql); 
	}
}

// // 要增加测试, 直接在下面加 TEST_F
// TEST_F(Test, BeginTest) {
//     Transaction *txn = nullptr;
//     txn = txn_manager->begin(txn, log_manager.get());

//     EXPECT_EQ(txn_manager->txn_map.size(), 1);
//     EXPECT_NE(txn, nullptr);
//     EXPECT_EQ(txn->get_state(), TransactionState::DEFAULT);
// }

// // test commit
// TEST_F(Test, CommitTest) {
//     exec_sql("create table t1 (num int);");
//     exec_sql("begin;");
//     exec_sql("insert into t1 values(1);");
//     exec_sql("insert into t1 values(2);");
//     exec_sql("insert into t1 values(3);");
//     exec_sql("update t1 set num = 4 where num = 1;");
//     exec_sql("delete from t1 where num = 3;");
//     exec_sql("commit;");
//     exec_sql("select * from t1;");
//     const char *str = "+------------------+\n"
//         "|              num |\n"
//         "+------------------+\n"
//         "|                4 |\n"
//         "|                2 |\n"
//         "+------------------+\n"
//         "Total record(s): 2\n";
//     EXPECT_STREQ(result, str);
//     // there should be 3 transactions
//     EXPECT_EQ(txn_manager->get_next_txn_id(), 3);
//     Transaction *txn = txn_manager->get_transaction(1);
//     EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
// }

// // test abort
// TEST_F(Test, AbortTest) {
//     exec_sql("create table t1 (num int);");
//     exec_sql("begin;");
//     exec_sql("insert into t1 values(1);");
//     exec_sql("insert into t1 values(2);");
//     exec_sql("insert into t1 values(3);");
//     exec_sql("update t1 set num = 4 where num = 1;");
//     exec_sql("delete from t1 where num = 3;");
//     exec_sql("abort;");
//     exec_sql("select * from t1;");
//     const char * str = "+------------------+\n"
//         "|              num |\n"
//         "+------------------+\n"
//         "+------------------+\n"
//         "Total record(s): 0\n";
//     EXPECT_STREQ(result, str);
//     EXPECT_EQ(txn_manager->get_next_txn_id(), 3);
//     Transaction *txn = txn_manager->get_transaction(1);
//     EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
// }


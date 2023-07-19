/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "defs.h"
#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "optimizer/plan.h"
#include "record_printer.h"
#include <cstddef>

const char *help_info = "Supported SQL syntax:\n"
												"  command ;\n"
												"command:\n"
												"  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
												"  DROP TABLE table_name\n"
												"  CREATE INDEX table_name (column_name)\n"
												"  DROP INDEX table_name (column_name)\n"
												"  INSERT INTO table_name VALUES (value [, value ...])\n"
												"  DELETE FROM table_name [WHERE where_clause]\n"
												"  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
												"  SELECT selector FROM table_name [WHERE where_clause]\n"
												"type:\n"
												"  {INT | FLOAT | CHAR(n)}\n"
												"where_clause:\n"
												"  condition [AND condition ...]\n"
												"condition:\n"
												"  column op {column | value}\n"
												"column:\n"
												"  [table_name.]column_name\n"
												"op:\n"
												"  {= | <> | < | > | <= | >=}\n"
												"selector:\n"
												"  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context) {
	if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
		switch (x->tag) {
			case T_CreateTable: {
				sm_manager_->create_table(x->tab_name_, x->cols_, context);
				break;
			}
			case T_DropTable: {
				sm_manager_->drop_table(x->tab_name_, context);
				break;
			}
			case T_CreateIndex: {
				sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
				break;
			}
			case T_DropIndex: {
				sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
				break;
			}
			default:
				throw InternalError("Unexpected field type");
				break;
		}
	}
}

// 执行help; show tables; desc table; show index; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
	if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
		switch (x->tag) {
			case T_Help: {
				memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
				*(context->offset_) = strlen(help_info);
				break;
			}
			case T_ShowTable: {
				sm_manager_->show_tables(context);
				break;
			}
			case T_DescTable: {
				sm_manager_->desc_table(x->tab_name_, context);
				break;
			}
			case T_ShowIndex: {
				sm_manager_->show_index(x->tab_name_, context); 
				break;
			}
			case T_Transaction_begin: {
				// 显示开启一个事务
				context->txn_->set_txn_mode(true);
				break;
			}
			case T_Transaction_commit: {
				context->txn_ = txn_mgr_->get_transaction(*txn_id);
				txn_mgr_->commit(context->txn_, context->log_mgr_);
				break;
			}
			case T_Transaction_rollback: {
				context->txn_ = txn_mgr_->get_transaction(*txn_id);
				txn_mgr_->abort(context->txn_, context->log_mgr_);
				break;
			}
			case T_Transaction_abort: {
				context->txn_ = txn_mgr_->get_transaction(*txn_id);
				txn_mgr_->abort(context->txn_, context->log_mgr_);
				break;
			}
			default:
				throw InternalError("Unexpected field type");
				break;
		}
	}
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, 
														std::vector<TabCol> sel_cols,
														Context *context) {

	// print header into file
	std::fstream outfile;
	outfile.open("output.txt", std::ios::out | std::ios::app);
	
	//需要求的聚合函数
	auto &aggregates = sm_manager_->db_.get_table(sel_cols.front().tab_name).get_aggregates(); 

	if (aggregates.size()) { //当前查询为聚合函数查询

		/* 执行query_plan, 获取每行的聚合值*/
		std::vector<AggregateCmp> aggregate_vals(aggregates.size());
		for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
			auto Tuple = executorTreeRoot->Next();
			std::vector<std::string> columns;
			for (size_t i = 0; i < executorTreeRoot->cols().size(); i ++ ) 
			{
				auto &col = executorTreeRoot->cols()[i];
				auto &col_name = col.name; 
				char *rec_buf = Tuple->data + col.offset;
				if (col.type == TYPE_INT) {
					auto val = *(int *) rec_buf; 
					for (size_t j = 0; j < aggregates.size(); j ++ ) {
						if (aggregates[j]->cols.front()->col_name == col_name) {
							aggregate_vals[j].update(val, aggregates[j]->aggregate_dir); 
						}
					}
				} else if (col.type == TYPE_FLOAT) {
					auto val = *(double *) rec_buf;
					for (size_t j = 0; j < aggregates.size(); j ++ ) {
						if (aggregates[j]->cols.front()->col_name == col_name) {
							aggregate_vals[j].update(val, aggregates[j]->aggregate_dir); 
						}
					}
				} else if (col.type == TYPE_STRING) {
					auto val = std::string((char *) rec_buf, col.len);
					for (size_t j = 0; j < aggregates.size(); j ++ ) {
						if (aggregates[j]->cols.front()->col_name == col_name) {
							val.resize(strlen(val.c_str())); //去除空字符串
							aggregate_vals[j].update(val, aggregates[j]->aggregate_dir); 
						}
					}
				}
			}
		}
		/** 打印行的信息, MAX, MIN的结果 */
		std::vector<std::string> column_vals; 
		for (size_t i = 0; i < aggregates.size(); i ++ ) {
			column_vals.push_back(aggregate_vals[i].get_val(aggregates[i]->aggregate_dir)); 

		}

		/** 获取要打印的col_name*/
		std::vector<std::string> captions; 
		for (auto aggregate : aggregates) {
			if ((bool) aggregate->col_alias) {
				captions.push_back(aggregate->col_alias->col_name);
			} else if (aggregate->aggregate_dir == Aggregate_COUNT) {
				captions.push_back("*"); //特判为COUNT的情况下
			} else {
				captions.push_back(aggregate->cols.front()->col_name);
			}
		}

		/** 将结果输出到outputfile*/
		outfile << "|";
		for (auto col_name : captions) {
			outfile << ' ' << col_name << " |"; 
		}
		outfile << "\n" 
				<< "|";

		for (auto val : column_vals) {
			outfile << ' ' <<  val << " |"; 
		}
		outfile << "\n";
		outfile.close();


		/**  terminal output*/
		// Print header into buffer
		RecordPrinter rec_printer(aggregates.size());
		rec_printer.print_separator(context);
		rec_printer.print_record(captions, context);
		rec_printer.print_separator(context);
		// Print column value into buffer
		rec_printer.print_record(column_vals, context); 
		// Print footer into buffer
		rec_printer.print_separator(context);
		// Print record count into buffer
		RecordPrinter::print_record_count(1, context);


		//initial
		aggregates.clear(); 
		
	} else { //普通的查询
		std::vector<std::string> captions;
		captions.reserve(sel_cols.size());
		for (auto &sel_col: sel_cols) {
			captions.push_back(sel_col.col_name);
		}
		// Print header into buffer
		RecordPrinter rec_printer(sel_cols.size());
		rec_printer.print_separator(context);
		rec_printer.print_record(captions, context);
		rec_printer.print_separator(context);

		outfile << "|";
		for (size_t i = 0; i < captions.size(); ++i) {
			outfile << " " << captions[i] << " |";
		}
		outfile << "\n";

		// Print records
		size_t num_rec = 0;
		// 执行query_plan
		for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
			auto Tuple = executorTreeRoot->Next();
			std::vector<std::string> columns;
			for (auto &col: executorTreeRoot->cols()) {
				std::string col_str;
				char *rec_buf = Tuple->data + col.offset;
				if (col.type == TYPE_INT) {
					col_str = std::to_string(*(int *) rec_buf);
				} else if (col.type == TYPE_FLOAT) {
					col_str = std::to_string(*(double *) rec_buf);
				} else if (col.type == TYPE_STRING) {
					col_str = std::string((char *) rec_buf, col.len);
					col_str.resize(strlen(col_str.c_str()));
				} else if (col.type == TYPE_DATETIME) {
					col_str = std::string((char *) rec_buf, col.len);
					// col_str.resize(strlen(col_str.c_str()));
				} else if (col.type == TYPE_BIGINT) {
					col_str = std::to_string(*(int64_t *) rec_buf);
				}
				columns.push_back(col_str);
			}
			// print record into buffer
			rec_printer.print_record(columns, context);
			// print record into file
			outfile << "|";
			for (size_t i = 0; i < columns.size(); ++i) {
				outfile << " " << columns[i] << " |";
			}
			outfile << "\n";
			num_rec++;
		}
		outfile.close();
		// Print footer into buffer
		rec_printer.print_separator(context);
		// Print record count into buffer
		RecordPrinter::print_record_count(num_rec, context);
	}
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
	exec->Next();
}
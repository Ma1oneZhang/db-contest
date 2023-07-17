/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"
#include "common/common.h"
#include "defs.h"
#include "errors.h"
#include "parser/ast.h"
#include "utils/log.h"
#include <memory>
#include <unordered_map>
#include <set>
#include <vector>
/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
	std::shared_ptr<Query> query = std::make_shared<Query>();
	if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
		// 处理表名
		query->tables = std::move(x->tabs);
		/** TODO: 检查表是否存在 */
		for (auto &table: query->tables) {
			if (!sm_manager_->db_.is_table(table)) {
				throw TableNotFoundError(table);
			}
		}
		// get all column name
		std::vector<ColMeta> all_cols;
		get_all_cols(query->tables, all_cols);

		if (x->has_aggregate) { //当前是聚合查询
			auto &aggregates = x->aggregates; 
			std::set<TabCol> sel_cols; //所有需要选择的列

			//将所有的aggregate都放入到ColMeta中的第一个袁术
			auto &bas_aggre = sm_manager_->db_.get_table(query->tables.front()).get_aggregates(); 
			bas_aggre.clear();  //将其清空, 之前的元素

			// 将所有aggregate情况加入到ColMeta中
			for (size_t i = 0; i < all_cols.size(); i ++ ) {
				auto &col = all_cols[i]; 
				for (auto aggregate : aggregates) {
					TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};

					if (!aggregate->cols.size() && i == 0) {
						TabCol sel_col = {.tab_name = all_cols.front().tab_name, .col_name = all_cols.front().name};
						aggregate->cols.push_back(std::make_shared<ast::Col>(sel_col.tab_name, sel_col.col_name)); 
						sel_cols.insert(sel_col); 
						bas_aggre.push_back(aggregate); 
					} else if (aggregate->cols.size() 
							// && aggregate->cols.front()->tab_name == sel_col.tab_name 
							&& aggregate->cols.front()->col_name == sel_col.col_name) {

						bas_aggre.push_back(std::move(aggregate)); 
						sel_cols.insert(sel_col); 
					}
				}
			}
			// 特判COUNT(*) 情况
			for (auto &aggregate : aggregates) {
				if (aggregate->cols.size() == 0) {

				}
			}
			query->cols = std::vector<TabCol>(sel_cols.begin(), sel_cols.end()); 
		} else {
			// 处理target list，再target list中添加上表名，例如 a.id
			for (auto &sv_sel_col: x->cols) {
				TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
				query->cols.push_back(sel_col);
			}
			if (query->cols.empty()) {
				// select all columns
				// select * statment
				for (auto &col: all_cols) {
					TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
					query->cols.push_back(sel_col);
				}
			} else {
				// infer table name from column name
				for (auto &sel_col: query->cols) {
					sel_col = check_column(all_cols, sel_col);// 列元数据校验
				}
			}
		}





		std::unordered_map<std::string, std::vector<std::string>> cnt;
		for (auto i: all_cols) {
			cnt[i.name].push_back(i.tab_name);
		}
		for (auto i: x->conds) {
			if (i->lhs->tab_name.empty() && !i->lhs->col_name.empty()) {
				if (cnt[i->lhs->col_name].size() == 2) {
					throw AmbiguousColumnError(i->lhs->col_name);
				}
				// 		i->lhs->tab_name = cnt[i->lhs->col_name].front();
			}
		}
		//处理where条件
		get_clause(x->conds, query->conds, query->tables);
		check_clause(query->tables, query->conds);
	} else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
		/** TODO: */
		// 处理表名
		query->tables.emplace_back(x->tab_name);
		/* 检查表是否存在 */
		for (auto &table: query->tables) {
			if (!sm_manager_->db_.is_table(table)) {
				throw TableNotFoundError(table);
			}
		}
		// get update clause
		for (auto clause: x->set_clauses) {
			TabCol tab_col = {x->tab_name, clause->col_name};
			query->cols.emplace_back(tab_col);
			// types // TODO in long int and data type
			if (auto x = std::dynamic_pointer_cast<ast::IntLit>(clause->val)) {
				Value val;
				val.type = TYPE_INT;
				val.set_int(x->val);
				val.raw = std::make_shared<RmRecord>(sizeof(int), (char *) (&x->val));
				if (!clause->is_self) {
					query->set_clauses.emplace_back(tab_col, val);
				} else {
					query->set_clauses.emplace_back(tab_col, val, clause->op);
				}
			} else if (auto x = std::dynamic_pointer_cast<ast::FloatLit>(clause->val)) {
				Value val;
				val.type = TYPE_FLOAT;
				val.set_float(x->val);
				val.raw = std::make_shared<RmRecord>(sizeof(double), (char *) (&x->val));
				if (!clause->is_self) {
					query->set_clauses.emplace_back(tab_col, val);
				} else {
					query->set_clauses.emplace_back(tab_col, val, clause->op);
				}
			} else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(clause->val)) {
				if (clause->is_self) {
					throw RMDBError("Unsupported operation");
				}
				auto &lhs_tab = sm_manager_->db_.get_table(tab_col.tab_name);
				auto lhs_col = lhs_tab.get_col(tab_col.col_name);
				auto lhs_type = lhs_col->type;
				Value val;
				if (lhs_type == TYPE_DATETIME) {
					val.type = TYPE_DATETIME;
					val.set_datetime(str_lit->val);
				} else {
					val.type = TYPE_STRING;
					val.set_str(str_lit->val);
				}
				val.raw = std::make_shared<RmRecord>((int) str_lit->val.size(), const_cast<char *>(str_lit->val.c_str()));
				query->set_clauses.emplace_back(tab_col, val);
			} else if (auto bigint_lit = std::dynamic_pointer_cast<ast::BigintLit>(clause->val)) {
				Value val;
				val.type = TYPE_BIGINT;
				val.set_bigint(bigint_lit->val);
				val.raw = std::make_shared<RmRecord>(sizeof(int64_t), (char *) (&bigint_lit->val));
				if (!clause->is_self) {
					query->set_clauses.emplace_back(tab_col, val);
				} else {
					query->set_clauses.emplace_back(tab_col, val, clause->op);
				}
			}
		}

		std::vector<ColMeta> all_cols;
		get_all_cols(query->tables, all_cols);
		if (query->cols.empty()) {
			// update all columns
			// the update statment have no where statment
			for (auto &col: all_cols) {
				TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
				query->cols.push_back(sel_col);
			}
		} else {
			// infer table name from column name
			for (auto &sel_col: query->cols) {
				sel_col = check_column(all_cols, sel_col);// 列元数据校验
			}
		}
		// handle the where condition
		get_clause(x->conds, query->conds, query->tables);
		check_clause(query->tables, query->conds);
	} else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
		//处理where条件
		get_clause(x->conds, query->conds, {x->tab_name});
		check_clause({x->tab_name}, query->conds);
	} else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
		// 处理insert 的values值
		for (size_t i = 0; i < x->vals.size(); i++) {
			auto &sv_val = x->vals[i];
			auto &col_type = sm_manager_->db_.get_table(x->tab_name).cols[i].type;
			query->values.push_back(convert_sv_value(sv_val, col_type));
		}
	} else {

		// do nothing
	}
	query->parse = std::move(parse);
	return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
	if (target.tab_name.empty()) {
		// Table name not specified, infer table name from column name
		std::string tab_name;
		for (auto &col: all_cols) {
			if (col.name == target.col_name) {
				if (!tab_name.empty()) {
					throw AmbiguousColumnError(target.col_name);
				}
				tab_name = col.tab_name;
			}
		}
		if (tab_name.empty()) {
			throw ColumnNotFoundError(target.col_name);
		}
		target.tab_name = tab_name;
	} else {
		/* Make sure target column exists */
		bool exist = false;
		for (const auto &col: all_cols) {
			if (col.tab_name == target.tab_name && col.name == target.col_name) {
				exist = true;
			}
		}
		if (!exist) {
			throw ColumnNotFoundError(target.col_name);
		}
	}
	return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
	for (auto &sel_tab_name: tab_names) {
		// 这里db_不能写成get_db(), 注意要传指针
		const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
		all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
	}
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds, const std::vector<std::string> &tab_names) {
	conds.clear();
	for (auto &expr: sv_conds) {
		Condition cond;
		cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};

		cond.op = convert_sv_comp_op(expr->op);
		if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
			cond.is_rhs_val = true;

			//get the col type;
			auto &lhs_tab = sm_manager_->db_.get_table(tab_names[0]);
			auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
			auto lhs_type = lhs_col->type;

			cond.rhs_val = convert_sv_value(rhs_val, lhs_type);
		} else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
			cond.is_rhs_val = false;


			cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
		}
		conds.push_back(cond);
	}
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
	// auto all_cols = get_all_cols(tab_names);
	std::vector<ColMeta> all_cols;
	get_all_cols(tab_names, all_cols);
	// Get raw values in where clause
	for (auto &cond: conds) {
		// Infer table name from column name
		cond.lhs_col = check_column(all_cols, cond.lhs_col);
		if (!cond.is_rhs_val) {
			cond.rhs_col = check_column(all_cols, cond.rhs_col);
		}
		TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
		auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
		ColType lhs_type = lhs_col->type;
		ColType rhs_type;
		if (cond.is_rhs_val) {
			if (cond.rhs_val.type == TYPE_FLOAT) {
				cond.rhs_val.init_raw(sizeof(double));
			} else if (cond.rhs_val.type == TYPE_INT) {
				cond.rhs_val.init_raw(sizeof(int));
			} else if (cond.rhs_val.type == TYPE_STRING) {
				cond.rhs_val.init_raw(lhs_col->len);
			} else if (cond.rhs_val.type == TYPE_DATETIME) {
				cond.rhs_val.init_raw(lhs_col->len);
			} else if (cond.rhs_val.type == TYPE_BIGINT) {
				cond.rhs_val.init_raw(sizeof(int64_t));
			}
			rhs_type = cond.rhs_val.type;
		} else {
			TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
			auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
			rhs_type = rhs_col->type;
		}
		if (lhs_type == TYPE_FLOAT || lhs_type == TYPE_INT) {
			if (rhs_type == TYPE_FLOAT || rhs_type == TYPE_INT) {
				continue;
			}
		}

		if (lhs_type == TYPE_BIGINT && rhs_type == TYPE_INT) {
			continue;
		}
		if (lhs_type == TYPE_INT && rhs_type == TYPE_BIGINT) {
			continue;
		}
		if (lhs_type == TYPE_FLOAT && rhs_type == TYPE_BIGINT) {
			continue;
		}
		if (lhs_type != rhs_type) {
			throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
		}
	}
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val, ColType type) {
	Value val;

	if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
		val.set_int(int_lit->val);
	} else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
		val.set_float(float_lit->val);
	} else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
		if (type == TYPE_DATETIME) {
			val.set_datetime(str_lit->val);
		} else {
			val.set_str(str_lit->val);
		}
	} else if (auto bigint_lit = std::dynamic_pointer_cast<ast::BigintLit>(sv_val)) {
		val.set_bigint(bigint_lit->val);
	} else {
		throw InternalError("Unexpected sv value type");
	}
	return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
	std::map<ast::SvCompOp, CompOp> m = {
		{ast::SV_OP_EQ, OP_EQ},
		{ast::SV_OP_NE, OP_NE},
		{ast::SV_OP_LT, OP_LT},
		{ast::SV_OP_GT, OP_GT},
		{ast::SV_OP_LE, OP_LE},
		{ast::SV_OP_GE, OP_GE},
	};
	return m.at(op);
}

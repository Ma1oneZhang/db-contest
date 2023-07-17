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

#include "ast.h"
#include <cassert>
#include <iostream>
#include <map>

namespace ast {

	class TreePrinter {
	public:
		static void print(const std::shared_ptr<TreeNode> &node) {
			print_node(node, 0);
		}

	private:
		static std::string offset2string(int offset) {
			return std::string(offset, ' ');
		}

		template<typename T>
		static void print_val(const T &val, int offset) {
			std::cout << offset2string(offset) << val << '\n';
		}

		template<typename T>
		static void print_val_list(const std::vector<T> &vals, int offset) {
			std::cout << offset2string(offset) << "LIST\n";
			offset += 2;
			for (auto &val: vals) {
				print_val(val, offset);
			}
		}

		static std::string type2str(SvType type) {
			static std::map<SvType, std::string> m{
				{SV_TYPE_INT, "INT"},
				{SV_TYPE_FLOAT, "FLOAT"},
				{SV_TYPE_STRING, "STRING"},
				{SV_TYPE_DATETIME, "DATETIME"},
				{SV_TYPE_BIGINT, "BIGINT"}};
			return m.at(type);
		}

		static std::string op2str(SvCompOp op) {
			static std::map<SvCompOp, std::string> m{
				{SV_OP_EQ, "=="},
				{SV_OP_NE, "!="},
				{SV_OP_LT, "<"},
				{SV_OP_GT, ">"},
				{SV_OP_LE, "<="},
				{SV_OP_GE, ">="},
			};
			return m.at(op);
		}

		static std::string aggregate2str(AggregateDir agg) {
			static std::map<AggregateDir, std::string> m{
				{Aggregate_COUNT, "COUNT"},
				{Aggregate_MAX, "MAX"},
				{Aggregate_MIN, "MIN"},
				{Aggregate_SUM, "SUM"},
			};
			return m.at(agg); 
		}

		static std::string orderByDir2str(OrderByDir agg) {
			static std::map<OrderByDir, std::string> m{
				{OrderBy_DEFAULT, "DEFAULT"},
				{OrderBy_ASC, "ASC"},
				{OrderBy_DESC, "DESC"},
			};
			return m.at(agg); 
		}

		template<typename T>
		static void print_node_list(std::vector<T> nodes, int offset) {
			std::cout << offset2string(offset);
			offset += 2;
			std::cout << "LIST\n";
			for (auto &node: nodes) {
				print_node(node, offset);
			}
		}

		static void print_node(const std::shared_ptr<TreeNode> &node, int offset) {
			std::cout << offset2string(offset);
			offset += 2;
			if (auto x = std::dynamic_pointer_cast<Help>(node)) {
				std::cout << "HELP\n";
			} else if (auto x = std::dynamic_pointer_cast<ShowTables>(node)) {
				std::cout << "SHOW_TABLES\n";
			} else if (auto x = std::dynamic_pointer_cast<CreateTable>(node)) {
				std::cout << "CREATE_TABLE\n";
				print_val(x->tab_name, offset);
				print_node_list(x->fields, offset);
			} else if (auto x = std::dynamic_pointer_cast<DropTable>(node)) {
				std::cout << "DROP_TABLE\n";
				print_val(x->tab_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<DescTable>(node)) {
				std::cout << "DESC_TABLE\n";
				print_val(x->tab_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<ShowIndex>(node)) {
				std::cout << "SHOW_INDEX\n"; 
				print_val(x->tab_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<CreateIndex>(node)) {
				std::cout << "CREATE_INDEX\n";
				print_val(x->tab_name, offset);
				// print_val(x->col_name, offset);
				for (auto col_name: x->col_names)
					print_val(col_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<DropIndex>(node)) {
				std::cout << "DROP_INDEX\n";
				print_val(x->tab_name, offset);
				// print_val(x->col_name, offset);
				for (auto col_name: x->col_names)
					print_val(col_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<ColDef>(node)) {
				std::cout << "COL_DEF\n";
				print_val(x->col_name, offset);
				print_node(x->type_len, offset);
			} else if (auto x = std::dynamic_pointer_cast<Col>(node)) {
				std::cout << "COL\n";
				print_val(x->tab_name + '.' + x->col_name, offset);
			} else if (auto x = std::dynamic_pointer_cast<TypeLen>(node)) {
				std::cout << "TYPE_LEN\n";
				print_val(type2str(x->type), offset);
				print_val(x->len, offset);
			} else if (auto x = std::dynamic_pointer_cast<IntLit>(node)) {
				std::cout << "INT_LIT\n";
				print_val(x->val, offset);
			} else if (auto x = std::dynamic_pointer_cast<FloatLit>(node)) {
				std::cout << "FLOAT_LIT\n";
				print_val(x->val, offset);
			} else if (auto x = std::dynamic_pointer_cast<StringLit>(node)) {
				std::cout << "STRING_LIT\n";
				print_val(x->val, offset);
			} else if (auto x = std::dynamic_pointer_cast<BigintLit>(node)) {
				std::cout << "BIGINT_LIT";
				print_val(x->val, offset);
			} else if (auto x = std::dynamic_pointer_cast<OrderBy>(node)) {
				std::cout << "ORDER BY\n";
				print_node_list(x->cols, offset);
				print_val(orderByDir2str(x->orderby_dir), offset); 
			} else if (auto x = std::dynamic_pointer_cast<SetClause>(node)) {
				if (x->is_self) {
					std::cout << "self operation SET_CLAUSE\n";
				} else {
					std::cout << "normal SET_CLAUSE\n";
				}
				print_val(x->col_name, offset);
				print_node(x->val, offset);
			} else if (auto x = std::dynamic_pointer_cast<Aggregate>(node)) {
				std::string str = aggregate2str(x->aggregate_dir) 
							+ '(' + (x->cols.size() == 0 ? "*" : x->cols.front()->col_name) + ')' ; 
				if (x->col_alias) str = str + " AS " + x->col_alias->col_name; 
				print_val(str, 0);
			} else if (auto x = std::dynamic_pointer_cast<BinaryExpr>(node)) {
				std::cout << "BINARY_EXPR\n";
				print_node(x->lhs, offset);
				print_val(op2str(x->op), offset);
				print_node(x->rhs, offset);
			} else if (auto x = std::dynamic_pointer_cast<InsertStmt>(node)) {
				std::cout << "INSERT\n";
				print_val(x->tab_name, offset);
				print_node_list(x->vals, offset);
			} else if (auto x = std::dynamic_pointer_cast<DeleteStmt>(node)) {
				std::cout << "DELETE\n";
				print_val(x->tab_name, offset);
				print_node_list(x->conds, offset);
			} else if (auto x = std::dynamic_pointer_cast<UpdateStmt>(node)) {
				std::cout << "UPDATE\n";
				print_val(x->tab_name, offset);
				print_node_list(x->set_clauses, offset);
				print_node_list(x->conds, offset);
			} else if (auto x = std::dynamic_pointer_cast<SelectStmt>(node)) {
				std::cout << "SELECT\n";
				if (x->has_aggregate) {
					print_node_list(x->aggregates, offset);
					print_val_list(x->tabs, offset);
					if (x->conds.size() > 0) print_node_list(x->conds, offset);
				} else { //not aggregate
					if (x->cols.size()) {
						print_node_list(x->cols, offset);
					} else {
						print_val("LIST", offset); 
						print_val("*", offset + 2); 
					}
					print_val_list(x->tabs, offset);
					if (x->conds.size() > 0) print_node_list(x->conds, offset);
					print_node_list(x->orders, offset);

				}
			} else if (auto x = std::dynamic_pointer_cast<TxnBegin>(node)) {
				std::cout << "BEGIN\n";
			} else if (auto x = std::dynamic_pointer_cast<TxnCommit>(node)) {
				std::cout << "COMMIT\n";
			} else if (auto x = std::dynamic_pointer_cast<TxnAbort>(node)) {
				std::cout << "ABORT\n";
			} else if (auto x = std::dynamic_pointer_cast<TxnRollback>(node)) {
				std::cout << "ROLLBACK\n";
			} else {
				assert(0);
			}
		}
	};

}// namespace ast

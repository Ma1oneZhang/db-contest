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
#include "defs.h"
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

enum JoinType {
	INNER_JOIN,
	LEFT_JOIN,
	RIGHT_JOIN,
	FULL_JOIN
};
namespace ast {

	enum SvType {
		SV_TYPE_INT,
		SV_TYPE_FLOAT,
		SV_TYPE_STRING,
		SV_TYPE_DATETIME,
		SV_TYPE_BIGINT
	};

	enum SvCompOp {
		SV_OP_EQ,
		SV_OP_NE,
		SV_OP_LT,
		SV_OP_GT,
		SV_OP_LE,
		SV_OP_GE
	};

	enum OrderByDir {
		OrderBy_DEFAULT,  //默认升序
		OrderBy_ASC,	  //升序
		OrderBy_DESC	  //降序
	};



	// Base class for tree nodes
	struct TreeNode {
		virtual ~TreeNode() = default;// enable polymorphism
	};

	struct Help : public TreeNode {
	};

	struct ShowTables : public TreeNode {
	};

	struct TxnBegin : public TreeNode {
	};

	struct TxnCommit : public TreeNode {
	};

	struct TxnAbort : public TreeNode {
	};

	struct TxnRollback : public TreeNode {
	};

	struct TypeLen : public TreeNode {
		SvType type;
		int len;

		TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
	};

	struct Field : public TreeNode {
	};

	struct ColDef : public Field {
		std::string col_name;
		std::shared_ptr<TypeLen> type_len;

		ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_) : col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
	};

	struct CreateTable : public TreeNode {
		std::string tab_name;
		std::vector<std::shared_ptr<Field>> fields;

		CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
	};

	struct DropTable : public TreeNode {
		std::string tab_name;

		DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
	};

	struct DescTable : public TreeNode {
		std::string tab_name;

		DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
	};

	struct ShowIndex : public TreeNode {
		std::string tab_name;

		ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
	};

	struct CreateIndex : public TreeNode {
		std::string tab_name;
		std::vector<std::string> col_names;

		CreateIndex(std::string tab_name_, std::vector<std::string> col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
	};

	struct DropIndex : public TreeNode {
		std::string tab_name;
		std::vector<std::string> col_names;

		DropIndex(std::string tab_name_, std::vector<std::string> col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
	};

	struct Expr : public TreeNode {
	};

	struct Value : public Expr {
	};

	struct IntLit : public Value {
		int val;

		IntLit(int val_) : val(val_) {}
	};

	struct FloatLit : public Value {
		double val;

		FloatLit(double val_) : val(val_) {}
	};

	struct StringLit : public Value {
		std::string val;

		StringLit(std::string val_) : val(std::move(val_)) {}
	};

	struct BigintLit : public Value {
		int64_t val;

		BigintLit(int64_t val_) : val(val_){};
	};

	struct DatetimeLit : public Value {
		Datetime val;

		DatetimeLit(Datetime val_) : val(std::move(val_)) {}
	};

	struct Col : public Expr {
		std::string tab_name;
		std::string col_name;

		Col(std::string tab_name_, std::string col_name_) : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
	};

	struct SetClause : public TreeNode {
		std::string col_name;
		std::shared_ptr<Value> val;
		bool is_self;
		int op;
		//  + - * /
		SetClause(std::string col_name_, std::shared_ptr<Value> val_) : col_name(std::move(col_name_)), val(std::move(val_)) { is_self = false; }
		SetClause(std::string col_name_, std::shared_ptr<Value> val_, int op_) : col_name(std::move(col_name_)), val(std::move(val_)), op(op_) { is_self = true; }
	};

	struct BinaryExpr : public TreeNode {
		std::shared_ptr<Col> lhs;
		SvCompOp op;
		std::shared_ptr<Expr> rhs;

		BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
	};

	struct OrderBy : public TreeNode {
		std::vector<std::shared_ptr<Col>> cols;
		OrderByDir orderby_dir;
		OrderBy(std::vector<std::shared_ptr<Col>> cols_, OrderByDir orderby_dir_) : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
	};

	struct Limit : public TreeNode {
		size_t limit_size; 
		Limit(size_t limit_size_) : limit_size(limit_size_) {}
	};

	struct Aggregate : public TreeNode {
		std::vector<std::shared_ptr<Col>> cols; 
		AggregateDir aggregate_dir; 	//聚合类型
		std::shared_ptr<Col> col_alias; //AS的别名

		Aggregate(std::vector<std::shared_ptr<Col>> cols_,
					AggregateDir aggregate_dir_,
					std::shared_ptr<Col> col_alias_) : cols(std::move(cols_)), aggregate_dir(std::move(aggregate_dir_)), col_alias(std::move(col_alias_)) {}
	};

	struct InsertStmt : public TreeNode {
		std::string tab_name;
		std::vector<std::shared_ptr<Value>> vals;

		InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
	};

	struct DeleteStmt : public TreeNode {
		std::string tab_name;
		std::vector<std::shared_ptr<BinaryExpr>> conds;

		DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
	};

	struct UpdateStmt : public TreeNode {
		std::string tab_name;
		std::vector<std::shared_ptr<SetClause>> set_clauses;
		std::vector<std::shared_ptr<BinaryExpr>> conds;

		UpdateStmt(std::string tab_name_,
							 std::vector<std::shared_ptr<SetClause>> set_clauses_,
							 std::vector<std::shared_ptr<BinaryExpr>> conds_) : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
	};

	struct JoinExpr : public TreeNode {
		std::string left;
		std::string right;
		std::vector<std::shared_ptr<BinaryExpr>> conds;
		JoinType type;

		JoinExpr(std::string left_, std::string right_,
						 std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) : left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
	};

	struct SelectStmt : public TreeNode {
		std::vector<std::shared_ptr<Col>> cols;
		std::vector<std::string> tabs;
		std::vector<std::shared_ptr<BinaryExpr>> conds;
		std::vector<std::shared_ptr<JoinExpr>> jointree;

		bool has_sort;
		std::vector<std::shared_ptr<OrderBy>> orders;

		bool has_aggregate; 
		std::vector<std::shared_ptr<Aggregate>> aggregates; //聚合函数

		bool has_limit; 
		std::shared_ptr<Limit> limit; 



		SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
							 std::vector<std::string> tabs_,
							 std::vector<std::shared_ptr<BinaryExpr>> conds_,
							 std::vector<std::shared_ptr<OrderBy>> orders_,
							 std::vector<std::shared_ptr<Aggregate>> aggregates_,
							 std::shared_ptr<Limit> limit_) : cols(std::move(cols_)), 
							 															tabs(std::move(tabs_)), 
																						conds(std::move(conds_)),
																						orders(std::move(orders_)), 
																						aggregates(std::move(aggregates_)),
																						limit(std::move(limit_)) {
			has_sort = (bool) orders.size();
			has_aggregate = (bool) aggregates.size();  //判断是否有聚合函数
			has_limit = (bool) limit; 
		}
	};

	// Semantic value
	struct SemValue {
		int sv_int;
		float sv_float;
		std::string sv_str;
		int64_t sv_bigint;
		OrderByDir sv_orderby_dir;
		AggregateDir sv_aggregate_dir;
		std::vector<std::string> sv_strs;

		std::shared_ptr<TreeNode> sv_node;

		SvCompOp sv_comp_op;

		std::shared_ptr<TypeLen> sv_type_len;

		std::shared_ptr<Field> sv_field;
		std::vector<std::shared_ptr<Field>> sv_fields;

		std::shared_ptr<Expr> sv_expr;

		std::shared_ptr<Value> sv_val;
		std::vector<std::shared_ptr<Value>> sv_vals;

		std::shared_ptr<Col> sv_col;
		std::vector<std::shared_ptr<Col>> sv_cols;

		std::shared_ptr<SetClause> sv_set_clause;
		std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

		std::shared_ptr<BinaryExpr> sv_cond;
		std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

		std::shared_ptr<OrderBy> sv_orderby;
		std::vector<std::shared_ptr<OrderBy>> sv_orderbys; 
		std::shared_ptr<Limit> sv_limit; 

		std::shared_ptr<Aggregate> sv_aggregate; 
		std::vector<std::shared_ptr<Aggregate>> sv_aggregates; 
	};

	extern std::shared_ptr<ast::TreeNode> parse_tree;

}// namespace ast

#define YYSTYPE ast::SemValue

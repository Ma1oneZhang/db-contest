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
#include "common/common.h"
#include "defs.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "parser/ast.h"
#include "system/sm.h"
#include <algorithm>
#include <memory>

class SortExecutor : public AbstractExecutor {
private:
	std::unique_ptr<AbstractExecutor> prev_;
	std::vector<ColMeta> cols_;   		// 所有Col
	std::vector<size_t> order_idx_;     //排序的列在cols中的序号
	std::vector<ast::OrderByDir> order_bys_; //排序字段的类型
	bool has_limit_; 
	std::shared_ptr<ast::Limit> limit_; 
	size_t tuple_num;
	std::vector<size_t> used_tuple;
	std::unique_ptr<RmRecord> current_tuple;
	size_t cur_tuple; //当前为表中的第几个数据 
	RmFileHandle *fh_;                // 表的数据文件句柄



	//排序的元素					((数据, 类型), Rid)
	struct node {
		char* val; 
		ColType type; 
		size_t len; 
	}; 
	std::vector<std::pair<std::vector<node>, Rid>> order_list; //所有的数据
	bool has_sorted; //是否已经完成排序

public:
	SortExecutor(std::unique_ptr<AbstractExecutor> prev, 
					RmFileHandle *fh,
					std::vector<ColMeta> cols, 
					std::vector<size_t> order_idx,
					std::vector<ast::OrderByDir> order_bys,
					std::shared_ptr<ast::Limit> limit) {
		prev_ = std::move(prev);
		cols_ = std::move(cols); 
		order_idx_ = std::move(order_idx); 
		order_bys_ = std::move(order_bys); 
		limit_ = std::move(limit); 
		has_limit_ = (bool) limit_; 
		fh_ = fh;
		tuple_num = 0;
		cur_tuple = 0; 
		has_sorted = false; 
		used_tuple.clear();
		order_list.clear(); 
	}
	const std::vector<ColMeta> &cols() override {
		return cols_;
	}
	void beginTuple() override {
		// prev_->beginTuple();

		for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
			auto Tuple = this->Next();
			// 只存储要排序的列

			std::vector<node> columns; 			 //存储需要排序的数据
			for (size_t i = 0; i < order_idx_.size(); i ++ ) {
				const auto idx = order_idx_[i]; 
				const auto &col = cols_[idx]; 

				auto src = Tuple->data + col.offset;  //基地址加偏移地址为当前数据的地址
				size_t len; 					//当前类型的长度
				if (col.type == TYPE_STRING) {  //如果是字符串, 需要给他重新设置长度
					len = strlen(src); 
				} else { 						//other type (TYPE_INT, TYPE_FLOAT)
					len = col.len; 
				}

				//分配空间
				auto rec_buf = new char[len]; 
				memcpy(rec_buf, src, len); 
			
				columns.push_back({std::move(rec_buf), col.type, std::move(len)});
			}

			//将当前行的数据存储在order_list中
			auto rid = prev_->rid();
			order_list.push_back({std::move(columns), std::move(rid)}); 
		}

		std::sort(order_list.begin(), order_list.end(), [&](const std::pair<std::vector<node>, Rid> a, 
																			const std::pair<std::vector<node>, Rid> b){
			for (size_t i = 0; i < order_idx_.size(); i ++ ) { //对所有排序的表就行搜索
				const auto &l = a.first[i]; 
				const auto &r = b.first[i]; 
				auto res = cmp(l.val, r.val, l.type, order_bys_[i], l.len); 

				if (res == -1) continue; //两个相等的情况
				else return (bool) res; 
			}
			return true; 
		}); 

		//释放空间
		for (auto &li : order_list) {
			for (auto &node : li.first) {
				delete []node.val; //释放空间 
			}
		}

		has_sorted = true;    //标记已经排序完成
		prev_->beginTuple();  //重新初始化一下
	}

	bool is_end() const override {
		if (!has_sorted || !has_limit_) { //如果没有排序成功 或者没有LIMIT, 直接返回prev_
			return prev_->is_end();
		} else {
			return cur_tuple >= limit_->limit_size; 
		}
	}

	void nextTuple() override {
		prev_->nextTuple();
	}


	std::unique_ptr<RmRecord> Next() override {
		if (!has_sorted) {
			return prev_->Next(); 
		} else {
			auto rid = order_list[cur_tuple ++ ].second; 
			return fh_->get_record(rid, nullptr); 
		}

	}

	Rid &rid() override { 
		return prev_->rid();
	}

	int cmp(char * lhs, char* rhs, ColType type, ast::OrderByDir order_by, int len) {
		int cmp;
		if (type == TYPE_INT) {
			cmp = ix_compare((int *) lhs, (int *) rhs, type, len);
		} else if (type == TYPE_FLOAT) {
			cmp = ix_compare((double *) lhs, (double *) rhs, type, len);
		} else if (type == TYPE_STRING) {
			cmp = ix_compare(lhs, rhs, type, len);
		} else {
			// somewhere unkonwn
			throw std::logic_error("somewhere unkonwn");
		}

		if (cmp == 0) return -1; //相等的情况

		switch (order_by) {
			case ast::OrderBy_DEFAULT:
			case ast::OrderBy_ASC: 
				if (cmp >= 0) 
					return false; 
				break; 
			case ast::OrderBy_DESC:
				if (cmp <= 0) 
					return false; 
				break; 
			default:
				break; 
		}
	
		return true; 
	}
};
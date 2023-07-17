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
#include "index/ix_defs.h"
#include "index/ix_manager.h"
#include "index/ix_scan.h"
#include "record/rm_defs.h"
#include "system/sm.h"
#include <cstdint>
#include <cstring>
#include <memory>

class IndexScanExecutor : public AbstractExecutor {
private:
	std::string tab_name_;            // 表名称
	TabMeta tab_;                     // 表的元数据
	std::vector<Condition> conds_;    // 扫描条件
	RmFileHandle *fh_;                // 表的数据文件句柄
	std::vector<ColMeta> cols_;       // 需要读取的字段
	size_t len_;                      // 选取出来的一条记录的长度
	std::vector<Condition> fed_conds_;// 扫描条件，和conds_字段相同

	std::vector<std::string> index_col_names_;// index scan涉及到的索引包含的字段
	IndexMeta index_meta_;                    // index scan涉及到的索引元数据

	Rid rid_;
	std::unique_ptr<RecScan> scan_;

	SmManager *sm_manager_;
	IxIndexHandle *ix_handle_;

	Iid begin_, end_;
	std::unique_ptr<RmRecord> lower_cmp_key_;
	bool is_lower;
	std::unique_ptr<RmRecord> upper_cmp_key_;
	bool is_upper;
	std::vector<int> offsets_;
	bool checkCondition(const std::unique_ptr<RmRecord> &rec) {
		for (auto &cond: conds_) {
			auto col = get_col(cols_, cond.lhs_col);
			auto lhs = rec->data + col->offset;
			char *rhs;
			ColType rhs_type;
			if (cond.is_rhs_val) {
				rhs_type = cond.rhs_val.type;
				rhs = cond.rhs_val.raw->data;
			} else {
				auto rhs_col = get_col(cols_, cond.rhs_col);
				rhs_type = rhs_col->type;
				rhs = rec->data + rhs_col->offset;
			}
			int cmp;
			if (col->type == TYPE_INT) {
				// handle int type
				if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((int *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					if (col->type != rhs_type) {
						throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
					}
					cmp = ix_compare((int *) lhs, (int *) rhs, rhs_type, col->len);
				}
			} else if (col->type == TYPE_FLOAT) {
				// handle float type
				if (rhs_type == TYPE_INT) {
					cmp = ix_compare((double *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_FLOAT) {
					cmp = ix_compare((double *) lhs, (double *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((double *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
			} else if (col->type == TYPE_STRING) {
				// handle string type
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare(lhs, rhs, rhs_type, col->len);
			} else if (col->type == TYPE_DATETIME) {
				if (col->type != rhs_type) {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
				cmp = ix_compare(lhs, rhs, rhs_type, col->len);
			} else if (col->type == TYPE_BIGINT) {
				if (rhs_type == TYPE_INT) {
					cmp = ix_compare((int64_t *) lhs, (int *) rhs, rhs_type, col->len);
				} else if (rhs_type == TYPE_BIGINT) {
					cmp = ix_compare((int64_t *) lhs, (int64_t *) rhs, rhs_type, col->len);
				} else {
					throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs_type));
				}
			} else {
				// somewhere unkonwn
				throw std::logic_error("somewhere unkonwn");
			}
			switch (cond.op) {
				case OP_EQ:
					if (cmp != 0)
						return false;
					break;
				case OP_NE:
					if (cmp == 0)
						return false;
					break;
				case OP_GE:
					if (cmp < 0)
						return false;
					break;
				case OP_LE:
					if (cmp > 0)
						return false;
					break;
				case OP_GT:
					if (cmp <= 0)
						return false;
					break;
				case OP_LT:
					if (cmp >= 0)
						return false;
					break;
				default:
					assert(false);
			}
		}
		return true;
	}

public:
	IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
										Context *context) {
		sm_manager_ = sm_manager;
		context_ = context;
		tab_name_ = std::move(tab_name);
		tab_ = sm_manager_->db_.get_table(tab_name_);
		conds_ = std::move(conds);
		index_col_names_ = index_col_names;
		index_meta_ = *(tab_.get_index_meta(index_col_names_));
		fh_ = sm_manager_->fhs_.at(tab_name_).get();
		cols_ = tab_.cols;
		len_ = cols_.back().offset + cols_.back().len;
		ix_handle_ = sm_manager_->ihs_[sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)].get();
		std::map<CompOp, CompOp>
			swap_op = {
				{OP_EQ, OP_EQ},
				{OP_NE, OP_NE},
				{OP_LT, OP_GT},
				{OP_GT, OP_LT},
				{OP_LE, OP_GE},
				{OP_GE, OP_LE},
			};
		for (auto &cond: conds_) {
			if (cond.lhs_col.tab_name != tab_name_) {
				// lhs is on other table, now rhs must be on this table
				assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
				// swap lhs and rhs
				std::swap(cond.lhs_col, cond.rhs_col);
				cond.op = swap_op.at(cond.op);
			}
		}
		fed_conds_ = conds_;
		begin_ = ix_handle_->leaf_begin();
		end_ = ix_handle_->leaf_end();
		int offset = 0;
		for (auto &name: index_col_names) {
			offsets_.push_back(offset);
			for (auto &col: cols_) {
				if (col.name == name) {
					offset += col.len;
				}
			}
		}
	}
	size_t tupleLen() const override {
		return len_;
	}

	const std::vector<ColMeta> &cols() override {
		return cols_;
	}
	void beginTuple() override {
		lower_cmp_key_ = std::make_unique<RmRecord>(ix_handle_->get_idx_len());
		upper_cmp_key_ = std::make_unique<RmRecord>(ix_handle_->get_idx_len());
		std::sort(conds_.begin(), conds_.end(), [&](Condition &lhs, Condition &rhs) {
			auto lhs_pos = std::find(index_col_names_.begin(), index_col_names_.end(), lhs.lhs_col.col_name) - index_col_names_.begin();
			auto rhs_pos = std::find(index_col_names_.begin(), index_col_names_.end(), rhs.lhs_col.col_name) - index_col_names_.begin();
			return lhs_pos < rhs_pos;
		});
		std::vector<std::vector<bool>> status(3, std::vector<bool>(200, false));
		size_t cnt_left = 0, cnt_right = 0;
		for (size_t i = 0; i < conds_.size(); i++) {
			const auto &cond = conds_[i];
			auto col = get_col(cols_, cond.lhs_col);
			auto pos = std::find(index_col_names_.begin(), index_col_names_.end(), cond.lhs_col.col_name) - index_col_names_.begin();
			if (cond.is_rhs_val && (cond.op != OP_EQ && cond.op != OP_NE)) {
				if (pos == 0) {
					// do nothing
				} else if (status[1][pos - 1]) {
					// do nothing
				} else if (status[0][pos] || status[2][pos]) {
					// do nothing
				} else {
					break;
				}
			}
			if (cond.is_rhs_val && cond.op != OP_NE) {
				switch (cond.op) {
					case OP_EQ:
						if (status[1][pos]) {
							break;
						}
						is_lower = true;
						is_upper = true;
						status[1][pos] = true;
						cnt_left++, cnt_right++;
						switch (col->type) {
							case TYPE_INT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										*(int *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_FLOAT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_FLOAT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.float_val;
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.float_val;
										break;
									case TYPE_BIGINT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_BIGINT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int64_t *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										*(int64_t *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_BIGINT:
										*(int64_t *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										*(int64_t *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_STRING:
								memcpy(lower_cmp_key_->data + offsets_[pos], cond.rhs_val.str_val.data(), cond.rhs_val.str_val.size());
								memcpy(upper_cmp_key_->data + offsets_[pos], cond.rhs_val.str_val.data(), cond.rhs_val.str_val.size());
								break;
							case TYPE_DATETIME:
								memcpy(lower_cmp_key_->data + offsets_[pos], cond.rhs_val.datetime_val.val.c_str(), cond.rhs_val.datetime_val.val.size());
								memcpy(upper_cmp_key_->data + offsets_[pos], cond.rhs_val.datetime_val.val.c_str(), cond.rhs_val.datetime_val.val.size());
								break;
							default:
								throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
						}
						break;
					case OP_GT:
					case OP_GE:
						cnt_left++;
						is_lower = true;
						if (status[0][pos]) {
							break;
						}
						status[0][pos] = true;
						switch (col->type) {
							case TYPE_INT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_FLOAT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_FLOAT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.float_val;
										break;
									case TYPE_BIGINT:
										*(double *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_BIGINT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int64_t *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_BIGINT:
										*(int64_t *) (lower_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_STRING:
								memcpy(lower_cmp_key_->data + offsets_[pos], cond.rhs_val.str_val.data(), cond.rhs_val.str_val.size());
								break;
							case TYPE_DATETIME:
								memcpy(lower_cmp_key_->data + offsets_[pos], cond.rhs_val.datetime_val.val.c_str(), cond.rhs_val.datetime_val.val.size());
								break;
							default:
								throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
						}
						break;
					case OP_LE:
					case OP_LT:
						cnt_right++;
						is_upper = true;
						if (status[2][pos]) {
							break;
						}
						status[2][pos] = true;
						switch (col->type) {
							case TYPE_INT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_FLOAT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_FLOAT:
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.float_val;
										break;
									case TYPE_BIGINT:
										*(double *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_BIGINT:
								switch (cond.rhs_val.type) {
									case TYPE_INT:
										*(int64_t *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.int_val;
										break;
									case TYPE_BIGINT:
										*(int64_t *) (upper_cmp_key_->data + offsets_[pos]) = cond.rhs_val.bigint_val;
										break;
									default:
										throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
								}
								break;
							case TYPE_STRING:
								memcpy(upper_cmp_key_->data + offsets_[pos], cond.rhs_val.str_val.data(), cond.rhs_val.str_val.size());
								break;
							case TYPE_DATETIME:
								memcpy(upper_cmp_key_->data + offsets_[pos], cond.rhs_val.datetime_val.val.c_str(), cond.rhs_val.datetime_val.val.size());
								break;
							default:
								throw IncompatibleTypeError(coltype2str(col->type), coltype2str(cond.rhs_val.type));
						}
						break;
					case OP_NE:
						throw std::logic_error("somewhere unknown");
				}
			}
		}
		if (is_lower) {
			size_t cnt = 0;
			for (size_t i = 0; i < conds_.size(); i++) {
				if (status[0][i] || status[1][i])
					cnt++;
				else
					break;
			}
			if (cnt == cnt_left) {
				if (cnt < index_col_names_.size()) {
					for (; cnt < index_col_names_.size(); cnt++) {
						const auto &name = index_col_names_[cnt];
						for (auto &col: cols_) {
							if (col.name == name) {
								switch (col.type) {
									case TYPE_INT:
										*(int *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int>::min();
										break;
									case TYPE_BIGINT:
										*(int64_t *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int64_t>::min();
										break;
									case TYPE_FLOAT:
										*(double *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<double>::min();
										break;
									case TYPE_STRING:
										memset(lower_cmp_key_->data + offsets_[cnt], 0, col.len);
										break;
									case TYPE_DATETIME:
										memset(lower_cmp_key_->data + offsets_[cnt], 0, col.len);
										break;
									default:
										throw std::logic_error("not supported");
								}
								break;
							}
						}
					}
				}
				begin_ = ix_handle_->lower_bound(lower_cmp_key_->data);
			}
		} else {
			for (size_t cnt = 0; cnt < index_col_names_.size(); cnt++) {
				const auto &name = index_col_names_[cnt];
				for (auto &col: cols_) {
					if (col.name == name) {
						switch (col.type) {
							case TYPE_INT:
								*(int *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int>::min();
								break;
							case TYPE_BIGINT:
								*(int64_t *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int64_t>::min();
								break;
							case TYPE_FLOAT:
								*(double *) (lower_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<double>::min();
								break;
							case TYPE_STRING:
								memset(lower_cmp_key_->data + offsets_[cnt], 0, col.len);
								break;
							case TYPE_DATETIME:
								memset(lower_cmp_key_->data + offsets_[cnt], 0, col.len);
								break;
							default:
								throw std::logic_error("not supported");
						}
						break;
					}
				}
			}
			begin_ = ix_handle_->lower_bound(lower_cmp_key_->data);
		}
		if (is_upper) {
			size_t cnt = 0;
			for (size_t i = 0; i < conds_.size(); i++) {
				if (status[1][i] || status[2][i])
					cnt++;
				else
					break;
			}
			if (cnt == cnt_right) {
				if (cnt < index_col_names_.size()) {
					for (; cnt < index_col_names_.size(); cnt++) {
						const auto &name = index_col_names_[cnt];
						for (auto &col: cols_) {
							if (col.name == name) {
								switch (col.type) {
									case TYPE_INT:
										*(int *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int>::max();
										break;
									case TYPE_FLOAT:
										*(double *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<double>::max();
										break;
									case TYPE_BIGINT:
										*(int64_t *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int64_t>::max();
										break;
									case TYPE_STRING:
										memset(upper_cmp_key_->data + offsets_[cnt], 0x7f, col.len);
										break;
									case TYPE_DATETIME:
										memset(upper_cmp_key_->data + offsets_[cnt], 0x7f, col.len);
										break;
									default:
										throw std::logic_error("not supported");
								}
								break;
							}
						}
					}
				}
				end_ = ix_handle_->upper_bound(upper_cmp_key_->data);
			}
		} else {
			for (size_t cnt = 0; cnt < index_col_names_.size(); cnt++) {
				const auto &name = index_col_names_[cnt];
				for (auto &col: cols_) {
					if (col.name == name) {
						switch (col.type) {
							case TYPE_INT:
								*(int *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int>::max();
								break;
							case TYPE_BIGINT:
								*(int64_t *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<int64_t>::max();
								break;
							case TYPE_FLOAT:
								*(double *) (upper_cmp_key_->data + offsets_[cnt]) = std::numeric_limits<double>::max();
								break;
							case TYPE_STRING:
								memset(upper_cmp_key_->data + offsets_[cnt], 0x7f, col.len);
								break;
							case TYPE_DATETIME:
								memset(upper_cmp_key_->data + offsets_[cnt], 0x7f, col.len);
								break;
							default:
								throw std::logic_error("not supported");
						}
						break;
					}
				}
			}
			end_ = ix_handle_->upper_bound(upper_cmp_key_->data);
		}
		scan_ = std::make_unique<IxScan>(ix_handle_, begin_, end_, sm_manager_->get_bpm());

		while (!scan_->is_end()) {
			rid_ = scan_->rid();
			if (!checkCondition(fh_->get_record(rid_, nullptr)))
				scan_->next();
			else
				break;
		}
	}

	void nextTuple() override {
		scan_->next();
		while (!scan_->is_end() && !checkCondition(fh_->get_record(rid_ = scan_->rid(), nullptr))) {
			scan_->next();
		}
	}

	bool is_end() const override {
		return scan_->is_end();
	}

	std::unique_ptr<RmRecord> Next() override {
		auto rec = fh_->get_record(rid_, nullptr);
		return rec;
	}

	Rid &rid() override { return rid_; }
};
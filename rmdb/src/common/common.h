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
#include "record/rm_defs.h"
#include <cassert>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


struct TabCol {
	std::string tab_name;
	std::string col_name;

	friend bool operator<(const TabCol &x, const TabCol &y) {
		return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
	}
};

struct Value {
	ColType type;// type of value
	union {
		int int_val;       // int value
		double float_val;  // float value
		int64_t bigint_val;// big_int val
	};
	std::string str_val;  // string value
	Datetime datetime_val;//datetime value;

	std::shared_ptr<RmRecord> raw;// raw record buffer

	void set_int(int int_val_) {
		type = TYPE_INT;
		int_val = int_val_;
	}

	void set_float(double float_val_) {
		type = TYPE_FLOAT;
		float_val = float_val_;
	}

	void set_str(std::string str_val_) {
		type = TYPE_STRING;
		str_val = std::move(str_val_);
	}

	void set_datetime(Datetime str_val_) {
		type = TYPE_DATETIME;
		datetime_val = std::move(str_val_);
	}

	void set_bigint(int64_t bigint_val_) {
		type = TYPE_BIGINT;
		bigint_val = bigint_val_;
	}

	void init_raw(size_t len) {
		assert(raw == nullptr);
		raw = std::make_shared<RmRecord>(len);
		if (type == TYPE_INT) {
			assert(len == sizeof(int));
			*(int *) (raw->data) = int_val;
		} else if (type == TYPE_FLOAT) {
			assert(len == sizeof(double));
			*(double *) (raw->data) = float_val;
		} else if (type == TYPE_STRING) {
			if (len < str_val.size()) {
				throw StringOverflowError();
			}
			memset(raw->data, 0, len);
			memcpy(raw->data, str_val.c_str(), str_val.size());
		} else if (type == TYPE_DATETIME) {
			if (len < datetime_val.val.size()) {
				throw StringOverflowError();
			}
			memset(raw->data, 0, datetime_val.val.size());
			memcpy(raw->data, datetime_val.val.c_str(), datetime_val.val.size());
		} else if (type == TYPE_BIGINT) {
			assert(len == sizeof(int64_t));
			*(int64_t *) (raw->data) = bigint_val;
		} else {
			assert(false);
		}
	}

	void str_resize(size_t len) {
		assert(type == TYPE_STRING);
		if (len < str_val.size()) {
			throw StringOverflowError();
		}
		raw = std::make_shared<RmRecord>(len);
		memset(raw->data, 0, len);
		memcpy(raw->data, str_val.c_str(), str_val.size());
	}
};

enum CompOp { OP_EQ,
							OP_NE,
							OP_LT,
							OP_GT,
							OP_LE,
							OP_GE };

struct Condition {
	TabCol lhs_col; // left-hand side column
	CompOp op;      // comparison operator
	bool is_rhs_val;// true if right-hand side is a value (not a column)
	TabCol rhs_col; // right-hand side column
	Value rhs_val;  // right-hand side value
};

enum class SetClauseOp {
	SELF_ADD,
	SELF_SUB,
	SELF_MUT,
	SELF_DIV
};

struct SetClause {
	TabCol lhs;
	Value rhs;
	bool is_self;
	SetClauseOp op;
	SetClause(TabCol lhs_, Value rhs_) {
		is_self = false;
		lhs = lhs_, rhs = rhs_;
	}
	SetClause(TabCol lhs_, Value rhs_, int op_) {
		is_self = true;
		lhs = lhs_, rhs = rhs_;
		switch (op_) {
			case 0:
				op = SetClauseOp::SELF_ADD;
				break;
			case 1:
				op = SetClauseOp::SELF_SUB;
				break;
			case 2:
				op = SetClauseOp::SELF_MUT;
				break;
			case 3:
				op = SetClauseOp::SELF_DIV;
				break;
			default:
				throw std::logic_error("illegal operation");
		}
	}
};
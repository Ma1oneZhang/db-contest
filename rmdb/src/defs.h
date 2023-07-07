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

#include <cstring>
#include <iostream>
#include "errors.h"
#include <map>
constexpr static int month_[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};


// 此处重载了<<操作符，在ColMeta中进行了调用
template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Datetime {
    std::string val; 

	// Datetime(const char* val_) :val(val_) {}; 
	Datetime() {}; 
	Datetime(const std::string &val_){ 
        check_datetime_legal(val_); 
        this->val = val_; 
    };  
	Datetime(const Datetime& datetime) { 
        // check_datetime_legal(datetime.val);
        this->val = datetime.val; 
    }

    void check_datetime_legal(std::string val){
        auto datetime = val.c_str(); 
		int yy, mm, dd, h, m, s; 
		sscanf(datetime, "%d-%d-%d %d:%d:%d", &yy, &mm, &dd, &h, &m, &s);
		if (      strlen(datetime) != 19
                ||yy < 1000 || yy > 9999
				|| mm < 1 || mm > 12 
				|| dd < 1 || (mm != 2 && dd > month_[mm])
				|| (mm == 2 && (((0 == yy % 4 && yy % 100 != 0) || (0 == yy % 400)) ? dd > 29 : dd > 28))
				|| h < 0 || h > 23 
				|| m < 0 || m > 59 	
				|| s < 0 || s > 59) {

			throw DatetimeError("Datetime format error");
		}
    }

	const Datetime operator=(const Datetime &datetime) {
        // check_datetime_legal(datetime.val);
		this->val = datetime.val; 
		return *this;
	}
	const Datetime operator=(const std::string &val_) {
        check_datetime_legal(val_);
		this->val = val_; 
		return *this; 
	}

	friend std::ostream &operator<<(std::ostream &os, Datetime &datetime) {
		os << datetime.val;
		return os;
	}
};

struct Rid {
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

enum ColType {
    TYPE_INT, 
    TYPE_FLOAT, 
    TYPE_STRING,
    TYPE_DATETIME,
    TYPE_NONE
};

inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,      "INT"},
            {TYPE_FLOAT,    "FLOAT"},
            {TYPE_STRING,   "STRING"},
            {TYPE_DATETIME, "DATETIME"}
    };
    return m.at(type);
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};

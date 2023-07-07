/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#undef NDEBUG

#include <cassert>

#include "parser.h"

int main() {
	std::vector<std::string> sqls = {
		"create table tb (a int, b datetime);",
		"",
	};
	for (auto &sql: sqls) {
		std::cout << sql << std::endl;
		YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
		assert(yyparse() == 0);
		if (ast::parse_tree != nullptr) {
			ast::TreePrinter::print(ast::parse_tree);
			yy_delete_buffer(buf);
			std::cout << std::endl;
		} else {
			std::cout << "exit/EOF" << std::endl;
		}
    }
    ast::parse_tree.reset();
    return 0;
}

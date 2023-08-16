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
		// "show tables;",
		// "desc tb;",
		// "create table tb (a int, b float, c char(4));",
		// "drop table tb;",
		// "create index tb(a);",
		// "create index tb(a, b, c);",
		// "drop index tb(a, b, c);",
		// "drop index tb(b);",
		// "insert into tb values (1, 3.14, 'pi');",
		// "delete from tb where a = 1;",
		// "update tb set a = 1, b = 2.2, c = 'xyz' where x = 2 and y < 1.1 and z > 'abc';",
		// "select * from tb;",
		// "select * from tb where x <> 2 and y >= 3. and z <= '123' and b < tb.a;",
		// "select x.a, y.b from x, y where x.a = y.b and c = d;",
		// "select x.a, y.b from x join y where x.a = y.b and c = d;",
		// "update grade set score = score + 5 where name = 'Calculus';",
		// "show index from warehouse;", 
		// "insert into a values(123123123);",
		// "insert into a values(123123123123123123);",
		// "CREATE TABLE t(bid bigint,sid int);",
		// "insert into a values(123123123123123123123123`);",
		// "exit;",
		// "select MAX(a) as b from t;",
		// "select MAX(a) from t;",
		// "select MAX(a), MIN(b) from t;",
		// "select * from t;",
		// "select * from t order by a,b desc, c asc limit 3;",
		// "LOAD ../../path/file_name.csv INTO tbName",
		// "set output_file off",
		// "update greade set score = score+5;",
		// "update greade set score = score-5;",
		// "update greade set score = score+5.0;",
		// "update greade set score = score-5.1234;",
		"select t.id,t_name,d_name from t,d where t.id = d.id;",
		"help;",
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

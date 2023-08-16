/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <cstddef>
#include <cstring>
#include <ostream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "defs.h"
#include "errors.h"
#include "index/ix.h"
#include "record/rm.h"
#include "record/rm_defs.h"
#include "record/rm_scan.h"
#include "record_printer.h"
#include "system/sm_meta.h"
#include "parser/parser_defs.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name) {
	struct stat st;
	return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name) {
	if (is_dir(db_name)) {
		throw DatabaseExistsError(db_name);
	}
	//为数据库创建一个子目录
	std::string cmd = "mkdir " + db_name;
	if (system(cmd.c_str()) < 0) {// 创建一个名为db_name的目录
		throw UnixError();
	}
	if (chdir(db_name.c_str()) < 0) {// 进入名为db_name的目录
		throw UnixError();
	}
	//创建系统目录
	DbMeta *new_db = new DbMeta();
	new_db->name_ = db_name;

	// 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
	std::ofstream ofs(DB_META_NAME);

	// 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
	ofs << *new_db;// 注意：此处重载了操作符<<

	delete new_db;

	// 创建日志文件
	disk_manager_->create_file(LOG_FILE_NAME);

	// 回到根目录
	if (chdir("..") < 0) {
		throw UnixError();
	}
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name) {
	if (!is_dir(db_name)) {
		throw DatabaseNotFoundError(db_name);
	}
	std::string cmd = "rm -r " + db_name;
	if (system(cmd.c_str()) < 0) {
		throw UnixError();
	}
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name) {
	if (!is_dir(db_name)) {
		throw DatabaseNotFoundError(db_name);
	}
	if (chdir(db_name.c_str()) < 0) {// 进入名为db_name的目录
		throw UnixError();
	}
	// read the meta file
	std::ifstream disk_meta_file(DB_META_NAME);
	disk_meta_file >> db_;
	// take open the data_file
	for (auto &[_, tab_meta]: db_.tabs_) {
		fhs_[tab_meta.name] = rm_manager_->open_file(tab_meta.name);
		std::vector<ColMeta> indexes;
		for (size_t i = 0; i < tab_meta.cols.size(); i++) {
			if (tab_meta.cols[i].index) {
				indexes.emplace_back(tab_meta.cols[i]);
			}
		}
		if (indexes.size()) {
			ihs_[tab_meta.name] = ix_manager_->open_index(tab_meta.name, indexes);
		} else {
			ihs_[tab_meta.name] = nullptr;
		}
	}
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
	// 默认清空文件
	std::ofstream ofs(DB_META_NAME);
	ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {


	for (auto &[name, handle]: fhs_) {
		rm_manager_->close_file(handle.get());
	}
	fhs_.clear();
	for (auto &[name, handle]: ihs_) {
		ix_manager_->close_index(handle.get());
	}
	ihs_.clear();

	flush_meta();
	db_.name_.clear();
	db_.tabs_.clear();

	// 回到根目录
	if (chdir("..") < 0) {
		throw UnixError();
	}
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context *context) {
	std::string outfile_content(""); //要写入到outfile中
	outfile_content += "| Tables |\n"; 

	RecordPrinter printer(1);
	printer.print_separator(context);
	printer.print_record({"Tables"}, context);
	printer.print_separator(context);
	for (auto &entry: db_.tabs_) {
		auto &tab = entry.second;
		printer.print_record({tab.name}, context);
		outfile_content += "| " + tab.name + " |\n"; 
	}

	disk_manager_->write_outfile(outfile_content); 
	printer.print_separator(context);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string &tab_name, Context *context) {
	TabMeta &tab = db_.get_table(tab_name);

	std::vector<std::string> captions = {"Field", "Type", "Index"};
	RecordPrinter printer(captions.size());
	// Print header
	printer.print_separator(context);
	printer.print_record(captions, context);
	printer.print_separator(context);
	// Print fields
	for (auto &col: tab.cols) {
		std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
		printer.print_record(field_info, context);
	}
	// Print footer
	printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context) {
	if (db_.is_table(tab_name)) {
		throw TableExistsError(tab_name);
	}
	// Create table meta
	int curr_offset = 0;
	TabMeta tab;
	tab.name = tab_name;
	std::unordered_set<std::string> exist;
	for (auto &col_def: col_defs) {
		if (exist.count(col_def.name)) {
			throw AmbiguousColumnError(col_def.name);
		}
		exist.insert(col_def.name);
		ColMeta col = {.tab_name = tab_name,
									 .name = col_def.name,
									 .type = col_def.type,
									 .len = col_def.len,
									 .offset = curr_offset,
									 .index = false};
		curr_offset += col_def.len;
		tab.cols.push_back(col);
	}
	// Create & open record file
	int record_size = curr_offset;// record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
	rm_manager_->create_file(tab_name, record_size);
	db_.tabs_[tab_name] = tab;
	// fhs_[tab_name] = rm_manager_->open_file(tab_name);
	fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

	flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string &tab_name, Context *context) {
	auto &tab_meta = db_.get_table(tab_name);
	auto fd = disk_manager_->get_file_fd(tab_name);
	buffer_pool_manager_->delete_all_pages(fd);
	rm_manager_->close_file(fhs_[tab_name].get());
	rm_manager_->destroy_file(tab_name);
	std::vector<ColMeta> indexes;
	for (size_t i = 0; i < tab_meta.cols.size(); i++) {
		if (tab_meta.cols[i].index) {
			indexes.emplace_back(tab_meta.cols[i]);
		}
	}
	if (indexes.size()) {
		ix_manager_->destroy_index(tab_name, indexes);
		ihs_.erase(tab_name);
	}
	fhs_.erase(tab_name);
	db_.tabs_.erase(tab_name);
	flush_meta();
}


/**
 * @description: 显示索引
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::show_index(const std::string &tab_name, Context *context) {

	RecordPrinter printer(1);
	auto &tab_meta = db_.get_table(tab_name);

	for (auto index: tab_meta.indexes) {
		std::string str("");
		str += "| " + tab_name + " | unique | (";
		for (size_t i = 0; i < index.col_num; i++) {
			if (i >= 1) str += ",";
			str += index.cols[i].name;
		}
		str += ") |\n";

		disk_manager_->write_outfile(str);
		printer.print_string(str, 40, context);
	}
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context) {
	auto &tab_meta = db_.get_table(tab_name);

	//如果当前索引已经存在
	if (tab_meta.is_index(col_names)) {
		throw IndexExistsError(tab_name, col_names);
	}

	//需要创建的索引
	std::vector<ColMeta> index_cols;
	size_t col_tot_len = 0;
	for (auto col_name: col_names) {
		auto col = tab_meta.get_col(col_name);
		col_tot_len += col->len;
		index_cols.emplace_back(*col);
	}

	if (index_cols.size()) {
		ix_manager_->create_index(tab_name, index_cols);
	}
	auto index_name_ = ix_manager_->get_index_name(tab_name, index_cols);
	auto ih = ix_manager_->open_index(tab_name, index_cols);
	auto file_handle = fhs_.at(tab_name).get();
	// check the duplicate
	std::unordered_set<std::string> vaildate_set;
	for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next()) {
		auto rec = file_handle->get_record(rm_scan.rid(), context);
		auto index_rec = std::make_unique<RmRecord>(col_tot_len);
		int tot_offset = 0;
		for (auto &col: index_cols) {
			memcpy(index_rec->data + tot_offset, rec->data + col.offset, col.len);
			tot_offset += col.len;
		}
		auto bin_data = std::string(index_rec->data, col_tot_len);
		if (vaildate_set.count(bin_data)) {
			throw DuplicateKeyError("update key duplicate");
		}
		vaildate_set.insert(bin_data);
	}
	for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next()) {
		auto rec = file_handle->get_record(rm_scan.rid(), context);
		auto index_rec = std::make_unique<RmRecord>(col_tot_len);
		int tot_offset = 0;
		for (auto &col: index_cols) {
			memcpy(index_rec->data + tot_offset, rec->data + col.offset, col.len);
			tot_offset += col.len;
		}
		ih->insert_entry(index_rec->data, rm_scan.rid(), context->txn_);
	}
	// ih->Draw(buffer_pool_manager_, "after_insert_3000_element.dot");

	// index_name = warehouse_id_xxx_xxx.idx
	auto index_name = ix_manager_->get_index_name(tab_name, index_cols);
	assert(ihs_.count(index_name) == 0);
	ihs_.emplace(index_name, std::move(ih));


	//加入索引数据
	IndexMeta indexes = {.tab_name = tab_name,
											 .col_tot_len = col_tot_len,
											 .col_num = col_names.size(),
											 .cols = std::move(index_cols)};

	tab_meta.indexes.push_back(indexes);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context) {
	auto &tab_meta = db_.tabs_.at(tab_name);

	//当前索引不存在
	if (!tab_meta.is_index(col_names)) {
		throw IndexNotFoundError(tab_name, col_names);
	}

	//需要创建的索引
	std::vector<ColMeta> index_cols;
	for (auto col_name: col_names) {
		auto col = tab_meta.get_col(col_name);
		index_cols.emplace_back(*col);
	}

	auto index_name = ix_manager_->get_index_name(tab_name, index_cols);
	buffer_pool_manager_->delete_all_pages(ihs_.at(index_name)->get_fd_());
	ix_manager_->close_index(ihs_.at(index_name).get());
	ix_manager_->destroy_index(tab_name, index_cols);
	ihs_.erase(index_name);

	//删除索引
	auto indexes = tab_meta.get_index_meta(col_names);
	tab_meta.indexes.erase(indexes);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context) {
	std::vector<std::string> col_names;
	for (auto col: cols) {
		col_names.emplace_back(col.name);
	}

	drop_index(tab_name, col_names, context);
}

void IxIndexHandle::ToGraph(const IxIndexHandle *ih, IxNodeHandle *node, BufferPoolManager *bpm, std::ofstream &out) {
	std::string leaf_prefix("LEAF_");
	std::string internal_prefix("INT_");
	if (node->is_leaf_page()) {
		IxNodeHandle *leaf = node;
		// Print node name
		out << leaf_prefix << leaf->get_page_no();
		// Print node properties
		out << "[shape=plain color=green ";
		// Print data of the node
		out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
		// Print data
		out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">page_no=" << leaf->get_page_no() << "</TD></TR>\n";
		out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">"
				<< "max_size=" << leaf->get_max_size() << ",min_size=" << leaf->get_min_size() << "</TD></TR>\n";
		out << "<TR>";
		for (int i = 0; i < leaf->get_size(); i++) {
			out << "<TD>" << leaf->key_at(i) << "</TD>\n";
		}
		out << "</TR>";
		// Print table end
		out << "</TABLE>>];\n";
		// Print Leaf node link if there is a next page
		if (leaf->get_next_leaf() != INVALID_PAGE_ID && leaf->get_next_leaf() > 1) {
			// 注意加上一个大于1的判断条件，否则若GetNextPageNo()是1，会把1那个结点也画出来
			out << leaf_prefix << leaf->get_page_no() << " -> " << leaf_prefix << leaf->get_next_leaf() << ";\n";
			out << "{rank=same " << leaf_prefix << leaf->get_page_no() << " " << leaf_prefix << leaf->get_next_leaf()
					<< "};\n";
		}

		// Print parent links if there is a parent
		if (leaf->get_parent_page_no() != INVALID_PAGE_ID) {
			out << internal_prefix << leaf->get_parent_page_no() << ":p" << leaf->get_page_no() << " -> " << leaf_prefix
					<< leaf->get_page_no() << ";\n";
		}
	} else {
		IxNodeHandle *inner = node;
		// Print node name
		out << internal_prefix << inner->get_page_no();
		// Print node properties
		out << "[shape=plain color=pink ";// why not?
		// Print data of the node
		out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
		// Print data
		out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">page_no=" << inner->get_page_no() << "</TD></TR>\n";
		out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">"
				<< "max_size=" << inner->get_max_size() << ",min_size=" << inner->get_min_size() << "</TD></TR>\n";
		out << "<TR>";
		for (int i = 0; i < inner->get_size(); i++) {
			out << "<TD PORT=\"p" << inner->value_at(i) << "\">";
			out << inner->key_at(i);
			// if (inner->key_at(i) != 0) {  // 原判断条件是if (i > 0)
			//     out << inner->key_at(i);
			// } else {
			//     out << " ";
			// }
			out << "</TD>\n";
		}
		out << "</TR>";
		// Print table end
		out << "</TABLE>>];\n";
		// Print Parent link
		if (inner->get_parent_page_no() != INVALID_PAGE_ID) {
			out << internal_prefix << inner->get_parent_page_no() << ":p" << inner->get_page_no() << " -> "
					<< internal_prefix << inner->get_page_no() << ";\n";
		}
		// Print leaves
		for (int i = 0; i < inner->get_size(); i++) {
			IxNodeHandle *child_node = ih->fetch_node(inner->value_at(i));
			ToGraph(ih, child_node, bpm, out);// 继续递归
			if (i > 0) {
				IxNodeHandle *sibling_node = ih->fetch_node(inner->value_at(i - 1));
				if (!sibling_node->is_leaf_page() && !child_node->is_leaf_page()) {
					out << "{rank=same " << internal_prefix << sibling_node->get_page_no() << " " << internal_prefix
							<< child_node->get_page_no() << "};\n";
				}
				bpm->unpin_page(sibling_node->get_page_id(), false);
			}
		}
	}
	bpm->unpin_page(node->get_page_id(), false);
}

void IxIndexHandle::Draw(BufferPoolManager *bpm, const std::string &outf) {
	std::ofstream out(outf);
	out << "digraph G {" << std::endl;
	IxNodeHandle *node = fetch_node(file_hdr_->root_page_);
	ToGraph(this, node, bpm, out);
	out << "}" << std::endl;
	out.close();

	// 由dot文件生成png文件
	std::string prefix = outf;
	prefix.replace(outf.rfind(".dot"), 4, "");
	std::string png_name = prefix + ".svg";
	std::string cmd = "dot -Tsvg " + outf + " -o " + png_name;
	system(cmd.c_str());

	// printf("Generate picture: build/%s/%s\n", TEST_DB_NAME.c_str(), png_name.c_str());
	printf("Generate picture: %s\n", png_name.c_str());
}
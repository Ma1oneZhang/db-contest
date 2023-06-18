/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "record/bitmap.h"
#include "rm_file_handle.h"
#include "utils/log.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
	// Todo:
	// 初始化file_handle和rid（指向第一个存放了记录的位置）
	for (int i = 1; i <= file_handle->file_hdr_.num_pages; i++) {
		if (i == file_handle_->file_hdr_.num_pages) {
			rid_ = {i, -1};
			return;
		}
		auto page_handle = file_handle->fetch_page_handle(i);
		if (page_handle.page_hdr->num_records != 0) {
			rid_ = {i, Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, -1)};
			file_handle->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
			return;
		}
		file_handle->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
	}
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
	// Todo:
	// 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
	bool new_page = false;
	if (rid_.slot_no == -1) {
		return;
	}
	for (int i = rid_.page_no; i <= file_handle_->file_hdr_.num_pages; i++) {
		if (i == file_handle_->file_hdr_.num_pages) {
			LOG_INFO("RETURN")
			rid_ = {i, -1};
			return;
		}
		auto page_handle = file_handle_->fetch_page_handle(i);
		if (page_handle.page_hdr->num_records != 0) {
			auto slot_no = Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, new_page ? -1 : rid_.slot_no);
			LOG_INFO("page_on = %d, num_records_per_page = %d", i, page_handle.file_hdr->num_records_per_page)
			LOG_INFO("start index = %d", new_page ? -1 : rid_.slot_no)
			LOG_INFO("find slot = %d", slot_no)
			if (slot_no == page_handle.file_hdr->num_records_per_page) {
				rid_.slot_no = -1;
				continue;
			}
			rid_ = {i, slot_no};
			file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
			LOG_INFO("GOT RID %d, %d", rid_.page_no, rid_.slot_no)
			return;
		}
		file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
		new_page = true;
	}
}
/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
	// Todo: 修改返回值
	return rid_.slot_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
	return rid_;
}
/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"
#include <mutex>
#include <stdexcept>

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
	// Todo:
	// 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
	// 1.1 未满获得frame
	// 1.2 已满使用lru_replacer中的方法选择淘汰页面
	if (free_list_.size() != 0) {
		*frame_id = *free_list_.rbegin();
		free_list_.pop_back();
		return true;
	}
	return replacer_->victim(frame_id);
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
	// 1 如果是脏页，写回磁盘，并且把dirty置为false
	if (page->is_dirty()) {
		page->mu.unlock();
		// reset is_dirty to false
		flush_page(page->get_page_id());
		page->mu.lock();
	}
	// 2 更新page table
	page_table_.Erase(page->get_page_id());
	page_table_.Insert(new_page_id, new_frame_id);
	// 3 重置page的data，更新page id
	page->set_page_id(new_page_id);
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page *BufferPoolManager::fetch_page(PageId page_id) {
	//Todo:
	// 4.     固定目标页，更新pin_count_
	// 5.     返回目标页
	frame_id_t frame{-1};
	// 1.     从page_table_中搜寻目标页
	if (!page_table_.Find(page_id, frame)) {
		// 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
		// 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
		{
			std::scoped_lock latch{latch_};
			if (!find_victim_page(&frame)) {
				return nullptr;
			}
		}
		{
			std::scoped_lock page_latch{pages_[frame].mu};
			// 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
			if (pages_[frame].is_dirty()) {
				update_page(&pages_[frame], page_id, frame);
			} else {
				page_table_.Erase(pages_[frame].get_page_id());
				page_table_.Insert(page_id, frame);
			}
			// 3.     调用disk_manager_的read_page读取目标页到frame
			disk_manager_->read_page(page_id.fd, page_id.page_no, pages_[frame].get_data(), PAGE_SIZE);
			pages_[frame].set_page_id(page_id);
		}
	}
	std::scoped_lock page_latch{pages_[frame].mu};
	pin_count_[frame]++;
	return &pages_[frame];
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(const PageId &page_id, bool is_dirty) {
	// Todo:
	// 0. lock latch
	// we are using atomic varible so we are lock_free in this function
	// 1. 尝试在page_table_中搜寻page_id对应的页P
	// 1.1 P在页表中不存在 return false
	// 1.2 P在页表中存在，获取其pin_count_
	// 2.1 若pin_count_已经等于0，则返回false
	// 2.2 若pin_count_大于0，则pin_count_自减一
	// 2.2.1 若自减后等于0，则调用replacer_的Unpin
	// 3 根据参数is_dirty，更改P的is_dirty_
	frame_id_t frame{-1};
	// 1.     从page_table_中搜寻目标页
	if (!page_table_.Find(page_id, frame)) {
		return false;
	}
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		if (pin_count_[frame] == 0) {
			return false;
		}
	}
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		pages_[frame].is_dirty_ |= is_dirty;
		pin_count_[frame]--;
		if (pin_count_[frame] == 0) {
			replacer_->unpin(frame);
		}
	}
	return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(const PageId &page_id) {
	// Todo:
	// 0. lock latch
	// 1. 查找页表,尝试获取目标页P
	// 1.1 目标页P没有被page_table_记录 ，返回false
	// 2. 无论P是否为脏都将其写回磁盘。
	// 3. 更新P的is_dirty_
	frame_id_t frame = {-1};
	if (!page_table_.Find(page_id, frame)) {
		return false;
	}
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		Page *page = &pages_[frame];
		disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
		page->is_dirty_ = false;
	}
	return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id) {
	// 1.   获得一个可用的frame，若无法获得则返回nullptr
	// 2.   在fd对应的文件分配一个新的page_id
	// 3.   将frame的数据写回磁盘
	// 4.   固定frame，更新pin_count_
	// 5.   返回获得的page
	frame_id_t frame{-1};
	{
		std::scoped_lock latch{latch_};
		if (free_list_.size() == 0) {
			if (!find_victim_page(&frame)) {
				return nullptr;
			}
		} else {
			frame = *free_list_.begin();
			free_list_.pop_front();
		}
	}
	*page_id = {page_id->fd, disk_manager_->allocate_page(page_id->fd)};
	PageId page_id_{};
	bool is_page_dirty;
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		page_id_ = pages_[frame].get_page_id();
		is_page_dirty = pages_[frame].is_dirty();
	}
	if (is_page_dirty)
		flush_page(page_id_);
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		page_table_.Erase(pages_[frame].get_page_id());
		pages_[frame].set_page_id(*page_id);
		pin_count_[frame]++;
	}
	page_table_.Insert(*page_id, frame);
	return &pages_[frame];
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(const PageId &page_id) {
	// 1.   在page_table_中查找目标页，若不存在返回true
	// 2.   若目标页的pin_count不为0，则返回false
	// 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
	frame_id_t frame{-1};
	if (!page_table_.Find(page_id, frame)) {
		return true;
	}
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		if (pin_count_[frame]) {
			return false;
		}
		page_table_.Erase(pages_[frame].get_page_id());
	}
	flush_page(page_id);
	{
		std::scoped_lock page_latch{pages_[frame].mu};
		pages_[frame].reset_memory();
	}
	{
		std::scoped_lock latch{latch_};
		free_list_.emplace_front(frame);
	}
	return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
	for (size_t i = 0; i < pool_size_; i++) {
		{
			std::scoped_lock page_latch{pages_[i].mu};
			if (fd == pages_[i].get_page_id().fd) {
				Page *page = &pages_[i];
				disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
				page->is_dirty_ = false;
			}
		}
	}
}
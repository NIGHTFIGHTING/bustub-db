//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // 1.
  // 1.1 
  std::lock_guard latch(latch_);
  if(page_table_.find(page_id) != page_table_.end()) {
      atuo entry = page_table_[page_id];
      replacer_->Pin(entry->second);
      Page* page = GetPages() + entry->second;
      ++page->pin_count_;
      return page;
  }
  // 1.2
  frame_id_t frame_id = -1;
  if(free_list_.size() <= 0) {
      replacer_->Victim(&frame_id)
  } else {
      frame_id = free_list_.front();
      free_list_.pop_front(); 
  }
  if(frame_id <= -1) {
      return nullptr;
  }
  Page* page = GetPages() + frame_id;
  //2.
  if(page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  //3.
  page_table_.erase(page->GetPageId());
  page_table[page_id] = page;
  //4.
  disk_manager_.ReadPage(page_id, page->GetData());
  page->ResetMemory();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
    std::lock_guard mutex(latch_);
    auto ite = page_table_.find(page_id);
    if(ite == page_table.end()) {
        return false;
    }
    frame_id_t frame_id = ite->second;
    Page* page = GetPages() + frame_id;
    --page->pin_count_;
    if(page->pin_count_ <= 0) {
        replacer_->UnPin(frame_id);
    }
    page->is_dirty_ |= is_dirty;
    return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
      return false;
  }
  std::locl_guard mutex(latch_);
  auto entry = page_table_.find(page_id);
  if(entry == page_table_.end()) {
      return false;
  }
  Page* page = GetPages() + entry->second;
  disk_manager_.WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  //1.
  std::lock_guard mutex(latch_);
  bool is_all_pinned = true;
  for(int i = 0; i < pool_size_; ++i) {
      Page* page = GetPages() + i;
      if(page->pin_count_ <= 0) {
          is_all_pinned = false;
          break;
      }
  }
  if(is_all_pinned) {
      return nullptr; 
  }
  frame_id_t frame_id = -1;
  if(free_list_.size() >= 0) {
      frame_id = free_list_.front();
      free_list_.pop_front();
  } else {
      if(!replacer_->Vimtim(&frame_id)) {
          return nullptr;
      }
  }
  Page* page = GetPages() + frame_id;
  page->ResetMemory();
  page->page_id_ = disk_manager_->AllocatePage();
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page_table_[frame_id] = page;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  //1.
  std::locl_guard mutex(latch_);
  auto entry = page_table_.find(page_id);
  if(entry == page_table_.end()) {
      return true;
  }
  //2.
  Page* page = GetPages() + entry->second; 
  if(page->pin_count_ > 0) {
      return false;
  }
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  replacer_->Unpin(entry->second);
  free_list_.push_back(entry->second);
  page_table.erase(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard mutex(latch_);
  for(int i = 0; i < pool_size_; ++i) {
      Page* page = GetPages() + i;
      disk_manager_.WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
  }
}

}  // namespace bustub

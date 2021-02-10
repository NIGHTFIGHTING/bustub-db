/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace cmudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
    return page_type_ == IndexPageType::LEAF_PAGE;
}
bool BPlusTreePage::IsRootPage() const {
    return parent_page_id_ == INVALID_PAGE_ID;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
    return size_;
}
void BPlusTreePage::SetSize(int size) {
    size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
    size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const {
    return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
    //p486�������ڲ��ڵ���������n��ָ�룬����Ӧ����n/2����ȡ����ָ��
    //����Ҷ�ӽڵ㣬��������n��ֵ������Ӧ����n/2����ȡ����ֵ
    if (IsRootPage() && IsLeafPage()) {
        // ֻ��һ��Ҷ�ӽڵ��������Ҷ�ӽڵ����sizeΪ0����ôӦ�õ���header page
        return 1;
    }
    return IsRootPage() ? 2 : (GetMaxSize() + 1) / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) {
    page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

bool BPlusTreePage::IsSafe(OperationType op) {
    if (op == OperationType::GET) {
        return true;
    } else if (op == OperationType::INSERT) {
        return GetSize() < GetMaxSize();
    } else if (op == OperationType::DELETE) {
        return GetSize() > GetMinSize();
    }
    return false;
}

} // namespace cmudb

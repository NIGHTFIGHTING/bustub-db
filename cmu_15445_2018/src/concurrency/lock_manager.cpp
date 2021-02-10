/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
#include <cassert>

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetSharedLockSet()->count(rid) == 0);

    Request request{txn->GetTransactionId(), LockMode::SHARED, false};
    if (lock_table_.count(rid) == 0) {
        lock_table_[rid].oldest = txn->GetTransactionId();
        lock_table_[rid].list.push_back(request);
    } else {
        // ����ȴ�������û�����������Ͳ���Ҫ������ϳ̶��ˣ���Ϊ��������¹��������ᱻ��Ȩ
        // ������ֵȴ��������Ҳ�Ͳ�������
        if (lock_table_[rid].exclusive_cnt != 0 && txn->GetTransactionId() > lock_table_[rid].oldest) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        } else {
            lock_table_[rid].oldest = txn->GetTransactionId();
            lock_table_[rid].list.push_back(request);
        }
    }

    // ͨ��������ǰ��ȫ������Ȩ��shared����
    Request *cur = nullptr;
    cv_.wait(lk, [&]() -> bool {

        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it->txn_id != txn->GetTransactionId()) {
                if (it->lock_mode != LockMode::SHARED || it->granted) {
                    return false;
                }
            } else {
                cur = &(*it);
                break;
            }
        }
        return true;
    });

    cur->granted = true;
    txn->GetSharedLockSet()->insert(rid);

    // �����Ѿ������˱仯�����������������л����ȡ
    cv_.notify_all();
    return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetExclusiveLockSet()->count(rid) == 0);

    Request request{txn->GetTransactionId(), LockMode::EXCLUSIVE, false};
    if (lock_table_.count(rid) == 0) {
        lock_table_[rid].oldest = txn->GetTransactionId();
        lock_table_[rid].list.push_back(request);
    } else {
        if (txn->GetTransactionId() > lock_table_[rid].oldest) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        } else {
            lock_table_[rid].oldest = txn->GetTransactionId();
            lock_table_[rid].list.push_back(request);
        }
    }
    // ͨ����������ǰ����֮ǰû���κ�����Ȩ������
    Request *cur = nullptr;
    cv_.wait(lk, [&]() -> bool {
        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it->txn_id != txn->GetTransactionId()) {
                if (it->granted) {
                    return false;
                }
            } else {
                cur = &(*it);
                break;
            }
        }
        return true;
    });

    cur->granted = true;
    lock_table_[rid].exclusive_cnt++;
    txn->GetExclusiveLockSet()->insert(rid);

    // ��Ȩһ�������������۹������������������������л����ȡ�����Բ���Ҫnotify
    return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    assert(txn->GetState() == TransactionState::GROWING);
    assert(txn->GetSharedLockSet()->count(rid) != 0);

    // ͨ����������Ҫ���������shared������Ψһһ��granted����
    cv_.wait(lk, [&]() -> bool {
        for (auto it = lock_table_[rid].list.begin();
                it != lock_table_[rid].list.end(); ++it) {
            if (it == lock_table_[rid].list.begin() && it->txn_id != txn->GetTransactionId()) {
                return false;
            }
            if (it != lock_table_[rid].list.begin() && it->granted) {
                return false;
            }
        }
        return true;
    });

    auto cur = lock_table_[rid].list.begin();
    cur->lock_mode = LockMode::EXCLUSIVE;
    lock_table_[rid].exclusive_cnt++;
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
    std::unique_lock<std::mutex> latch(mutex_);
    assert(txn->GetSharedLockSet()->count(rid) || txn->GetExclusiveLockSet()->count(rid));

    if (strict_2PL_) {
        if (txn->GetState() != TransactionState::ABORTED ||
                txn->GetState() != TransactionState::COMMITTED) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
    }
    if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
    }

    for (auto it = lock_table_[rid].list.begin();
            it != lock_table_[rid].list.end(); ++it) {
        if (it->txn_id == txn->GetTransactionId()) {
            if (it->lock_mode == LockMode::SHARED) {
                txn->GetSharedLockSet()->erase(rid);
            } else {
                txn->GetExclusiveLockSet()->erase(rid);
                lock_table_[rid].exclusive_cnt--;
            }
            lock_table_[rid].list.erase(it);
            break;
        }
    }
    // ����oldest
    for (auto it = lock_table_[rid].list.begin();
            it != lock_table_[rid].list.end(); ++it) {
        if (it->txn_id < lock_table_[rid].oldest) {
            lock_table_[rid].oldest = it->txn_id;
        }
    }
    cv_.notify_all();
    return true;
}

} // namespace cmudb

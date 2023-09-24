// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#include "dtx.h"

DTX::DTX(DTXContext *context) : context(context), tx_id(0), addr_cache(nullptr) {
    addr_cache = context->GetAddrCache();
    t_id = GetThreadID();
}

bool DTX::ExeRO() {
    std::vector<DirectRead> pending_direct_ro;
    std::vector<HashRead> pending_hash_ro;
    IssueReadOnly(pending_direct_ro, pending_hash_ro);
    context->Sync();
    std::list<InvisibleRead> pending_invisible_ro;
    std::list<HashRead> pending_next_hash_ro;
    if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro, pending_next_hash_ro))
        return false;
    if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro))
        return false;
    while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty()) {
        context->Sync();
        if (!CheckInvisibleRO(pending_invisible_ro))
            return false;
        if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
            return false;
    }
    return true;
}

bool DTX::ExeRW() {
    is_ro_tx = false;
    std::vector<DirectRead> pending_direct_ro;
    std::vector<HashRead> pending_hash_ro;
    std::vector<CasRead> pending_cas_rw;
    std::vector<HashRead> pending_hash_rw;
    std::vector<InsertOffRead> pending_insert_off_rw;
    std::list<InvisibleRead> pending_invisible_ro;
    std::list<HashRead> pending_next_hash_ro;
    std::list<HashRead> pending_next_hash_rw;
    std::list<InsertOffRead> pending_next_off_rw;
    IssueReadOnly(pending_direct_ro, pending_hash_ro);
    IssueReadLock(pending_cas_rw, pending_hash_rw, pending_insert_off_rw);
    context->Sync();
    if (!CheckDirectRO(pending_direct_ro, pending_invisible_ro, pending_next_hash_ro))
        return false;
    if (!CheckHashRO(pending_hash_ro, pending_invisible_ro, pending_next_hash_ro))
        return false;
    if (!CheckHashRW(pending_hash_rw, pending_invisible_ro, pending_next_hash_rw))
        return false;
    if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro, pending_next_off_rw))
        return false;
    if (!CheckCasRW(pending_cas_rw, pending_next_hash_rw, pending_next_off_rw))
        return false;
    while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty() || !pending_next_hash_rw.empty() ||
           !pending_next_off_rw.empty()) {
        context->Sync();
        if (!CheckInvisibleRO(pending_invisible_ro))
            return false;
        if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
            return false;
        if (!CheckNextHashRW(pending_invisible_ro, pending_next_hash_rw))
            return false;
        if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw))
            return false;
    }
    ParallelUndoLog();
    return true;
}

bool DTX::Validate() {
    if (not_eager_locked_rw_set.empty() && read_only_set.empty())
        return true;
    std::vector<ValidateRead> pending_validate;
    IssueValidate(pending_validate);
    context->Sync();
    for (auto &re: pending_validate) {
        auto it = re.item->item_ptr;
        if (re.has_lock_in_validate) {
            if (*((lock_t *) re.cas_buf) != STATE_CLEAN) {
                return false;
            }
            version_t my_version = it->version;
            if (it->user_insert) {
                for (auto &old_version: old_version_for_insert) {
                    if (old_version.table_id == it->table_id && old_version.key == it->key) {
                        my_version = old_version.version;
                        break;
                    }
                }
            }
            if (my_version != *((version_t *) re.version_buf)) {
                return false;
            }
        } else {
            if (it->version != *((version_t *) re.version_buf)) {
                return false;
            }
        }
    }
    return true;
}

bool DTX::CoalescentCommit() {
    char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
    *(lock_t *) cas_buf = STATE_LOCKED | STATE_INVISIBLE;
    std::vector<CommitWrite> pending_commit_write;
    context->Sync();
    IssueCommitAllSelectFlush(pending_commit_write, cas_buf);
    context->Sync();
    *((lock_t *) cas_buf) = 0;
    for (auto &re: pending_commit_write) {
        context->Write(cas_buf, GlobalAddress(re.node_id, re.lock_off), sizeof(lock_t));
        context->PostRequest();
    }
    return true;
}

void DTX::ParallelUndoLog() {
    size_t log_size = sizeof(tx_id) + sizeof(t_id);
    for (auto &set_it: read_write_set) {
        if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
            log_size += DataItemSize;
            set_it.is_logged = true;
        }
    }
    char *written_log_buf = AllocLocalBuffer(log_size);
    offset_t cur = 0;
    std::memcpy(written_log_buf + cur, &tx_id, sizeof(tx_id));
    cur += sizeof(tx_id);
    std::memcpy(written_log_buf + cur, &t_id, sizeof(t_id));
    cur += sizeof(t_id);
    for (auto &set_it: read_write_set) {
        if (!set_it.is_logged && !set_it.item_ptr->user_insert) {
            std::memcpy(written_log_buf + cur, set_it.item_ptr.get(), DataItemSize);
            cur += DataItemSize;
            set_it.is_logged = true;
        }
    }

    for (int i = 0; i < context->GetRemoteNodes(); i++) {
        offset_t log_offset = GetNextLogOffset(i, log_size);
        context->Write(written_log_buf, GlobalAddress(i, log_offset), log_size);
        context->PostRequest();
    }
}

void DTX::Abort() {
    char *unlock_buf = AllocLocalBuffer(sizeof(lock_t));
    *((lock_t *) unlock_buf) = 0;
    for (auto &index: locked_rw_set) {
        auto &it = read_write_set[index].item_ptr;
        node_id_t primary_node_id = GetPrimaryNodeID(it->table_id);
        context->Write(unlock_buf,
                       GlobalAddress(primary_node_id, it->GetRemoteLockAddr()),
                       sizeof(lock_t));
        context->PostRequest();
    }
    context->RetryTask();
    context->EndTask();
}

bool DTX::IssueReadOnly(std::vector<DirectRead> &pending_direct_ro, std::vector<HashRead> &pending_hash_ro) {
    for (auto &item: read_only_set) {
        if (item.is_fetched)
            continue;
        auto it = item.item_ptr;
        node_id_t node_id = GetPrimaryNodeID(it->table_id);
        item.read_which_node = node_id;
        auto offset = addr_cache->Search(node_id, it->table_id, it->key);
        if (offset != NOT_FOUND) {
            it->remote_offset = offset;
            char *buf = AllocLocalBuffer(DataItemSize);
            pending_direct_ro.emplace_back(DirectRead{.node_id = node_id, .item = &item, .buf = buf});
            context->read(buf, GlobalAddress(node_id, offset), DataItemSize);
            context->PostRequest();
        } else {
            HashMeta meta = GetPrimaryHashMetaWithTableID(it->table_id);
            uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
            offset_t node_off = idx * meta.node_size + meta.base_off;
            char *buf = AllocLocalBuffer(sizeof(HashNode));
            pending_hash_ro.emplace_back(HashRead{.node_id = node_id, .item = &item, .buf = buf, .meta = meta});
            context->read(buf, GlobalAddress(node_id, node_off), sizeof(HashNode));
            context->PostRequest();
        }
    }
    return true;
}

bool DTX::IssueReadLock(std::vector<CasRead> &pending_cas_rw, std::vector<HashRead> &pending_hash_rw,
                        std::vector<InsertOffRead> &pending_insert_off_rw) {
    for (size_t i = 0; i < read_write_set.size(); i++) {
        if (read_write_set[i].is_fetched)
            continue;
        auto it = read_write_set[i].item_ptr;
        auto node_id = GetPrimaryNodeID(it->table_id);
        read_write_set[i].read_which_node = node_id;
        auto offset = addr_cache->Search(node_id, it->table_id, it->key);
        if (offset != NOT_FOUND) {
            it->remote_offset = offset;
            locked_rw_set.emplace_back(i);
            char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
            char *data_buf = AllocLocalBuffer(DataItemSize);
            pending_cas_rw.emplace_back(
                    CasRead{.node_id = node_id, .item = &read_write_set[i], .cas_buf = cas_buf, .data_buf = data_buf});
            context->CompareAndSwap(cas_buf,
                                    GlobalAddress(node_id, it->GetRemoteLockAddr(offset)),
                                    STATE_CLEAN, STATE_LOCKED);
            context->read(data_buf, GlobalAddress(node_id, offset), DataItemSize);
            context->PostRequest();
        } else {
            not_eager_locked_rw_set.emplace_back(i);
            const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
            uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
            offset_t node_off = idx * meta.node_size + meta.base_off;
            char *local_hash_node = AllocLocalBuffer(sizeof(HashNode));
            if (it->user_insert) {
                pending_insert_off_rw.emplace_back(
                        InsertOffRead{.node_id = node_id, .item = &read_write_set[i],
                                .buf = local_hash_node, .meta = meta, .node_off = node_off});
            } else {
                pending_hash_rw.emplace_back(
                        HashRead{.node_id = node_id, .item = &read_write_set[i], .buf = local_hash_node, .meta = meta});
            }
            context->read(local_hash_node, GlobalAddress(node_id, node_off), sizeof(HashNode));
            context->PostRequest();
        }
    }
    return true;
}

bool DTX::IssueValidate(std::vector<ValidateRead> &pending_validate) {
    for (auto &index: not_eager_locked_rw_set) {
        locked_rw_set.emplace_back(index);
        char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
        *(lock_t *) cas_buf = 0xdeadbeaf;
        char *version_buf = AllocLocalBuffer(sizeof(version_t));
        auto &it = read_write_set[index].item_ptr;
        node_id_t node_id = read_write_set[index].read_which_node;
        pending_validate.push_back(
                ValidateRead{.node_id = node_id, .item = &read_write_set[index], .cas_buf = cas_buf,
                        .version_buf = version_buf, .has_lock_in_validate = true});
        context->CompareAndSwap(cas_buf, GlobalAddress(node_id, it->GetRemoteLockAddr()),
                                STATE_CLEAN, STATE_LOCKED);
        context->read(version_buf, GlobalAddress(node_id, it->GetRemoteVersionAddr()), sizeof(version_t));
        context->PostRequest();
    }
    for (auto &set_it: read_only_set) {
        auto it = set_it.item_ptr;
        node_id_t node_id = set_it.read_which_node;
        char *version_buf = AllocLocalBuffer(sizeof(version_t));
        pending_validate.push_back(
                ValidateRead{.node_id = node_id, .item = &set_it, .cas_buf = nullptr,
                        .version_buf = version_buf, .has_lock_in_validate = false});
        context->read(version_buf, GlobalAddress(node_id, it->GetRemoteVersionAddr()),
                      sizeof(version_t));
        context->PostRequest();
    }
    return true;
}

bool DTX::IssueCommitAllSelectFlush(std::vector<CommitWrite> &pending_commit_write, char *cas_buf) {
    size_t current_i = 0;
    for (auto &set_it: read_write_set) {
        char *data_buf = AllocLocalBuffer(DataItemSize);
        auto it = set_it.item_ptr;
        if (!it->user_insert) {
            it->version++;
        }

        it->lock = STATE_LOCKED | STATE_INVISIBLE;
        memcpy(data_buf, (char *) it.get(), DataItemSize);
        node_id_t node_id = GetPrimaryNodeID(it->table_id);
        pending_commit_write.push_back(CommitWrite{.node_id = node_id, .lock_off = it->GetRemoteLockAddr()});

        context->Write(cas_buf, GlobalAddress(node_id, it->GetRemoteLockAddr()), sizeof(lock_t));
        context->Write(data_buf, GlobalAddress(node_id, it->remote_offset), DataItemSize);
        context->PostRequest();

        const HashMeta &primary_hash_meta = GetPrimaryHashMetaWithTableID(it->table_id);
        auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;
        auto *backup_node_ids = GetBackupNodeID(it->table_id);
        if (!backup_node_ids)
            continue;

        const std::vector<HashMeta> *backup_hash_metas = GetBackupHashMetasWithTableID(it->table_id);
        for (size_t i = 0; i < backup_node_ids->size(); i++) {
            auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
            auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);
            node_id_t backup_node_id = backup_node_ids->at(i);
            pending_commit_write.push_back(CommitWrite{.node_id = backup_node_id, .lock_off = remote_lock_off});
            char *data_buf = AllocLocalBuffer(DataItemSize);
            it->lock = STATE_INVISIBLE;
            it->remote_offset = remote_item_off;
            memcpy(data_buf, (char *) it.get(), DataItemSize);
            context->Write(cas_buf, GlobalAddress(backup_node_id, remote_lock_off), sizeof(lock_t));
            context->Write(data_buf, GlobalAddress(backup_node_id, remote_item_off), DataItemSize);
            if (current_i == read_write_set.size() - 1) {
                char *flush_buf = AllocLocalBuffer(RFlushReadSize);
                context->read(flush_buf, GlobalAddress(backup_node_id, it->remote_offset), RFlushReadSize);
            }
            context->PostRequest();
        }
        current_i++;
    }
    return true;
}

bool DTX::CheckDirectRO(std::vector<DirectRead> &pending_direct_ro,
                        std::list<InvisibleRead> &pending_invisible_ro,
                        std::list<HashRead> &pending_next_hash_ro) {
    for (auto &res: pending_direct_ro) {
        auto *it = res.item->item_ptr.get();
        auto *fetched_item = (DataItem *) res.buf;
        if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
            if (likely(fetched_item->valid)) {
                *it = *fetched_item;
                res.item->is_fetched = true;
                if (unlikely((it->lock & STATE_INVISIBLE))) {
                    char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
                    uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
                    pending_invisible_ro.emplace_back(
                            InvisibleRead{.node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
                    context->read(cas_buf, GlobalAddress(res.node_id, lock_offset), sizeof(lock_t));
                }
            } else {
                addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
                return false;
            }
        } else {
            node_id_t remote_node_id = GetPrimaryNodeID(it->table_id);
            const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
            uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
            offset_t node_off = idx * meta.node_size + meta.base_off;
            auto *local_hash_node = (HashNode *) AllocLocalBuffer(sizeof(HashNode));
            pending_next_hash_ro.emplace_back(
                    HashRead{.node_id = remote_node_id, .item = res.item, .buf = (char *) local_hash_node, .meta = meta});
            context->read((char *) local_hash_node, GlobalAddress(remote_node_id, node_off), sizeof(HashNode));
        }
    }
    return true;
}

bool DTX::CheckHashRO(std::vector<HashRead> &pending_hash_ro,
                      std::list<InvisibleRead> &pending_invisible_ro,
                      std::list<HashRead> &pending_next_hash_ro) {
    for (auto &res: pending_hash_ro) {
        auto *local_hash_node = (HashNode *) res.buf;
        auto *it = res.item->item_ptr.get();
        bool find = false;

        for (auto &item: local_hash_node->data_items) {
            if (item.valid && item.key == it->key && item.table_id == it->table_id) {
                *it = item;
                addr_cache->Insert(res.node_id, it->table_id, it->key, it->remote_offset);
                res.item->is_fetched = true;
                find = true;
                break;
            }
        }

        if (likely(find)) {
            if (unlikely((it->lock & STATE_INVISIBLE))) {
                char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
                uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
                pending_invisible_ro.emplace_back(
                        InvisibleRead{.node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
                context->read(cas_buf, GlobalAddress(res.node_id, lock_offset), sizeof(lock_t));
            }
        } else {
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            pending_next_hash_ro.emplace_back(
                    HashRead{.node_id = res.node_id, .item = res.item, .buf = res.buf, .meta = res.meta});
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
        }
    }
    return true;
}

bool DTX::CheckInvisibleRO(std::list<InvisibleRead> &pending_invisible_ro) {
    for (auto iter = pending_invisible_ro.begin(); iter != pending_invisible_ro.end();) {
        auto res = *iter;
        auto lock_value = *((lock_t *) res.buf);
        if (lock_value & STATE_INVISIBLE) {
            context->read(res.buf, GlobalAddress(res.node_id, res.off), sizeof(lock_t));
            iter++;
        } else {
            iter = pending_invisible_ro.erase(iter);
        }
    }
    return true;
}

bool DTX::CheckNextHashRO(std::list<InvisibleRead> &pending_invisible_ro, std::list<HashRead> &pending_next_hash_ro) {
    for (auto iter = pending_next_hash_ro.begin(); iter != pending_next_hash_ro.end();) {
        auto res = *iter;
        auto *local_hash_node = (HashNode *) res.buf;
        auto *it = res.item->item_ptr.get();
        bool find = false;

        for (auto &item: local_hash_node->data_items) {
            if (item.valid && item.key == it->key && item.table_id == it->table_id) {
                *it = item;
                addr_cache->Insert(res.node_id, it->table_id, it->key, it->remote_offset);
                res.item->is_fetched = true;
                find = true;
                break;
            }
        }

        if (likely(find)) {
            if (unlikely((it->lock & STATE_INVISIBLE))) {
                char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
                uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
                pending_invisible_ro.emplace_back(
                        InvisibleRead{.node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
                context->read(cas_buf, GlobalAddress(res.node_id, lock_offset), sizeof(lock_t));
            }
            iter = pending_next_hash_ro.erase(iter);
        } else {
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
            iter++;
        }
    }
    return true;
}

bool DTX::CheckCasRW(std::vector<CasRead> &pending_cas_rw, std::list<HashRead> &pending_next_hash_rw,
                     std::list<InsertOffRead> &pending_next_off_rw) {
    for (auto &re: pending_cas_rw) {
        if (*((lock_t *) re.cas_buf) != STATE_CLEAN) {
            return false;
        }
        auto it = re.item->item_ptr;
        auto *fetched_item = (DataItem *) (re.data_buf);
        if (likely(fetched_item->key == it->key && fetched_item->table_id == it->table_id)) {
            if (it->user_insert) {
                if (it->version < fetched_item->version) return false;
                old_version_for_insert.push_back(
                        OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = fetched_item->version});
            } else {
                if (likely(fetched_item->valid)) {
                    assert(fetched_item->remote_offset == it->remote_offset);
                    *it = *fetched_item;
                } else {
                    addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
                    return false;
                }
            }
            re.item->is_fetched = true;
        } else {
            *((lock_t *) re.cas_buf) = STATE_CLEAN;
            context->Write(re.cas_buf, GlobalAddress(re.node_id, it->GetRemoteLockAddr()), sizeof(lock_t));
            const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
            uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
            offset_t node_off = idx * meta.node_size + meta.base_off;
            auto *local_hash_node = (HashNode *) AllocLocalBuffer(sizeof(HashNode));
            if (it->user_insert) {
                pending_next_off_rw.emplace_back(
                        InsertOffRead{.node_id = re.node_id, .item = re.item, .buf = (char *) local_hash_node, .meta = meta, .node_off = node_off});
            } else {
                pending_next_hash_rw.emplace_back(
                        HashRead{.node_id = re.node_id, .item = re.item, .buf = (char *) local_hash_node, .meta = meta});
            }
            context->read(local_hash_node, GlobalAddress(re.node_id, node_off), sizeof(HashNode));
            context->PostRequest();
        }
    }
    return true;
}

int DTX::FindMatchSlot(HashRead &res, std::list<InvisibleRead> &pending_invisible_ro) {
    auto *local_hash_node = (HashNode *) res.buf;
    auto *it = res.item->item_ptr.get();
    bool find = false;
    for (auto &item: local_hash_node->data_items) {
        if (item.valid && item.key == it->key && item.table_id == it->table_id) {
            *it = item;
            addr_cache->Insert(res.node_id, it->table_id, it->key, it->remote_offset);
            res.item->is_fetched = true;
            find = true;
            break;
        }
    }
    if (likely(find)) {
        if (unlikely((it->lock & STATE_INVISIBLE))) {
            char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
            uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
            pending_invisible_ro.emplace_back(
                    InvisibleRead{.node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
            context->read(cas_buf, GlobalAddress(res.node_id, lock_offset), sizeof(lock_t));
        }
        return SLOT_FOUND;
    }
    return SLOT_NOT_FOUND;
}

bool DTX::CheckHashRW(std::vector<HashRead> &pending_hash_rw,
                      std::list<InvisibleRead> &pending_invisible_ro,
                      std::list<HashRead> &pending_next_hash_rw) {
    for (auto &res: pending_hash_rw) {
        auto rc = FindMatchSlot(res, pending_invisible_ro);
        if (rc == SLOT_NOT_FOUND) {
            auto *local_hash_node = (HashNode *) res.buf;
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            pending_next_hash_rw.emplace_back(
                    HashRead{.node_id = res.node_id, .item = res.item, .buf = res.buf, .meta = res.meta});
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
        }
    }
    return true;
}

bool DTX::CheckNextHashRW(std::list<InvisibleRead> &pending_invisible_ro,
                          std::list<HashRead> &pending_next_hash_rw) {
    for (auto iter = pending_next_hash_rw.begin(); iter != pending_next_hash_rw.end();) {
        auto res = *iter;
        auto rc = FindMatchSlot(res, pending_invisible_ro);
        if (rc == SLOT_FOUND)
            iter = pending_next_hash_rw.erase(iter);
        else if (rc == SLOT_NOT_FOUND) {
            auto *local_hash_node = (HashNode *) res.buf;
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
            iter++;
        }
    }
    return true;
}

int DTX::FindInsertOff(InsertOffRead &res, std::list<InvisibleRead> &pending_invisible_ro) {
    offset_t possible_insert_position = OFFSET_NOT_FOUND;
    version_t old_version;
    auto *local_hash_node = (HashNode *) res.buf;
    auto it = res.item->item_ptr;
    for (int i = 0; i < ITEM_NUM_PER_NODE; i++) {
        auto &data_item = local_hash_node->data_items[i];
        if (possible_insert_position == OFFSET_NOT_FOUND && !data_item.valid && data_item.lock == STATE_CLEAN) {
            std::pair<node_id_t, offset_t> new_pos(res.node_id, res.node_off + i * DataItemSize);
            if (inserted_pos.find(new_pos) != inserted_pos.end()) {
                continue;
            } else {
                inserted_pos.insert(new_pos);
            }
            possible_insert_position = res.node_off + i * DataItemSize;
            old_version = data_item.version;
        } else if (data_item.valid && data_item.key == it->key && data_item.table_id == it->table_id) {
            if (it->version < data_item.version) {
                return VERSION_TOO_OLD;
            }
            possible_insert_position = res.node_off + i * DataItemSize;
            old_version = data_item.version;
            it->lock = data_item.lock;
            break;
        }
    }
    if (possible_insert_position != OFFSET_NOT_FOUND) {
        it->remote_offset = possible_insert_position;
        addr_cache->Insert(res.node_id, it->table_id, it->key, possible_insert_position);
        old_version_for_insert.push_back(
                OldVersionForInsert{.table_id = it->table_id, .key = it->key, .version = old_version});
        if (unlikely((it->lock & STATE_INVISIBLE))) {
            char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
            uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
            pending_invisible_ro.emplace_back(
                    InvisibleRead{.node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
            context->read(cas_buf, GlobalAddress(res.node_id, lock_offset), sizeof(lock_t));
        }
        res.item->is_fetched = true;
        return OFFSET_FOUND;
    }
    return OFFSET_NOT_FOUND;
}

bool DTX::CheckInsertOffRW(std::vector<InsertOffRead> &pending_insert_off_rw,
                           std::list<InvisibleRead> &pending_invisible_ro,
                           std::list<InsertOffRead> &pending_next_off_rw) {
    for (auto &res: pending_insert_off_rw) {
        auto rc = FindInsertOff(res, pending_invisible_ro);
        if (rc == VERSION_TOO_OLD)
            return false;
        else if (rc == OFFSET_NOT_FOUND) {
            auto *local_hash_node = (HashNode *) res.buf;
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            pending_next_off_rw.emplace_back(
                    InsertOffRead{.node_id = res.node_id, .item = res.item, .buf = res.buf, .meta = res.meta, .node_off = res.node_off});
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
        }
    }
    return true;
}

bool DTX::CheckNextOffRW(std::list<InvisibleRead> &pending_invisible_ro,
                         std::list<InsertOffRead> &pending_next_off_rw) {
    for (auto iter = pending_next_off_rw.begin(); iter != pending_next_off_rw.end();) {
        auto &res = *iter;
        auto rc = FindInsertOff(res, pending_invisible_ro);
        if (rc == VERSION_TOO_OLD)
            return false;
        else if (rc == OFFSET_FOUND)
            iter = pending_next_off_rw.erase(iter);
        else if (rc == OFFSET_NOT_FOUND) {
            auto *local_hash_node = (HashNode *) res.buf;
            if (local_hash_node->next == nullptr) return false;
            auto node_off = (uint64_t) local_hash_node->next - res.meta.data_ptr + res.meta.base_off;
            context->read(res.buf, GlobalAddress(res.node_id, node_off), sizeof(HashNode));
            iter++;
        }
    }
    return true;
}

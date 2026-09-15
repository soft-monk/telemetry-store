// src/backends/memory_backend.cc · 内存后端实现
#include "telemetry_store/backends/memory_backend.h"

#include <algorithm>
#include <tuple>

#include "query.h"

namespace telemetry_store {

MemoryBackend::MemoryBackend(std::size_t rowLimit) : rowLimit_(rowLimit) {}

bool MemoryBackend::append(RecordBatch batch) {
    std::lock_guard<std::mutex> lk(mtx_);
    for (const auto& r : batch) {
        auto it = byDevice_.find(r.deviceId);
        if (it == byDevice_.end()) {
            deviceOrder_.push_back(r.deviceId);
            it = byDevice_.emplace(r.deviceId, Bucket{}).first;
        }
        Bucket& b = it->second;
        // 追加序通常是时间序；一旦出现回拨就标记为乱序，查询时改走排序路径。
        if (!b.rows.empty() && r.ts < b.rows.back().ts) b.tsSorted = false;
        b.rows.push_back(r);
        ++rows_;
        bytes_ += r.deviceId.size() + r.type.size() + r.data.dump().size();
    }
    enforceLimitLocked();
    return true;  // 内存后端不拒收
}

std::size_t MemoryBackend::read(const Query& q, const Cursor* beginAfter,
                                const RecordVisitor& visit) {
    std::lock_guard<std::mutex> lk(mtx_);

    std::size_t matched = 0;
    auto handle = [&](const Record& r) -> bool {
        if (!q.acceptsType(r.type)) return true;
        if (!detail::inRange(r, q.fromTs, q.toTs)) return true;
        if (beginAfter != nullptr && !detail::cursorPasses(r, *beginAfter)) return true;
        ++matched;
        return visit(r);  // false = 调用方收够了，立即停
    };

    if (q.devices.empty()) {
        for (const auto& dev : deviceOrder_) {
            auto it = byDevice_.find(dev);
            if (it == byDevice_.end()) continue;
            if (!handleBucket(it->second, q, handle)) break;
        }
    } else {
        for (const auto& dev : q.devices) {
            auto it = byDevice_.find(dev);
            if (it == byDevice_.end()) continue;
            if (!handleBucket(it->second, q, handle)) break;
        }
    }
    return matched;
}

bool MemoryBackend::handleBucket(const Bucket& b, const Query& q,
                                 const std::function<bool(const Record&)>& handle) {
    (void)q;
    if (!b.tsSorted) {
        // 乱序桶：拷一份排序后遍历（常见情形不会走到这里）。
        std::vector<Record> tmp(b.rows.begin(), b.rows.end());
        detail::sortByKey(tmp);
        for (const auto& r : tmp) {
            if (!handle(r)) return false;
        }
        return true;
    }
    // 有序桶：二分跳到区间起点，避免扫全桶。
    auto begin = std::lower_bound(b.rows.begin(), b.rows.end(), q.fromTs,
                                  [](const Record& r, Millis ts) { return r.ts < ts; });
    for (auto it = begin; it != b.rows.end(); ++it) {
        if (it->ts >= q.toTs) break;  // 左闭右开
        if (!handle(*it)) return false;
    }
    return true;
}

bool MemoryBackend::purge(Millis cutoffTs) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (cutoffTs <= 0) return true;
    for (auto& kv : byDevice_) {
        auto& rows = kv.second.rows;
        std::size_t before = rows.size();
        rows.erase(std::remove_if(rows.begin(), rows.end(),
                                  [cutoffTs](const Record& r) { return r.ts < cutoffTs; }),
                   rows.end());
        std::size_t removed = before - rows.size();
        if (removed > 0) {
            rows_ -= removed;
            kv.second.tsSorted = true;  // 删除保序
        }
    }
    return true;
}

bool MemoryBackend::trimToRows(std::size_t maxRows) {
    std::lock_guard<std::mutex> lk(mtx_);
    rowLimit_ = maxRows;
    enforceLimitLocked();
    return true;
}

void MemoryBackend::enforceLimitLocked() {
    if (rowLimit_ == 0 || rows_ <= rowLimit_) return;

    // 全局最旧优先：跨设备按 (ts, deviceId) 找最旧的一条删掉。
    // 设备数不多时这是最简单的正确做法；极端规模请换段文件后端。
    while (rows_ > rowLimit_) {
        bool removed = false;
        for (auto& kv : byDevice_) {
            auto& rows = kv.second.rows;
            if (rows.empty()) continue;
            auto oldest = std::min_element(
                rows.begin(), rows.end(), [](const Record& a, const Record& b) {
                    return std::tie(a.ts, a.deviceId) < std::tie(b.ts, b.deviceId);
                });
            if (oldest != rows.end()) {
                bytes_ -= oldest->deviceId.size() + oldest->type.size() + oldest->data.dump().size();
                rows.erase(oldest);
                --rows_;
                removed = true;
                break;  // 重新扫描，保证"全局最旧"的判定始终成立
            }
        }
        if (!removed) break;  // 空库兜底，防死循环
    }
}

Millis MemoryBackend::earliestRetainedTs() const {
    std::lock_guard<std::mutex> lk(mtx_);
    Millis best = 0;
    for (const auto& kv : byDevice_) {
        for (const auto& r : kv.second.rows) {
            if (best == 0 || r.ts < best) best = r.ts;
        }
    }
    return best;
}

std::size_t MemoryBackend::approxRows() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return rows_;
}

std::size_t MemoryBackend::approxBytes() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return bytes_;
}

void MemoryBackend::clear() {
    std::lock_guard<std::mutex> lk(mtx_);
    byDevice_.clear();
    deviceOrder_.clear();
    rows_ = 0;
    bytes_ = 0;
}

}  // namespace telemetry_store

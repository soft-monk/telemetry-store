// telemetry-store · backends/memory_backend.h · 默认内存后端（L1：零依赖，clone 即跑）
//
// 设计依据：docs/设计/遥测留存概要设计.md §2.2（物理形态由后端决定）· §7 R2
// 需求：TLM-NFR-07（不引外部服务）· TLM-RET-04/05
//
// 定位：让模块**独立可跑**。生产环境请换成段文件后端或宿主自己的实现。
// 物理形态：按设备分桶的顺序表（追加序）+ 每桶内的 ts 单调序，配套二分查找。
//
// 它是**公开面**的一部分：只有公开了，"空环境 clone 下来就能跑出效果"才成立。
#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "telemetry_store/backend.h"

namespace telemetry_store {

/// 纯内存后端。线程安全（backend.h 约定 ①）。
///
/// ⚠️ 它**不持久**：进程退出即失去全部数据——所以它只适合示例、测试与"再丢一次也认"的用法。
/// 要跨重启留存用 `SegmentFileBackend`。
class MemoryBackend : public IStorageBackend {
public:
    /// 构造期即设定最旧优先淘汰上限（0 = 不限），用于 TLM-RET-05 的容量裁剪。
    explicit MemoryBackend(std::size_t rowLimit = 0);

    bool append(RecordBatch batch) override;
    std::size_t read(const Query& q, const Cursor* beginAfter,
                     const RecordVisitor& visit) override;
    bool purge(Millis cutoffTs) override;
    bool canPurge() const override { return true; }
    Millis earliestRetainedTs() const override;
    std::size_t approxRows() const override;
    std::size_t approxBytes() const override;
    const char* name() const override { return "memory"; }
    bool trimToRows(std::size_t maxRows) override;

    /// 便于测试/示例观察：清空全部数据（不重置计数）。
    void clear();

private:
    /// 每个设备一份顺序表。`ts` 允许乱序（设备时钟可能回拨），查询时统一排序，
    /// 但**常见情形（追加序 = 时间序）下不做排序**，靠这个标志走快路径。
    struct Bucket {
        std::deque<Record> rows;
        bool               tsSorted = true;  ///< false 时查询需排序 / 退化为全桶扫描
    };

    void enforceLimitLocked();

    /// 遍历一个设备桶：有序桶二分跳转，乱序桶先排序。
    /// 返回 false = 访问者喊停（收够了），外层应立刻停止整体遍历。
    bool handleBucket(const Bucket& b, const Query& q,
                      const std::function<bool(const Record&)>& handle);

    mutable std::mutex mtx_;
    std::unordered_map<std::string, Bucket> byDevice_;
    std::vector<std::string>                deviceOrder_;  ///< 无序集合的稳定遍历序
    std::size_t                             rows_ = 0;
    std::size_t                             bytes_ = 0;
    std::size_t                             rowLimit_ = 0;
};

}  // namespace telemetry_store

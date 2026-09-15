// telemetry-store · backend.h  存储后端（反向接口：存哪里由宿主说了算）
//
// 设计依据：docs/设计/遥测留存概要设计.md §3.5 · §4.2 · §4.3 · §6.3
// 需求：TLM-ADP-01（零 SQL / 零连接串 / 零表名）· TLM-QRY-04 · TLM-RET-04/05
//
// ⚠️ 实现本接口必读的三条约定：
//   ① **必须线程安全**：模块会从写入线程与保留维护线程同时调用（§5 约定 2）。
//   ② **只做追加，不要求 upsert**：同一 (deviceId, ts, type) 允许存多份，
//      去重（后写胜）由模块在**读路径**完成（D7）。这样任何后端都能满足，
//      不必被迫提供唯一索引或更新能力。
//   ③ **能力要诚实**：不会做的事（如删除）就 `canPurge()` 返回 false，
//      模块会据此降级并如实记账，**绝不假装成功**（TLM-RET-04 / P4）。
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "telemetry_store/record.h"

namespace telemetry_store {

/// 一批记录（只读视图）。
///
/// 为什么不用 `std::span`：本模块口径是 **C++17**（TLM-NFR-07 零依赖、广兼容），
/// `std::span` 是 C++20 才有的。自己这个三行视图同样**零拷贝、零分配**，
/// 且让 C++17 的宿主也能直接调用。
struct RecordBatch {
    const Record* data = nullptr;
    std::size_t   size = 0;

    RecordBatch() = default;
    RecordBatch(const Record* d, std::size_t n) : data(d), size(n) {}
    /// 从任意连续容器构造（vector / array / 数组）。
    template <typename Alloc>
    RecordBatch(const std::vector<Record, Alloc>& v) : data(v.data()), size(v.size()) {}

    const Record* begin() const { return data; }
    const Record* end() const { return data + size; }
    bool         empty() const { return size == 0; }
    const Record& operator[](std::size_t i) const { return data[i]; }
};

/// 逐条回吐候选记录。返回 false = 调用方已收够，实现**应尽快停止**遍历。
/// 用回调而不是返回 vector：区间很大时由模块控制内存与截断，避免后端一次性撑爆。
using RecordVisitor = std::function<bool(const Record&)>;

class IStorageBackend {
public:
    virtual ~IStorageBackend() = default;

    // ─────────────────────────────────────────────── 写
    /// 成批追加（TLM-WRT-03 的落点：模块把 1000 条/s 折成约 1 次/s 调用）。
    /// 返回 false = 本批**未落盘**，调用方会保留数据并在下一轮重试。
    /// 允许重复写入同一记录（去重在读路径）。
    virtual bool append(RecordBatch batch) = 0;

    // ─────────────────────────────────────────────── 读
    /// 区间读：`[q.fromTs, q.toTs)`，`q.devices` / `q.types` 为空表示不过滤。
    /// `beginAfter` 非空时只回吐**严格大于**该游标的记录（续查，D8）。
    /// 实现 SHOULD 按 ts 升序回吐；模块会自行排序与去重，**不依赖**此保证。
    /// 返回：本区间内符合条件的**记录条数**（未去重；不受遍历提前停止影响）。
    /// ⚠️ `count` 与 `read` 的筛选口径必须一致——否则 `hasMore` 会误报。
    virtual std::size_t read(const Query& q, const Cursor* beginAfter,
                             const RecordVisitor& visit) = 0;

    // ─────────────────────────────────────────────── 清理与容量
    /// 删除 `ts < cutoffTs` 的记录。返回 false = 失败（模块计 purgeFailures 并下轮重试）。
    virtual bool purge(Millis cutoffTs) = 0;

    /// 是否支持删除。false → 保留策略降级为"只标记、不删除"，且不重复尝试。
    virtual bool canPurge() const = 0;

    // ─────────────────────────────────────────────── 水位与规模
    /// 仍保留的数据里**最早**的 ts；空库返回 0。
    /// 用于判定 truncatedByRetention（TLM-QRY-05），以及清理后回填状态。
    virtual Millis earliestRetainedTs() const = 0;

    /// 当前记录条数（**估算即可**，用于容量策略 TLM-RET-05）。
    virtual std::size_t approxRows() const = 0;

    /// 当前占用字节数（估算即可，用于 maxBytes 策略；不支持时返回 0）。
    virtual std::size_t approxBytes() const { return 0; }

    /// 容量裁剪：把总条数压到 `maxRows` 以内（最旧优先，TLM-RET-05）。
    /// 返回 false = 本后端不支持容量裁剪（模块计入 unsupported 并如实反映，不假装成功）。
    /// 默认返回 false —— 只实现时间保留的后端不必关心它。
    virtual bool trimToRows(std::size_t /*maxRows*/) { return false; }

    /// 诊断用短名，如 "memory" / "segfile"。会出现在 Status::backend 里。
    virtual const char* name() const = 0;
};

}  // namespace telemetry_store

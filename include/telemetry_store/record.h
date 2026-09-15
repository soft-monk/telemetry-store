// telemetry-store · record.h  数据结构（对外）
//
// 设计依据：docs/设计/遥测留存概要设计.md §2 / §3.3 / §3.4
// 需求：TLM-WRT-04/05/08 · TLM-QRY-01..05 · TLM-RPL-01..04 · TLM-GAP-01/04 · TLM-OBS-01
//
// 两条贯穿本文件的口径：
//   ① 区间一律 **[fromTs, toTs) 左闭右开**，结果一律 **ts 升序**（D5）。
//   ② 数据的**时间由调用方给**（Record::ts），模块不自己打时间（D3）。
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace telemetry_store {

using Millis = std::int64_t;

/// 一行 = 一个时刻上的一次事件。
///
/// 逻辑主键 `(deviceId, ts, type)`；`seq` 是**诊断信息，不是主键**——
/// 它随进程重启归零，只用来回答"这批数据有没有缺口"（P3）。
struct Record {
    std::string    deviceId;   ///< 设备标识（主键前半）
    std::string    type;       ///< 事件名，形如 uav.pos / link.quality（信封 type 原样）
    Millis         ts = 0;     ///< 事件**产生**时刻，epoch 毫秒（主键后半）
    nlohmann::json data = nlohmann::json::object();  ///< 负载，原样透传：不看、不改、不索引
    std::uint64_t  seq = 0;    ///< 进程内单调递增序号（0 = 由模块在 append 时补齐）

    /// 主键三元组比较：先 ts，再 deviceId，再 type（决定查询结果的全序）。
    static bool lessByKey(const Record& a, const Record& b);
    bool sameKeyAs(const Record& other) const;

    nlohmann::json toJson() const;
    static Record fromJson(const nlohmann::json& j);
};

/// 续查游标 = 上一次结果末尾的位置（D8：`(ts, deviceId, type)` 三元组，全序续查）。
/// `valid == false` 表示"从头开始"。
struct Cursor {
    Millis      ts       = 0;
    std::string deviceId;
    std::string type;
    bool        valid    = false;

    static Cursor after(const Record& r);
    nlohmann::json toJson() const;
    static Cursor fromJson(const nlohmann::json& j);
};

/// 查询条件。devices / types 为空表示**不过滤**。
struct Query {
    std::vector<std::string> devices;              ///< 空 = 全部设备
    std::vector<std::string> types;                ///< 空 = 全部类型
    Millis      fromTs = 0;                        ///< 含
    Millis      toTs   = 0;                        ///< 不含
    std::size_t limit  = 0;                        ///< 0 = 用 StoreOptions::maxRowsPerQuery

    bool acceptsDevice(const std::string& id) const;
    bool acceptsType(const std::string& t) const;
};

/// 查询结果 + **覆盖元信息**。
///
/// `partial` / `truncatedByRetention` 的存在就是为了 P4（越界要明说）：
/// 使用者必须能区分"这段时间没有数据"和"我没查到全部数据"。
struct QueryResult {
    std::vector<Record> rows;                      ///< ts 升序；同 ts 按 deviceId/type 稳定排序
    bool                hasMore = false;           ///< true 时用 cursor 续查（不静默截断）
    Cursor              cursor;                    ///< hasMore 时有效
    Millis              coveredFromTs = 0;         ///< 实际返回的最早 ts（无数据 = 0）
    Millis              coveredToTs   = 0;         ///< 实际返回的最晚 ts（无数据 = 0）
    bool                truncatedByRetention = false;  ///< 查询区间早于保留边界，早期数据已被清理
    bool                partial = false;               ///< 结果可能不完整（缓冲未落盘 / 序号缺口 / 满页截断）
    bool                rejected = false;              ///< 请求被**拒绝执行**（如 limit 超过上限）
    std::string         rejectReason;                  ///< rejected 时的可读原因
    std::size_t         scanned = 0;               ///< 后端实际吐出的候选条数（诊断用）

    bool empty() const { return rows.empty(); }
    nlohmann::json toJson() const;
};

/// 回放 / 补发的共同输出：**一条有序时间轴**（D12：不含 speed，播放节奏是出口模块的自由）。
struct Timeline {
    Millis              fromTs = 0, toTs = 0;
    Millis              durationMs = 0;
    std::vector<Record> samples;                   ///< 多设备时是**归并后的一条**时间轴
    bool                sampled = false;           ///< 是否做过稀疏化（TLM-RPL-04）
    std::size_t         sampleStep = 1;            ///< everyNth 的实际步长
    bool                hasMore = false;
    Cursor              cursor;
    bool                truncatedByRetention = false;
    bool                partial = false;

    nlohmann::json toJson() const;
};

/// 回放请求。`deviceId` 与 `q.devices` 二选一（都为空 = 全部设备）。
struct ReplayRequest {
    Query       q;
    std::string deviceId;
    std::size_t everyNth      = 1;   ///< 每第 N 条
    Millis      minIntervalMs = 0;   ///< 最小间隔；与 everyNth 同时给出时取**更稀疏**者
};

/// 补发请求：客户端重连后问"我断的那段时间发生了什么"（TLM-GAP-01/02）。
///
/// 两种用法：
///   · 单一水位：`lastSeenTs` + `devices`
///   · 每设备各自水位：`perDevice`（非空时优先，忽略 lastSeenTs/devices）
/// 一律返回 **ts > 水位** 的记录（严格大于，避免与客户端已有数据重复）。
struct SinceRequest {
    std::vector<std::string>              devices;     ///< 空 + perDevice 空 = 全部设备
    Millis                                lastSeenTs = 0;
    std::vector<std::pair<std::string, Millis>> perDevice;
    std::vector<std::string>              types;       ///< 空 = 全部类型
    std::size_t                           limit = 0;   ///< 0 = 用 StoreOptions::maxRowsPerQuery
};

/// 一段**未被覆盖**的时间区间（缺口识别的输出，TLM-GAP-04）。
///
/// 判定口径是**阈值法**（D9）：相邻两条记录间隔 > maxGapMs 即视为一段缺口。
/// 真正的失联语义属设备健康域，本模块不重复实现。
struct Gap {
    std::string deviceId;
    Millis      fromTs = 0;      ///< 缺口起点：前一条记录之后（含）
    Millis      toTs   = 0;      ///< 缺口终点：后一条记录之前（不含）
    Millis      durationMs = 0;
    std::uint64_t seqBefore = 0; ///< 缺口前一条的序号（0 = 无前一条）
    std::uint64_t seqAfter  = 0; ///< 缺口后一条的序号（0 = 无后一条）
};

/// 保留清理的执行结果（TLM-RET-03）。
struct RetentionReport {
    bool          supported  = true;   ///< false = 后端不支持删除，保留降级为"只标记"
    std::uint64_t purgedRows = 0;      ///< 本次**时间清理**删除的条数（后端估算）
    std::uint64_t capacityEvictedRows = 0;  ///< 本次**容量裁剪**淘汰的条数（TLM-RET-05 的"淘汰要计数"）
    Millis        cutoffTs   = 0;      ///< 本次使用的清理截止点（删除 ts < cutoffTs）
    Millis        nowTs      = 0;
    bool          capacityTrimmed = false;  ///< 本次是否因容量上限触发
    std::string   backend;
    nlohmann::json toJson() const;
};

/// 可观测快照（TLM-OBS-01/02）。模块内所有计数都在这里，**不静默**（P2）。
struct Status {
    std::uint64_t appended = 0;              ///< 收到并接管的记录条数
    std::uint64_t persisted = 0;             ///< 已成功交付后端的条数
    std::uint64_t buffered = 0;              ///< 当前未刷盘缓冲深度
    std::uint64_t droppedByOverflow = 0;     ///< 缓冲溢出丢弃（唯一允许的丢弃，必有计数）
    std::uint64_t backendWriteFailures = 0;  ///< 后端写入失败次数（条数仍在缓冲里重试）
    std::uint64_t rejectedByLimit = 0;       ///< 查询被上限拒绝的次数
    std::uint64_t purgeFailures = 0;
    std::uint64_t purgedRows = 0;
    Millis        earliestPurgedTs = 0;
    Millis        lastPurgeTs = 0;
    double        lastWriteMs = 0;
    double        avgWriteMs  = 0;
    std::uint64_t seqFirst = 0, seqLast = 0; ///< 差值 ≠ appended 即存在缺口（P3）
    std::uint64_t flushes = 0;
    std::string   thresholdState = "ok";     ///< ok | degraded（后端连续失败）
    bool          purgeUnsupported = false;    ///< 后端不支持删除 → 保留降级（TLM-RET-04）
    bool          capacityUnsupported = false; ///< 后端不支持容量裁剪（TLM-RET-05）
    std::string   backend;
    std::string   version;

    nlohmann::json toJson() const;
};

}  // namespace telemetry_store

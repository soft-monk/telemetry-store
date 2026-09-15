// telemetry-store · store.h   ★ 宿主只认识这一个头文件
//
// 设计依据：docs/设计/遥测留存概要设计.md §3.2 / §4 / §5 / §6.3
//
// 一句话：**把实时流存下来，能查、能回放、能补发。**
//
//   ┌────────────┐   append   ┌──────────────────────────────────────┐
//   │ 宿主装配点  │───────────▶│ Store  写入缓冲 → 反向接口 → 后端     │
//   │（懂业务的那层）│  query   │        查询 / 回放 / 补发 / 保留      │
//   └────────────┘◀───────────└──────────────────────────────────────┘
//
// 五条使用前必须知道的约定：
//   ① 数据**只从 append 进**：模块不订阅任何上游，落库由宿主调 append（装配点在宿主里）。
//   ② `append` **非阻塞主路径**：P95 ≤ 1 ms；触发刷盘的那一次会包含后端写入耗时，
//      所以承诺是 P95 而不是 P100（D6：刷盘由调用驱动，不另开写线程）。
//   ③ `append` / `flush` / `sweep` **不抛异常**；后端失败只记账并在下轮重试。
//   ④ 区间一律 `[fromTs, toTs)`，结果 `ts` 升序；超限**截断 + 游标**，不静默丢（D5/D8/P4）。
//   ⑤ 一个 Store **独占**一个后端实例；`IStorageBackend` 实现**必须线程安全**（§5）。
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "telemetry_store/backend.h"
#include "telemetry_store/clock.h"
#include "telemetry_store/log_sink.h"
#include "telemetry_store/policy.h"
#include "telemetry_store/record.h"

namespace telemetry_store {

/// 刷盘阈值（D6）。两条任一满足即把整批交给后端。两条都为 0 表示只能手动 flush()。
struct FlushPolicy {
    std::size_t  maxRecords = 1000;  ///< 缓冲达到该条数即刷
    std::int64_t maxDelayMs = 200;   ///< 距上次刷盘达到该毫秒数即刷
};

/// 缓冲写满时的行为（TLM-WRT-06）。**没有"静默丢弃"这一项**（P2）。
enum class BufferFullPolicy {
    DropOldest,  ///< 淘汰最旧一条并计数告警（默认：绝不阻塞宿主线程）
    Block,       ///< 等下一次刷盘把空间腾出来（调用方接受背压）
};

struct StoreOptions {
    FlushPolicy       flush;                          ///< 默认 1000 条 / 200 ms
    std::size_t       bufferCapacity   = 65536;       ///< 未刷盘缓冲上限（条）
    BufferFullPolicy  onFull           = BufferFullPolicy::DropOldest;
    std::size_t       maxRowsPerQuery  = 10000;       ///< 单次查询返回上限（0 = 不限）
    std::size_t       maxScannedPerQuery = 0;         ///< 单次查询候选扫描上限（0 = 用 maxRows×10）
    std::int64_t      sweepIntervalMs  = 60000;       ///< 保留巡检周期（0 = 不自动巡检）
    std::int64_t      nominalPeriodMs  = 1000;        ///< 名义上报周期；gaps() 默认阈值 = 3×（D9）
    std::int64_t      warnWriteMs      = 1000;        ///< 单次后端写入超过该毫秒数即告警
    bool              enableSweeper    = true;        ///< 测试可关掉维护线程，全手动驱动
    bool              queryFlushesFirst = true;       ///< 查询前先刷盘，保证"刚写的能查到"（§4.6）
};

/// 实时数据留存层门面。
///
/// 线程模型（§5）：
///   · 宿主写入线程（0..N）→ append / flush
///   · 宿主查询线程（0..M）→ query / since / replay / gaps / status
///   · 模块维护线程（1）    → sweep（enableSweeper 时）
/// 所有公开方法**线程安全**；内部单锁保护缓冲与后端调用，计数为原子量。
class Store {
public:
    /// 默认依赖：内存后端 + 系统时钟 + 永久保留 + 无日志。
    /// 用途：示例、测试、以及"先把数据管起来"的起步形态（TLM-NFR-07 零依赖）。
    explicit Store(StoreOptions opt = StoreOptions{});
    ~Store();

    /// 完整注入形态（GoF 意义上的装配点：宿主在这里决定存哪里、怎么留）。
    /// 引用必须比 Store 活得久。
    Store(StoreOptions opt,
          IStorageBackend& backend,
          IClock&          clock,
          IRetentionPolicy& policy,
          ILogSink*        log = nullptr);

    Store(const Store&)            = delete;
    Store& operator=(const Store&) = delete;

    // ─────────────────────────────────────────────── 写入（非阻塞、不抛）
    /// 追加一条。`rec.seq == 0` 时由模块补齐序号（单调）。
    void append(Record rec);
    /// 便捷重载。
    void append(std::string deviceId, std::string type, Millis ts, nlohmann::json data);
    /// 批量追加（宿主手上已是整批时用，语义与逐条完全一致）。
    void appendBatch(std::vector<Record> recs);
    /// 立即把缓冲全部交付后端。**返回即已交付**（失败则仍在缓冲里等待重试）。
    void flush();

    // ─────────────────────────────────────────────── 查询（区间 [from, to)，升序）
    //
    // ⚠️ 这三个方法**非 const**，因为它们有一个副作用：查询前会先把未刷盘缓冲交付后端
    //    （`queryFlushesFirst`，保证"刚 append 进去的立刻能查到"）。
    //    要纯只读的观测请不要用它们，用 `status()`。
    QueryResult query(const Query& q);
    /// 续查：用上一次结果里的 `cursor`。
    QueryResult queryPage(const Query& q, const Cursor& c);

    // ─────────────────────────────────────────────── 回放 / 补发
    /// 补发：返回 **ts > 水位** 的记录，包成时间轴（TLM-GAP-01/02/03）。
    Timeline since(const SinceRequest& req);
    /// 回放：取区间数据 + 稀疏化，输出时间轴（TLM-RPL-01..04）。
    Timeline replay(const ReplayRequest& req);
    /// 缺口识别（TLM-GAP-04）。maxGapMs <= 0 时用 `nominalPeriodMs × 3`。
    /// devices 为空 = 全部设备。结果受 `maxScannedPerQuery` 保护，超大区间请自行切段。
    std::vector<Gap> gaps(Millis fromTs, Millis toTs, Millis maxGapMs = 0,
                          const std::vector<std::string>& devices = {});

    // ─────────────────────────────────────────────── 保留 / 状态
    /// 立即巡检一次（TLM-RET-01..05）。不抛；失败只记账。
    RetentionReport sweep();
    /// 可观测快照（TLM-OBS-01/02）。不加锁、不刷盘，可高频调用。
    Status status() const;

    /// 当前选项（只读）。
    const StoreOptions& options() const { return opt_; }
    /// 当前后端名（诊断用，如 "memory" / "segfile"）。
    const char* backendName() const;

    // ─────────────────────────────────────────────── 纯函数工具（不依赖任何上游代码）
    /// 标准信封 `{ "type": ..., "data": {...}, "ts": ... }` → Record（TLM-ADP-04）。
    /// 只依赖信封**语法**，不 import `realtime-hub` 的任何东西。
    /// 缺字段 / 类型不符 / `ts` 非 number → 返回 false（不抛）。
    static bool fromEnvelope(const nlohmann::json& envelope, Record& out);

private:
    /// 取数的两种口径（内部用）：
    ///   Caller    调用方发起的查询 —— 受 `maxRowsPerQuery` 守卫约束（越界要拒绝，P4）
    ///   ScanOnly  模块自己的内部扫描（如 gaps）—— 不受该守卫约束，只受扫描上限约束；
    ///             被扫描上限截断时**必须**记日志，绝不假装分析过全区间
    enum class FetchMode { Caller, ScanOnly };

    /// 真正的构造逻辑（两个公开构造函数都汇到这里）。
    void init(ILogSink* log);
    /// 把缓冲整批交给后端（调用者须持有 mtx_）。返回交付条数。
    std::size_t flushLocked();
    /// 巡检线程主体。
    void sweeperLoop();
    /// 通用区间取数（query / since / replay / gaps 共用一条路径）。
    QueryResult fetch(const Query& q, const Cursor* beginAfter,
                      FetchMode mode = FetchMode::Caller);

    StoreOptions        opt_;
    IStorageBackend*    backend_ = nullptr;   ///< nullptr = 模块自持的默认内存后端
    std::unique_ptr<IStorageBackend> ownedBackend_;
    IClock*             clock_   = nullptr;
    std::unique_ptr<IClock> ownedClock_;
    IRetentionPolicy*   policy_  = nullptr;
    std::unique_ptr<IRetentionPolicy> ownedPolicy_;
    ILogSink*           log_     = nullptr;   ///< nullptr → 静态空实现

    /// 缓冲本体与后端调用共用一把锁（§5 锁口径：取批/入队/记账在锁内）。
    /// 用 recursive_mutex 是因为 public 方法之间会互相调用（如 flush → flushLocked）。
    mutable std::recursive_mutex mtx_;
    mutable std::condition_variable_any cv_;
    std::vector<Record>          buffer_;
    std::size_t                  bufferHead_ = 0;   ///< 逻辑队首（避免 erase 移动）
    std::size_t                  bufferCount_ = 0;  ///< 逻辑长度
    std::int64_t                 lastFlushMs_ = 0;  ///< steady 时刻
    bool                         warnedOverflow_ = false;
    /// 本实例是否**真的删过数据**（时间清理或容量裁剪成功执行过）。
    /// 仅用于 truncatedByRetention 的判定：必须区分"本来就没数据"与"数据被删了"，
    /// 否则会谎报数据丢失。两种裁剪都算：对使用者而言"早期数据没了"是同一件事。
    /// 受 mtx_ 保护（只在 sweep / fetch 里访问，两者都持锁）。
    bool                         everPurged_ = false;

    std::thread       sweeper_;
    std::atomic<bool> stop_{false};

    std::atomic<std::uint64_t> appended_{0};
    std::atomic<std::uint64_t> persisted_{0};
    std::atomic<std::uint64_t> droppedByOverflow_{0};
    std::atomic<std::uint64_t> backendWriteFailures_{0};
    std::atomic<std::uint64_t> consecutiveWriteFailures_{0};
    std::atomic<std::uint64_t> rejectedByLimit_{0};
    std::atomic<std::uint64_t> purgeFailures_{0};
    std::atomic<std::uint64_t> purgedRows_{0};
    std::atomic<std::uint64_t> flushes_{0};
    std::atomic<Millis>        earliestPurgedTs_{0};
    std::atomic<Millis>        lastPurgeTs_{0};
    std::atomic<std::uint64_t> seqCounter_{0};
    std::atomic<std::uint64_t> seqFirst_{0};
    std::atomic<std::uint64_t> seqLast_{0};
    std::atomic<double>        lastWriteMs_{0.0};
    std::atomic<double>        totalWriteMs_{0.0};
    std::atomic<bool>          purgeUnsupported_{false};
    std::atomic<bool>          capacityUnsupported_{false};
};

}  // namespace telemetry_store

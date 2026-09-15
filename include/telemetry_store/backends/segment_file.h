// telemetry-store · backends/segment_file.h · 文件分段后端（L2：跨重启留存，仍零外部依赖）
//
// 设计依据：docs/设计/遥测留存概要设计.md §2.2 · §7 R3 · 决策 D15
// 需求：TLM-NFR-07（不引外部服务）· TLM-QRY-02（区间查询要能二分跳）· TLM-RET-04（删整段）
//
// 为什么是"分段 + 段内稀疏索引"：
//   · 一次查询只需要读**落在区间里的段**，不用扫全量历史；
//   · 段内按**序号稀疏索引**（每 N 条记一条 (ts, offset)）就能二分跳到区间起点，
//     不必逐行反序列化前面几十万条；
//   · 清理过期数据时，**整段删除**即可（若只有一段跨越边界，只重写那一段）。
//
// 物理形态：
//   <dir>/
//     ├─ 0000000001700000000000-0.seg     段文件：每条记录一行 JSON（JSON Lines）+ '#' 开头的索引行
//     └─ ...                              文件名 = 起始 ts（补零，保证字典序 = 时间序）+ 序号
//
// 段文件格式（一行一条，UTF-8，'\n' 分隔）：
//   {"deviceId":"uav-1","type":"uav.pos","ts":1700000000000,"seq":1,"data":{...}}
//   #idx {"ts":1700000000000,"off":123,"row":0}      ← 每 indexEvery 行插一条，供二分
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "telemetry_store/backend.h"

namespace telemetry_store {

/// 分段文件后端。线程安全（backend.h 约定 ①）。
///
/// 构造即"打开/续写"目录：目录里已有的段会被扫出来接着用（跨进程重启留存，TLM-NFR-06/R3）。
class SegmentFileBackend : public IStorageBackend {
public:
    struct Options {
        std::size_t  maxRowsPerSegment = 65536;  ///< 段容量：达到即滚新段
        std::size_t  indexEvery        = 1024;   ///< 段内每多少行插一条稀疏索引
        std::size_t  readCacheRows     = 20000;  ///< 段读缓存上限（条），超出按最旧淘汰
    };

    explicit SegmentFileBackend(std::string dir, Options opt = Options{});
    ~SegmentFileBackend() override;

    SegmentFileBackend(const SegmentFileBackend&)            = delete;
    SegmentFileBackend& operator=(const SegmentFileBackend&) = delete;

    bool append(RecordBatch batch) override;
    std::size_t read(const Query& q, const Cursor* beginAfter,
                     const RecordVisitor& visit) override;
    bool purge(Millis cutoffTs) override;
    bool canPurge() const override { return true; }
    Millis earliestRetainedTs() const override;
    std::size_t approxRows() const override;
    std::size_t approxBytes() const override;
    const char* name() const override { return "segfile"; }

    /// 关闭当前段（把缓冲刷到磁盘）。析构与 purge 都会调它。
    void flushToDisk();

    /// 数据目录（诊断用）。
    const std::string& dir() const { return dir_; }
    /// 段数量（诊断 / 测试用）。
    std::size_t segmentCount() const;

private:
    struct SegmentInfo {
        std::string  path;
        Millis       startTs = 0;   ///< 段内最早 ts（也是文件名前缀，-1 = 尚无数据）
        Millis       endTs   = 0;   ///< 段内最晚 ts
        std::uint64_t rows   = 0;
        std::uint64_t bytes  = 0;
    };
    struct IndexEntry {
        Millis      ts  = 0;
        std::streamoff off = 0;
        std::uint64_t row = 0;
    };
    /// 段读缓存：避免同一段在一次查询里被反复读盘。
    struct CacheEntry {
        std::string         path;
        std::vector<Record> rows;
        std::size_t         bytes = 0;
    };
    struct ReadHit {
        bool          found = false;
        Millis        ts    = 0;
        std::streamoff off  = 0;
        std::uint64_t row   = 0;
    };

    /// 打开（或创建）一个新段；调用者须持锁。
    bool openNewSegmentLocked(Millis firstTs);
    /// 关闭当前段的写入流；调用者须持锁。
    void closeWriterLocked();
    /// 扫目录，装载已有段（取文件名的起始 ts 与行数）；调用者须持锁。
    void loadExistingLocked();
    /// 统计一个段文件的行数（跳过索引行）；用于装载已有段。
    static std::uint64_t countRowsIn(const std::string& path);
    /// 在段文件里二分定位第一个 `ts >= wantTs` 的近似位置；调用者须持锁。
    ReadHit locateLocked(const std::string& path, uint64_t wantTs);
    /// 读一个段（带缓存）。返回记录序列（**保持文件内顺序**）。
    bool readSegmentLocked(const std::string& path, std::vector<Record>& out);
    /// 重写一个段，只保留 `keep` 谓词为真的记录；调用者须持锁。
    bool rewriteSegmentLocked(SegmentInfo& seg, Millis cutoffTs);
    /// 段读缓存淘汰到上限以内；调用者须持锁。
    void trimCacheLocked();

    std::string  dir_;
    Options      opt_;
    mutable std::mutex mtx_;

    std::vector<SegmentInfo> segs_;        ///< 段元数据（按 startTs 升序）
    std::string  curPath_;                 ///< 当前写入段（空 = 还没开）
    std::ofstream writer_;
    std::uint64_t curRows_ = 0;            ///< 当前段已写行数
    std::uint64_t curBytes_ = 0;           ///< 当前段字节数（估算）
    Millis       curStartTs_ = -1;
    Millis       curEndTs_ = 0;

    std::deque<CacheEntry> cache_;         ///< 段读缓存（LRU 尾部为最新）
    std::size_t  cacheBytes_ = 0;
};

}  // namespace telemetry_store

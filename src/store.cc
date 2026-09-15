// src/store.cc · 门面实现：写入编排 / 查询编排 / 回放 / 补发 / 保留 / 状态
//
// 设计依据：docs/设计/遥测留存概要设计.md §4（关键机制）· §5（并发模型）· §6.3（错误口径）
// 需求：TLM-WRT-01..08 · QRY-01..05 · RPL-01..04 · GAP-01..05 · RET-01..05 · OBS-01..03
#include "telemetry_store/store.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "telemetry_store/backends/memory_backend.h"
#include "query.h"
#include "util.h"

namespace telemetry_store {
namespace {

NullLogSink g_nullLog;

std::int64_t steadyNow() { return steadyMs(); }

double msSince(std::int64_t startSteady) {
    return static_cast<double>(steadyNow() - startSteady);
}

}  // namespace

// ================================================================ 构造 / 析构
Store::Store(StoreOptions opt) : opt_(std::move(opt)) {
    auto mem = std::make_unique<MemoryBackend>();
    ownedBackend_ = std::move(mem);  // 先持有所有权，再取裸指针
    backend_ = ownedBackend_.get();
    auto clock = std::make_unique<SystemClock>();
    ownedClock_ = std::move(clock);
    clock_ = ownedClock_.get();
    auto policy = std::make_unique<KeepForeverPolicy>();
    ownedPolicy_ = std::move(policy);
    policy_ = ownedPolicy_.get();
    init(nullptr);
}

Store::Store(StoreOptions opt, IStorageBackend& backend, IClock& clock,
             IRetentionPolicy& policy, ILogSink* log)
    : opt_(std::move(opt)), backend_(&backend), clock_(&clock), policy_(&policy) {
    init(log);
}

void Store::init(ILogSink* log) {
    log_ = (log != nullptr) ? log : &g_nullLog;
    lastFlushMs_ = steadyNow();

    if (opt_.enableSweeper && opt_.sweepIntervalMs > 0) {
        sweeper_ = std::thread([this] { sweeperLoop(); });
    }
}

Store::~Store() {
    // 顺序很重要：先让巡检线程退出（它也会取锁调后端），再刷最后一次缓冲。
    stop_.store(true);
    cv_.notify_all();
    if (sweeper_.joinable()) sweeper_.join();

    std::lock_guard<std::recursive_mutex> lk(mtx_);
    flushLocked();  // TLM-WRT-07：析构等价于 flush()
}

void Store::sweeperLoop() {
    std::unique_lock<std::recursive_mutex> lk(mtx_);
    while (!stop_.load()) {
        if (cv_.wait_for(lk, std::chrono::milliseconds(opt_.sweepIntervalMs),
                         [this] { return stop_.load(); })) {
            break;  // 收到停止
        }
        lk.unlock();
        try {
            sweep();
        } catch (const std::exception& e) {
            log_->log("ret", nlohmann::json{{"event", "sweepThrew"}, {"error", e.what()}});
        } catch (...) {
            log_->log("ret", nlohmann::json{{"event", "sweepThrew"}, {"error", "unknown"}});
        }
        lk.lock();
    }
}

// ================================================================ 写入
void Store::append(Record rec) {
    // 用 unique_lock：Block 策略下要在这把锁上等（condition_variable_any 的要求）。
    std::unique_lock<std::recursive_mutex> lk(mtx_);

    // seq 由模块补齐（TLM-WRT-05）：调用方给了就尊重，没给就发号。
    if (rec.seq == 0) rec.seq = seqCounter_.fetch_add(1) + 1;
    if (seqFirst_.load() == 0) seqFirst_.store(rec.seq);
    seqLast_.store(rec.seq);

    // 缓冲上限（TLM-WRT-06）：绝不静默丢弃，且 Block 也有 2s 兜底——append 不允许无限期挂住。
    if (opt_.bufferCapacity > 0 && bufferCount_ >= opt_.bufferCapacity) {
        if (opt_.onFull == BufferFullPolicy::Block) {
            const std::int64_t deadline = steadyNow() + 2000;
            while (bufferCount_ >= opt_.bufferCapacity && steadyNow() < deadline) {
                cv_.wait_for(lk, std::chrono::milliseconds(50));
            }
        }
        if (bufferCount_ >= opt_.bufferCapacity) {
            flushLocked();  // 先试试能不能腾出空间
        }
        while (opt_.bufferCapacity > 0 && bufferCount_ >= opt_.bufferCapacity) {
            buffer_[bufferHead_] = Record{};  // 释放字符串与 JSON 占用的内存
            bufferHead_ = (bufferHead_ + 1) % buffer_.size();
            --bufferCount_;
            droppedByOverflow_.fetch_add(1);
            if (!warnedOverflow_) {
                warnedOverflow_ = true;
                log_->log("wrt", nlohmann::json{{"event", "bufferOverflow"},
                                                {"policy", "DropOldest"},
                                                {"capacity", opt_.bufferCapacity}});
            }
        }
    }

    if (buffer_.empty()) buffer_.resize(64);
    if (bufferCount_ == buffer_.size()) {  // 环形扩容
        std::vector<Record> grown(buffer_.size() * 2);
        for (std::size_t i = 0; i < bufferCount_; ++i) {
            grown[i] = std::move(buffer_[(bufferHead_ + i) % buffer_.size()]);
        }
        buffer_.swap(grown);
        bufferHead_ = 0;
    }
    const std::size_t slot = (bufferHead_ + bufferCount_) % buffer_.size();
    buffer_[slot] = std::move(rec);
    ++bufferCount_;
    appended_.fetch_add(1);

    // 刷盘阈值（D6）：条数或时间任一达成即刷——刷盘**在调用线程内同步完成**。
    const bool byCount = opt_.flush.maxRecords > 0 && bufferCount_ >= opt_.flush.maxRecords;
    const bool byTime  = opt_.flush.maxDelayMs > 0 &&
                         (steadyNow() - lastFlushMs_) >= opt_.flush.maxDelayMs;
    if (byCount || byTime) flushLocked();
}

void Store::append(std::string deviceId, std::string type, Millis ts, nlohmann::json data) {
    Record r;
    r.deviceId = std::move(deviceId);
    r.type     = std::move(type);
    r.ts       = ts;
    r.data     = std::move(data);
    append(std::move(r));
}

void Store::appendBatch(std::vector<Record> recs) {
    for (auto& r : recs) append(std::move(r));
}

void Store::flush() {
    std::lock_guard<std::recursive_mutex> lk(mtx_);
    flushLocked();
    cv_.notify_all();
}

std::size_t Store::flushLocked() {
    if (bufferCount_ == 0) {
        lastFlushMs_ = steadyNow();
        return 0;
    }

    // 取出整批交给后端（排序不在这里做——后端只追加，排序发生在读路径）。
    std::vector<Record> batch;
    batch.reserve(bufferCount_);
    for (std::size_t i = 0; i < bufferCount_; ++i) {
        batch.push_back(std::move(buffer_[(bufferHead_ + i) % buffer_.size()]));
    }

    const std::int64_t t0 = steadyNow();
    bool ok = false;
    try {
        ok = backend_->append(RecordBatch(batch));
    } catch (const std::exception& e) {
        // 模块边界内吞掉异常（§6.3）：绝不让后端的异常穿到宿主。
        log_->log("wrt", nlohmann::json{{"event", "backendThrew"}, {"error", e.what()}});
        ok = false;
    } catch (...) {
        log_->log("wrt", nlohmann::json{{"event", "backendThrew"}, {"error", "unknown"}});
        ok = false;
    }
    const double cost = msSince(t0);
    lastWriteMs_.store(cost);
    totalWriteMs_.store(totalWriteMs_.load() + cost);

    std::size_t delivered = 0;
    if (ok) {
        bufferHead_ = 0;
        bufferCount_ = 0;
        delivered = batch.size();
        persisted_.fetch_add(delivered);
        flushes_.fetch_add(1);
        consecutiveWriteFailures_.store(0);
        warnedOverflow_ = false;
        cv_.notify_all();
    } else {
        // 交付失败：**数据留在缓冲里等下一轮重试**，并如实计数（TLM-OBS-02）。
        const std::size_t n = batch.size();
        if (buffer_.size() - bufferHead_ >= n) {
            for (std::size_t i = 0; i < n; ++i) buffer_[bufferHead_ + i] = std::move(batch[i]);
        } else {  // 环形回绕
            for (std::size_t i = 0; i < n; ++i) {
                buffer_[(bufferHead_ + i) % buffer_.size()] = std::move(batch[i]);
            }
        }
        backendWriteFailures_.fetch_add(1);
        consecutiveWriteFailures_.fetch_add(1);
    }

    if (cost >= static_cast<double>(opt_.warnWriteMs)) {
        log_->log("wrt", nlohmann::json{{"event", "slowFlush"},
                                        {"records", batch.size()},
                                        {"ms", cost},
                                        {"thresholdMs", opt_.warnWriteMs}});
    }
    lastFlushMs_ = steadyNow();
    return delivered;
}

// ================================================================ 查询
QueryResult Store::fetch(const Query& q, const Cursor* beginAfter, FetchMode mode) {
    QueryResult out;

    // 越界要明说（P4，设计 §4.6）：调用方把 `Query::limit` 设得**超过本 Store 允许的上限**时，
    // 直接拒绝执行并记账，而不是悄悄只给一部分。想要更多就分页（queryPage + cursor），
    // 或调大 `maxRowsPerQuery`。
    //
    // 为什么不做"悄悄封顶"：封顶看着更宽容，但它让 `limit` 变成"我大概想要这么多"，
    // 而调用方无法区分"我就只有这些数据"和"你要的太多所以我少给了"。
    // `rejected=true` + 可读 reason 把这件事**说清楚**，符合"越界要明说"。
    //
    // ScanOnly（模块内部扫描，如 gaps）不走这道守卫：它要的是"扫到哪算哪"，
    // 由扫描上限（maxScannedPerQuery）单独约束，被截断时会记日志。
    if (mode == FetchMode::Caller && opt_.maxRowsPerQuery > 0 &&
        q.limit > opt_.maxRowsPerQuery) {
        rejectedByLimit_.fetch_add(1);
        log_->log("qry", nlohmann::json{{"event", "rejectedByLimit"},
                                        {"requested", q.limit},
                                        {"maxRowsPerQuery", opt_.maxRowsPerQuery}});
        out.rejected = true;
        out.rejectReason = "limit " + std::to_string(q.limit) + " 超过 maxRowsPerQuery " +
                           std::to_string(opt_.maxRowsPerQuery) + "：请分页（queryPage + cursor）";
        return out;  // 空结果 + rejected=true：调用方看得出"这不是没有数据"
    }

    std::lock_guard<std::recursive_mutex> lk(mtx_);

    // ScanOnly 用"扫描上限"当唯一约束——不回落成 maxRowsPerQuery，避免静默截断。
    const std::size_t pageLimit = (mode == FetchMode::ScanOnly)
                                      ? 0
                                      : (q.limit > 0 ? q.limit : opt_.maxRowsPerQuery);
    const std::size_t scanCap = opt_.maxScannedPerQuery > 0
                                    ? opt_.maxScannedPerQuery
                                    : (pageLimit > 0 ? pageLimit * 10 : 0);

    // ⚠️ 这个快照必须在 flushLocked() **之前**取：
    //    查询前的这次刷盘如果失败，缓冲里就还有没落盘的数据，查询结果自然可能不完整。
    //    早期版本把快照放在 flush 之后，于是"查询前那次刷盘的失败"被吃掉，
    //    partial 永远是 false —— 一个"越失败越说自己没问题"的错（由需求自测抓出）。
    const std::uint64_t failuresBefore = backendWriteFailures_.load();
    const std::uint64_t droppedBefore  = droppedByOverflow_.load();

    if (opt_.queryFlushesFirst) flushLocked();  // §4.6：刚写进去的要能查到

    std::vector<Record> rows;
    std::size_t matched = 0;
    bool scanStopped = false;
    if (pageLimit > 0) rows.reserve(pageLimit + 1);

    const RecordVisitor visitor = [&](const Record& r) -> bool {
        if (!detail::matches(r, q)) return true;  // 后端可以多吐，模块自己兜底过滤
        ++matched;
        rows.push_back(r);
        if (scanCap > 0 && rows.size() >= scanCap) {
            scanStopped = true;
            return false;  // 收够了
        }
        return true;
    };

    backend_->read(q, beginAfter, visitor);

    detail::sortByKey(rows);
    detail::dedupLastWins(rows);  // TLM-WRT-08：同一主键后写胜（D7：写在读路径）

    out.scanned = matched;

    if (pageLimit > 0 && rows.size() > pageLimit) {
        rows.resize(pageLimit);
        out.hasMore = true;
        out.cursor  = Cursor::after(rows.back());
    } else if (scanStopped) {
        // 命中扫描上限：如实说明"可能还有"，而不是假装查完了。
        out.hasMore = true;
        if (!rows.empty()) out.cursor = Cursor::after(rows.back());
    }
    out.rows = std::move(rows);

    if (!out.rows.empty()) {
        out.coveredFromTs = out.rows.front().ts;
        out.coveredToTs   = out.rows.back().ts;
    }

    // P4：越界要明说 —— 查询区间早于保留边界时，如实标注"早期数据已被删掉"。
    //
    // ⚠️ 必须同时满足两个条件，缺一个就会误报：
    //   ① 这个 Store **确实删过数据**（everPurged_：时间清理或容量裁剪成功过）
    //   ② 查询区间起点早于**当前仍保留的最早数据**
    // 只用 ② 的写法分不清"本来就没数据"与"数据被删了"：
    // 从一开始就查 [0, now)（这是个很自然的写法）会被无端标成"被清理过"，
    // 于是使用者以为丢过数据 —— 一个"谎报数据丢失"的错（由需求自测抓出）。
    const Millis earliest = backend_->earliestRetainedTs();
    if (everPurged_ && earliest > 0 && q.fromTs < earliest) out.truncatedByRetention = true;
    // P2：查询过程中发生过写入失败 → 缓冲里还有没落盘的数据，结果可能不完整。
    if (backendWriteFailures_.load() != failuresBefore) out.partial = true;
    if (scanStopped) out.partial = true;
    // P3：发生过缓冲溢出丢弃 → 序号已经断了，这个库**不再完整**，如实标注。
    //     一旦丢过就一直是 true（丢弃是不可逆的），不会因为后来写得好就假装完好。
    if (droppedByOverflow_.load() > 0 || droppedBefore > 0) out.partial = true;

    return out;
}

QueryResult Store::query(const Query& q) {
    return fetch(q, nullptr);
}

QueryResult Store::queryPage(const Query& q, const Cursor& c) {
    if (!c.valid) return fetch(q, nullptr);
    return fetch(q, &c);
}

// ================================================================ 补发
Timeline Store::since(const SinceRequest& req) {
    Timeline tl;

    if (!req.perDevice.empty()) {
        // 每设备各自水位（TLM-GAP-02）：一设备一趟，各自取 ts > 该设备水位。
        std::unordered_set<std::string> seen;
        for (const auto& kv : req.perDevice) {
            if (!seen.insert(kv.first).second) continue;  // 同一设备给两次水位只认第一次
            Query q;
            q.devices = {kv.first};
            q.types   = req.types;
            q.fromTs  = kv.second + 1;
            q.toTs    = std::numeric_limits<Millis>::max();
            q.limit   = req.limit;
            QueryResult part = fetch(q, nullptr);
            for (auto& r : part.rows) tl.samples.push_back(std::move(r));
            tl.hasMore = tl.hasMore || part.hasMore;
            tl.partial = tl.partial || part.partial;
            tl.truncatedByRetention = tl.truncatedByRetention || part.truncatedByRetention;
            if (part.hasMore) tl.cursor = part.cursor;
        }
    } else {
        Query q;
        q.devices = req.devices;
        q.types   = req.types;
        q.fromTs  = req.lastSeenTs + 1;  // 严格大于（TLM-GAP-01）
        q.toTs    = std::numeric_limits<Millis>::max();
        q.limit   = req.limit;
        QueryResult part = fetch(q, nullptr);
        tl.samples = std::move(part.rows);
        tl.hasMore = part.hasMore;
        tl.cursor  = part.cursor;
        tl.partial = part.partial;
        tl.truncatedByRetention = part.truncatedByRetention;
    }

    detail::sortByKey(tl.samples);  // 多设备归并成一条时间轴
    if (!tl.samples.empty()) {
        tl.fromTs = tl.samples.front().ts;
        tl.toTs   = tl.samples.back().ts;
        tl.durationMs = tl.toTs - tl.fromTs;
    }
    return tl;
}

// ================================================================ 回放
Timeline Store::replay(const ReplayRequest& req) {
    Query q = req.q;
    if (!req.deviceId.empty()) q.devices = {req.deviceId};

    const std::size_t everyNth = req.everyNth > 0 ? req.everyNth : 1;
    const std::size_t baseLimit = q.limit > 0 ? q.limit : opt_.maxRowsPerQuery;

    // ⚠️ 这里**不能**把 q.limit 乘上 everyNth（曾经就是这么写的）：
    //    乘出来的 20000/50000 会超过 maxRowsPerQuery，被"超限拒绝"逻辑挡掉，
    //    结果是"一稀疏化就返回 0 条"——一个只在采样时发作的 bug（由 example_replay 抓出）。
    //    正确做法：候选数量由 fetch 的扫描上限（maxScannedPerQuery）自然约束，
    //    采样后若仍超过 baseLimit 再截断并置 hasMore。
    q.limit = 0;

    QueryResult res = fetch(q, nullptr);

    Timeline tl;
    tl.fromTs = q.fromTs;
    tl.toTs   = q.toTs;
    tl.sampled = (everyNth > 1) || (req.minIntervalMs > 0);
    tl.sampleStep = everyNth;
    tl.partial = res.partial;
    tl.truncatedByRetention = res.truncatedByRetention;

    Millis lastEmittedTs = 0;
    bool haveEmitted = false;
    std::size_t candidates = 0;
    for (const auto& r : res.rows) {
        // TLM-RPL-04：每第 N 条 —— 按**候选计数**取模，与 minIntervalMs 互不干扰。
        if (everyNth > 1 && (candidates++ % everyNth) != 0) continue;
        // 最小间隔：与上一条已发出的样本比。
        if (req.minIntervalMs > 0 && haveEmitted && (r.ts - lastEmittedTs) < req.minIntervalMs) {
            continue;
        }
        tl.samples.push_back(r);
        lastEmittedTs = r.ts;
        haveEmitted = true;
    }
    if (baseLimit > 0 && tl.samples.size() > baseLimit) {
        tl.samples.resize(baseLimit);
        tl.hasMore = true;
        tl.cursor = Cursor::after(tl.samples.back());
    }
    if (!tl.samples.empty()) {
        tl.durationMs = tl.samples.back().ts - tl.samples.front().ts;
    }
    return tl;
}

// ================================================================ 缺口识别
std::vector<Gap> Store::gaps(Millis fromTs, Millis toTs, Millis maxGapMs,
                             const std::vector<std::string>& devices) {
    std::vector<Gap> out;

    const Millis threshold =
        maxGapMs > 0 ? maxGapMs : (opt_.nominalPeriodMs > 0 ? opt_.nominalPeriodMs * 3 : 0);
    if (threshold <= 0 || toTs <= fromTs) return out;

    // 与 query 同源：取全区间的有序记录，再在内存里按设备分组走一遍。
    // D9：阈值法（相邻间隔 > 阈值即一段缺口），不做失联语义判定。
    //
    // ⚠️ 这里必须用 **ScanOnly** 口径取数：
    //    早期版本传 q.limit=0，fetch 会把 pageLimit 回落到 maxRowsPerQuery（默认 10000），
    //    大区间于是被**静默截断**——缺口恰好落在被截掉的后半段时 gaps() 报"0 段缺口"，
    //    而调用方从 std::vector<Gap> 上完全看不出自己被截断了（P4 违规）。
    //    ScanOnly 让扫描上限（maxScannedPerQuery）成为唯一约束，真到上限时下面会记日志告警。
    Query q;
    q.devices = devices;
    q.fromTs  = fromTs;
    q.toTs    = toTs;
    q.limit   = 0;
    QueryResult res = fetch(q, nullptr, FetchMode::ScanOnly);

    // 被扫描上限截断了就**说出来**，不假装分析过全区间（P4：越界要明说）。
    if (res.partial || res.hasMore) {
        log_->log("gap", nlohmann::json{{"event", "scanTruncated"},
                                        {"fromTs", fromTs},
                                        {"toTs", toTs},
                                        {"scanned", res.scanned},
                                        {"hint", "区间过大，请切段调用 gaps() 或调大 maxScannedPerQuery"}});
    }

    // ⚠️ 必须**按设备分组**比较，不能比较"相邻两行"（曾经就是那么写的）：
    //    多设备交错上报时相邻两行几乎总是不同设备，于是缺口全被漏掉
    //    —— 表现为"多设备查询永远报 0 段缺口"（由 example_replay 抓出）。
    //    rows 已按 (ts, deviceId, type) 排序，所以同一设备的记录未必相邻，只保留每设备的上一条。
    std::unordered_map<std::string, Record> lastOf;
    for (const auto& r : res.rows) {
        auto it = lastOf.find(r.deviceId);
        if (it == lastOf.end()) {
            lastOf.emplace(r.deviceId, r);
            continue;
        }
        const Record& prev = it->second;
        if ((r.ts - prev.ts) > threshold) {
            Gap g;
            g.deviceId   = r.deviceId;
            g.fromTs     = prev.ts + 1;  // 前一条之后（含）
            g.toTs       = r.ts;         // 后一条之前（不含）
            g.durationMs = g.toTs - g.fromTs;
            g.seqBefore  = prev.seq;
            g.seqAfter   = r.seq;
            out.push_back(std::move(g));
        }
        it->second = r;  // 该设备的水位前移
    }

    // 输出顺序稳定：先按缺口起点，再按设备名。
    std::sort(out.begin(), out.end(), [](const Gap& a, const Gap& b) {
        return std::tie(a.fromTs, a.deviceId) < std::tie(b.fromTs, b.deviceId);
    });
    return out;
}

// ================================================================ 保留清理
RetentionReport Store::sweep() {
    RetentionReport rep;
    rep.backend = backendName();

    std::lock_guard<std::recursive_mutex> lk(mtx_);
    rep.nowTs = clock_->nowMs();

    if (!backend_->canPurge()) {
        // TLM-RET-04：能力诚实 —— 不假装成功，也不反复重试。
        if (!purgeUnsupported_.exchange(true)) {
            log_->log("ret", nlohmann::json{{"event", "purgeUnsupported"}, {"backend", rep.backend}});
        }
        rep.supported = false;
        return rep;
    }

    // 时间保留（TLM-RET-01）。
    //
    // ⚠️ IRetentionPolicy 是**按 type** 给 TTL，而 IStorageBackend::purge 只能按 ts 一刀切。
    //    没有类型清单时，正确做法是取**最大** TTL（少清，绝不多清）：
    //    宁可晚清，也不能把"本该留 7 天"的数据按 1 小时的 TTL 删掉——那是未经通知的数据销毁。
    //    真需要逐类型裁剪的重后端，应在自己的实现里按类型处理。
    const std::int64_t ttl = policy_->ttlMs(std::string());
    if (ttl > 0) {
        const Millis cutoff = detail::cutoffFor(rep.nowTs, ttl);
        if (cutoff > 0) {
            rep.cutoffTs = cutoff;
            const std::size_t rowsBefore = backend_->approxRows();
            bool ok = false;
            try {
                ok = backend_->purge(cutoff);
            } catch (...) {
                ok = false;
            }
            if (ok) {
                const std::size_t rowsAfter = backend_->approxRows();
                rep.purgedRows = rowsBefore > rowsAfter ? (rowsBefore - rowsAfter) : 0;
                purgedRows_.fetch_add(rep.purgedRows);
                earliestPurgedTs_.store(std::max(earliestPurgedTs_.load(), cutoff));
                everPurged_ = true;  // 供 truncatedByRetention 判定（见 fetch）
            } else {
                purgeFailures_.fetch_add(1);
                log_->log("ret", nlohmann::json{{"event", "purgeFailed"},
                                                {"cutoffTs", cutoff},
                                                {"backend", rep.backend}});
            }
        }
    }

    // 容量上限（TLM-RET-05）：时间清不掉的部分按条数压。
    const std::size_t cap = policy_->maxRows();
    if (cap > 0 && backend_->approxRows() > cap) {
        const std::size_t rowsBeforeTrim = backend_->approxRows();
        bool ok = false;
        try {
            ok = backend_->trimToRows(cap);
        } catch (...) {
            ok = false;
        }
        if (ok) {
            rep.capacityTrimmed = true;
            // TLM-RET-05 要求"淘汰要计数"：这里如实记下本次淘汰了多少条。
            // 用"裁剪前后条数差"算，而不是靠后端提供淘汰账本 —— 后者会给
            // IStorageBackend 加上一层它不该关心的记账责任（接口污染）。
            const std::size_t rowsAfterTrim = backend_->approxRows();
            rep.capacityEvictedRows = rowsBeforeTrim > rowsAfterTrim
                                          ? static_cast<std::uint64_t>(rowsBeforeTrim - rowsAfterTrim)
                                          : 0;
            everPurged_ = true;  // 容量裁剪同样"删了最旧的数据"，也供 truncatedByRetention 判定
        } else if (!capacityUnsupported_.exchange(true)) {
            log_->log("ret", nlohmann::json{{"event", "capacityUnsupported"},
                                            {"backend", rep.backend},
                                            {"maxRows", cap}});
        }
    }

    lastPurgeTs_.store(rep.nowTs);
    if (rep.purgedRows > 0 || rep.capacityTrimmed) log_->log("ret", rep.toJson());
    return rep;
}

// ================================================================ 状态
Status Store::status() const {
    std::lock_guard<std::recursive_mutex> lk(mtx_);

    Status s;
    s.appended             = appended_.load();
    s.persisted            = persisted_.load();
    s.buffered             = bufferCount_;
    s.droppedByOverflow    = droppedByOverflow_.load();
    s.backendWriteFailures = backendWriteFailures_.load();
    s.rejectedByLimit      = rejectedByLimit_.load();
    s.purgeFailures        = purgeFailures_.load();
    s.purgedRows           = purgedRows_.load();
    s.earliestPurgedTs     = earliestPurgedTs_.load();
    s.lastPurgeTs          = lastPurgeTs_.load();
    s.lastWriteMs          = lastWriteMs_.load();
    s.seqFirst             = seqFirst_.load();
    s.seqLast              = seqLast_.load();
    s.flushes              = flushes_.load();
    s.purgeUnsupported     = purgeUnsupported_.load();
    s.capacityUnsupported  = capacityUnsupported_.load();
    s.backend              = backendName();
    s.version              = TELEMETRY_STORE_VERSION;

    const std::uint64_t n = s.flushes;
    s.avgWriteMs = (n > 0) ? (totalWriteMs_.load() / static_cast<double>(n)) : 0.0;
    s.thresholdState = (consecutiveWriteFailures_.load() >= 3) ? "degraded" : "ok";
    return s;
}

const char* Store::backendName() const {
    return (backend_ != nullptr) ? backend_->name() : "none";
}

// ================================================================ 信封 → 记录
bool Store::fromEnvelope(const nlohmann::json& envelope, Record& out) {
    if (!envelope.is_object()) return false;

    auto itType = envelope.find("type");
    if (itType == envelope.end() || !itType->is_string()) return false;
    if (itType->get_ref<const std::string&>().empty()) return false;

    auto itData = envelope.find("data");
    if (itData == envelope.end() || !itData->is_object()) return false;

    auto itTs = envelope.find("ts");
    if (itTs == envelope.end() || !itTs->is_number()) return false;

    out.type = *itType;
    out.data = *itData;
    out.ts   = static_cast<Millis>(itTs->get<double>());
    // deviceId 不在信封顶层（信封只有三个字段）——从 data 里取；取不到就留给宿主填。
    // 模块**不猜**业务字段名（P1：只记事实，不做解释）。
    auto itDev = itData->find("deviceId");
    if (itDev != itData->end() && itDev->is_string()) {
        out.deviceId = itDev->get<std::string>();
    }
    out.seq = 0;  // 由 append 补齐
    return true;
}

}  // namespace telemetry_store

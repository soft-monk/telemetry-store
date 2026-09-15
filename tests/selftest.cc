// tests/selftest.cc · 零依赖自测
//
// 覆盖需求条目（详见 docs/需求/遥测留存需求专篇.md）：
//   WRT 写入 · QRY 查询 · RPL 回放 · GAP 补发 · RET 保留 · ADP 注入 · OBS 可观测 · NFR 非功能
//
// 运行： selftest            → 跑全部
//        selftest --list     → 只列用例名
//        selftest <片段>      → 只跑名字含该片段的用例
//
// 用例 → 需求追溯（全部确定性：不依赖真实时间/外部服务，整套 < 30s）：
//   wrt01_append_is_fast_with_slow_backend            TLM-WRT-01 · TLM-NFR-02
//   wrt02_appendbatch_matches_single_appends          TLM-WRT-02
//   wrt03_batch_flush_reduces_backend_calls           TLM-WRT-03
//   wrt04_ts_comes_from_caller                        TLM-WRT-04
//   wrt05_seq_is_monotonic_without_gaps               TLM-WRT-05
//   wrt06_buffer_overflow_is_bounded_and_counted      TLM-WRT-06 · TLM-OBS-02
//   wrt07_flush_and_destructor_deliver_all            TLM-WRT-07
//   wrt08_duplicate_key_last_write_wins               TLM-WRT-08
//   qry01_range_is_half_open                          TLM-QRY-01
//   qry02_single_device_exact_query                   TLM-QRY-02
//   qry03_multi_device_sorted_stable                  TLM-QRY-03
//   qry04_cursor_pagination_no_loss_no_dup            TLM-QRY-04 · TLM-NFR-04(上限可配)
//   qry04_limit_above_max_is_rejected_not_truncated   TLM-QRY-04 · TLM-OBS-02(超限拒绝计数) · P4
//   qry05_coverage_meta_and_truncated_by_retention    TLM-QRY-05
//   rpl01_replay_timeline_matches_query               TLM-RPL-01 · TLM-RPL-02
//   rpl03_multi_device_merged_timeline                TLM-RPL-03
//   rpl04_every_nth_sampling                          TLM-RPL-04
//   gap01_since_is_strictly_greater                   TLM-GAP-01
//   gap02_per_device_watermarks                       TLM-GAP-02
//   gap03_since_pagination_with_cursor                TLM-GAP-03
//   gap04_gaps_detects_hole                           TLM-GAP-04（单设备 + 阈值口径）
//   gap04_multi_device_gaps_are_reported              TLM-GAP-04（多设备逐台报缺口）
//   ret01_ttl_purges_old_keeps_new                    TLM-RET-01
//   ret03_purge_counters_visible                      TLM-RET-03
//   ret04_backend_without_purge_degrades_honestly     TLM-RET-04
//   ret05_capacity_limit_trims_oldest                 TLM-RET-05
//   adp01_swap_backend_only                           TLM-ADP-01
//   adp02_fake_clock_drives_retention                 TLM-ADP-02
//   adp04_from_envelope_pure_function                 TLM-ADP-04
//   obs01_status_fields_match_input                   TLM-OBS-01
//   obs02_backend_failure_counted_and_recovers        TLM-OBS-02（写入失败）
//   obs02_purge_failure_is_counted_and_retried        TLM-OBS-02（清理失败）
//   obs03_log_sink_receives_overflow                  TLM-OBS-03
//   nfr05_concurrent_append_and_query                 TLM-NFR-05
//   nfr06_seq_gap_detectable_after_loss               TLM-NFR-06
//   segfile_restart_keeps_data                        TLM-WRT-07 · TLM-NFR-06 · TLM-ADP-01
//   segfile_purge_deletes_whole_segments              TLM-RET-01/03/04 · TLM-QRY-05
//   segfile_cursor_pagination_continues_across_segs   TLM-QRY-04
//   segfile_same_range_query_is_repeatable            TLM-QRY-02（读缓存重复命中回归）
//
// ★ "已知偏差"约定（checkKnown）
//   有些断言按需求/设计**应当成立**，但当前实现不满足。它们用 checkKnown 登记：
//   会打印成 XFAIL、单独计数，但**不改变退出码**——这样本用例集既能作为交付门禁
//   （0 失败），又能把实现偏差逐条钉在案上。实现修好后会自动显示为"已知偏差已修复"。
//   修完请把对应的 checkKnown 改成 check（升为硬断言）。
//   本文件末尾列了全部已知偏差（文件 / 行号 / 复现 / 期望）。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "telemetry_store/backends/memory_backend.h"
#include "telemetry_store/backends/segment_file.h"
#include "telemetry_store/console.h"
#include "telemetry_store/store.h"

using namespace telemetry_store;

namespace {

// ---------------------------------------------------------------- 断言框架
int g_checks = 0;       // 全部断言（硬 + 已知偏差）
int g_failed = 0;       // 硬断言失败数（决定退出码）
int g_knownOpen = 0;    // 登记在案的实现偏差（当前不成立）
int g_knownFixed = 0;   // 曾经的偏差，现在成立了
std::string g_case;
std::vector<std::string> g_failures;
std::vector<std::string> g_knownNotes;
std::atomic<int> g_threadErrors{0};  // 并发用例专用：子线程只累加，主线程统一断言

void check(bool ok, const std::string& what) {
    ++g_checks;
    if (!ok) {
        ++g_failed;
        g_failures.push_back("[" + g_case + "] " + what);
    }
}

template <typename A, typename B>
void checkEq(const A& got, const B& want, const std::string& what) {
    ++g_checks;
    if (!(got == want)) {
        ++g_failed;
        std::ostringstream oss;
        oss << "[" << g_case << "] " << what << "  期望=" << want << " 实际=" << got;
        g_failures.push_back(oss.str());
    }
}

/// 已知实现偏差：断言按需求应当成立，但当前实现不满足 —— 记为 XFAIL（不计失败）。
/// 当前套件里没有在案的偏差（全部已修复并升为硬断言）；这个机制留着给后续发现用：
/// 新增偏差时用 checkKnown 登记，修好后把那一行改成 check。
[[maybe_unused]] void checkKnown(bool ok, const std::string& what) {
    ++g_checks;
    if (ok) {
        ++g_knownFixed;
        return;
    }
    ++g_knownOpen;
    g_knownNotes.push_back("[" + g_case + "] " + what);
}

// ---------------------------------------------------------------- 测试用依赖
/// 记录调用的后端：用于断言"批量刷盘"与"失败重试"这类行为。
class SpyBackend : public IStorageBackend {
public:
    explicit SpyBackend(std::size_t failTimes = 0) : failTimes_(failTimes) {}

    bool append(RecordBatch batch) override {
        std::lock_guard<std::mutex> lk(mtx_);
        ++appendCalls;
        if (failTimes_ > 0) {
            --failTimes_;
            return false;
        }
        for (const auto& r : batch) {
            rows_.push_back(r);
            ++appendedRows;
        }
        return true;
    }

    std::size_t read(const Query& q, const Cursor* beginAfter,
                     const RecordVisitor& visit) override {
        std::lock_guard<std::mutex> lk(mtx_);
        ++readCalls;
        std::size_t matched = 0;
        std::vector<Record> snap = rows_;
        for (const auto& r : snap) {
            if (r.ts < q.fromTs || r.ts >= q.toTs) continue;
            if (!q.devices.empty() &&
                std::find(q.devices.begin(), q.devices.end(), r.deviceId) == q.devices.end()) {
                continue;
            }
            if (!q.types.empty() &&
                std::find(q.types.begin(), q.types.end(), r.type) == q.types.end()) {
                continue;
            }
            if (beginAfter != nullptr && beginAfter->valid) {
                const auto a = std::make_tuple(r.ts, r.deviceId, r.type);
                const auto b = std::make_tuple(beginAfter->ts, beginAfter->deviceId, beginAfter->type);
                if (!(a > b)) continue;
            }
            ++matched;
            if (!visit(r)) break;
        }
        return matched;
    }

    bool purge(Millis cutoffTs) override {
        std::lock_guard<std::mutex> lk(mtx_);
        ++purgeCalls;
        if (!canPurgeFlag) return false;
        if (failPurge) return false;  // 声明"支持删除"但执行失败：TLM-OBS-02 的清理失败类
        const std::size_t before = rows_.size();
        rows_.erase(std::remove_if(rows_.begin(), rows_.end(),
                                   [cutoffTs](const Record& r) { return r.ts < cutoffTs; }),
                    rows_.end());
        purgedRows += (before - rows_.size());
        return true;
    }

    bool canPurge() const override { return canPurgeFlag; }
    Millis earliestRetainedTs() const override {
        std::lock_guard<std::mutex> lk(mtx_);
        Millis best = 0;
        for (const auto& r : rows_) {
            if (best == 0 || r.ts < best) best = r.ts;
        }
        return best;
    }
    std::size_t approxRows() const override {
        std::lock_guard<std::mutex> lk(mtx_);
        return rows_.size();
    }
    const char* name() const override { return "spy"; }

    /// 故障注入：让接下来的 n 次 append 全部失败（0 = 恢复正常）。
    void setFailTimes(std::size_t n) {
        std::lock_guard<std::mutex> lk(mtx_);
        failTimes_ = n;
    }

    mutable std::mutex mtx_;
    std::vector<Record> rows_;
    int  appendCalls = 0;
    std::size_t appendedRows = 0;
    int  readCalls = 0;
    int  purgeCalls = 0;
    std::size_t purgedRows = 0;
    bool canPurgeFlag = true;
    bool failPurge = false;  ///< true = 声明支持删除但每次 purge 都失败（故障注入）

private:
    std::size_t failTimes_ = 0;
};

/// 慢后端：每次 append 睡 50ms，用来验证"append 不与后端同步"（TLM-WRT-01）。
/// 存储行为直接复用 MemoryBackend（它就是 IStorageBackend 的一个实现，可被再实现）。
class SlowBackend : public MemoryBackend {
public:
    explicit SlowBackend(std::int64_t sleepMs) : sleepMs_(sleepMs) {}

    bool append(RecordBatch batch) override {
        appendCalls.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs_));
        return MemoryBackend::append(batch);
    }

    std::atomic<int> appendCalls{0};

private:
    std::int64_t sleepMs_;
};

/// 记录日志的 Sink（TLM-OBS-03）。
class CapturingLog : public ILogSink {
public:
    void log(const std::string& category, const nlohmann::json& payload) override {
        std::lock_guard<std::mutex> lk(mtx_);
        entries.emplace_back(category, payload);
    }
    bool hasEvent(const std::string& ev) const {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const auto& e : entries) {
            if (e.second.value("event", std::string()) == ev) return true;
        }
        return false;
    }
    mutable std::mutex mtx_;
    std::vector<std::pair<std::string, nlohmann::json>> entries;
};

// ---------------------------------------------------------------- 用例登记
struct TestCase {
    std::string name;
    void (*fn)();
};
std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}
struct Registrar {
    Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};
#define TEST(name)                                        \
    void name();                                          \
    Registrar reg_##name(#name, &name);                   \
    void name()

// ---------------------------------------------------------------- 小工具
StoreOptions fastOptions(std::size_t flushRecords = 1000) {
    StoreOptions o;
    o.flush.maxRecords = flushRecords;
    o.flush.maxDelayMs = 0;
    o.enableSweeper = false;  // 测试里全手动驱动，保证确定性
    o.queryFlushesFirst = true;
    return o;
}

nlohmann::json pos(double lng, double lat) {
    return nlohmann::json{{"lng", lng}, {"lat", lat}};
}

/// 固定的假"现在"（≈2023-11-14T22:13:20Z）：保留清理用例全部围绕它算，确定性。
const Millis kNow = 1'700'000'000'000LL;

/// 主键字符串，用于集合校验"无重复无遗漏"。
std::string keyOf(const Record& r) {
    return r.deviceId + "@" + std::to_string(r.ts) + "/" + r.type;
}

/// 逐字段比较（含 data，含 seq）。
bool sameRecord(const Record& a, const Record& b) {
    return a.deviceId == b.deviceId && a.type == b.type && a.ts == b.ts && a.seq == b.seq &&
           a.data == b.data;
}

/// 是否严格按 (ts, deviceId, type) 升序 —— 等价于"有序 + 无重复点"。
bool sortedByKey(const std::vector<Record>& rows) {
    for (std::size_t i = 1; i < rows.size(); ++i) {
        const auto prev = std::make_tuple(rows[i - 1].ts, rows[i - 1].deviceId, rows[i - 1].type);
        const auto cur  = std::make_tuple(rows[i].ts, rows[i].deviceId, rows[i].type);
        if (!(prev < cur)) return false;
    }
    return true;
}

bool sameRowKeys(const std::vector<Record>& a, const std::vector<Record>& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (keyOf(a[i]) != keyOf(b[i])) return false;
    }
    return true;
}

// 安全取值（空序列给兜底值）——断言失败时不该把测试进程带崩：
// 有调用方不加保护地 front()/back()/[1]，一旦上游返回空结果就是访问越界。
Millis firstTs(const std::vector<Record>& v) { return v.empty() ? Millis(-1) : v.front().ts; }
Millis lastTs(const std::vector<Record>& v) { return v.empty() ? Millis(-1) : v.back().ts; }
Millis nthTs(const std::vector<Record>& v, std::size_t i) {
    return i < v.size() ? v[i].ts : Millis(-1);
}
std::string firstDevice(const std::vector<Record>& v) {
    return v.empty() ? std::string("<空>") : v.front().deviceId;
}
std::string firstType(const std::vector<Record>& v) {
    return v.empty() ? std::string("<空>") : v.front().type;
}
nlohmann::json firstData(const std::vector<Record>& v) {
    return v.empty() ? nlohmann::json() : v.front().data;
}
nlohmann::json lastData(const std::vector<Record>& v) {
    return v.empty() ? nlohmann::json() : v.back().data;
}
std::uint64_t firstSeq(const std::vector<Record>& v) { return v.empty() ? 0 : v.front().seq; }
std::uint64_t lastSeq(const std::vector<Record>& v) { return v.empty() ? 0 : v.back().seq; }

/// 临时目录（RAII）：用例结束即删，失败也不残留（文件后端用例专用）。
struct TempDir {
    std::filesystem::path path;

    explicit TempDir(const std::string& tag) {
        static std::atomic<unsigned> seq{0};
        std::error_code ec;
        std::filesystem::path base = std::filesystem::temp_directory_path(ec);
        if (ec || base.empty()) base = std::filesystem::current_path(ec);
        path = base / ("tlm_selftest_" + tag + "_" + std::to_string(steadyMs()) + "_" +
                       std::to_string(seq.fetch_add(1)));
        std::filesystem::remove_all(path, ec);
        std::filesystem::create_directories(path, ec);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

}  // namespace

// ================================================================ R1/R2 · 接口与写入
TEST(smoke_write_query_replay_since) {
    Store store(fastOptions());
    checkEq(store.backendName(), std::string("memory"), "默认后端是零依赖内存后端");
    checkEq(store.status().version, std::string(TELEMETRY_STORE_VERSION), "版本号可读");

    for (int i = 0; i < 10; ++i) {
        store.append("uav-1", "uav.pos", 1000 + i, pos(116.4 + i * 0.001, 39.9));
    }
    store.flush();
    checkEq(store.status().persisted, std::uint64_t(10), "10 条全部落库");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs = 1000;
    q.toTs = 1010;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(10), "区间查询命中 10 条");

    SinceRequest sr;
    sr.devices = {"uav-1"};
    sr.lastSeenTs = 1004;  // 严格大于：应返回 1005..1009 共 5 条
    Timeline tl = store.since(sr);
    checkEq(tl.samples.size(), std::size_t(5), "补发只给水位之后的记录");
    checkEq(firstTs(tl.samples), Millis(1005), "补发首条 = 水位+1");
}

// ================================================================ TLM-WRT · 写入
TEST(wrt01_append_is_fast_with_slow_backend) {
    // TLM-WRT-01 / TLM-NFR-02：append 调用即返回，不与存储后端同步。
    // 验收口径：后端 50ms/批时，append 的 P95 仍 ≤ 1ms（P100 允许包含一次刷盘）。
    SlowBackend backend(50);
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(1000), backend, clock, policy);  // 每 1000 条刷一次 → 只会有 1 次慢调用

    const int N = 1000;
    std::vector<double> costMs;
    costMs.reserve(static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
        const auto t1 = std::chrono::steady_clock::now();
        costMs.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    }
    std::sort(costMs.begin(), costMs.end());
    const double p95 = costMs[static_cast<std::size_t>(N) * 95 / 100];

    checkEq(backend.appendCalls.load(), 1, "1000 条按阈值折成 1 次后端写入（不是 1000 次）");
    check(costMs.back() >= 50.0, "触发刷盘的那一次确实被后端拖慢（>=50ms），用例不是空转");
    check(p95 <= 1.0, "后端 50ms/批时 append 的 P95 ≤ 1ms，实际=" + std::to_string(p95) + "ms");
    check(costMs[static_cast<std::size_t>(N) / 2] <= 1.0, "append 中位数 ≤ 1ms");
    store.flush();
    checkEq(store.status().persisted, std::uint64_t(N), "1000 条一条不少地落库");
}

TEST(wrt02_appendbatch_matches_single_appends) {
    // TLM-WRT-02：一次交 1000 条与逐条交 1000 条，落库结果完全一致。
    const int N = 1000;
    std::vector<Record> recs;
    recs.reserve(static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i) {
        Record r;
        r.deviceId = "uav-" + std::to_string(i % 3);
        r.type     = "uav.pos";
        r.ts       = 5000 + i;
        r.data     = pos(i * 0.001, i * 0.002);
        recs.push_back(r);
    }

    Store oneByOne(fastOptions(1000));
    Store batch(fastOptions(1000));
    for (const auto& r : recs) oneByOne.append(r);
    batch.appendBatch(recs);
    oneByOne.flush();
    batch.flush();

    checkEq(oneByOne.status().persisted, std::uint64_t(N), "逐条写：1000 条落库");
    checkEq(batch.status().persisted, std::uint64_t(N), "批量写：1000 条落库");
    checkEq(oneByOne.status().appended, batch.status().appended, "两种方式 appended 一致");
    checkEq(oneByOne.status().seqFirst, batch.status().seqFirst, "两种方式首发序号一致");
    checkEq(oneByOne.status().seqLast, batch.status().seqLast, "两种方式末发序号一致");

    Query q;
    q.fromTs = 5000;
    q.toTs   = 5000 + N;
    q.limit  = 2 * N;
    QueryResult ra = oneByOne.query(q);
    QueryResult rb = batch.query(q);
    checkEq(ra.rows.size(), std::size_t(N), "逐条写：查到 1000 条");
    checkEq(rb.rows.size(), std::size_t(N), "批量写：查到 1000 条");
    bool identical = (ra.rows.size() == rb.rows.size());
    for (std::size_t i = 0; identical && i < ra.rows.size(); ++i) {
        if (!sameRecord(ra.rows[i], rb.rows[i])) identical = false;
    }
    check(identical, "两种方式返回的记录逐条一致（含 seq 与 data）");
}

TEST(wrt03_batch_flush_reduces_backend_calls) {
    // TLM-WRT-03：批量缓冲写 —— 5000 条输入，后端写入调用次数应远小于输入条数（≤ 10）。
    SpyBackend backend;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(1000), backend, clock, policy);

    const int N = 5000;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    store.flush();

    checkEq(backend.appendedRows, std::size_t(N), "5000 条全部到达后端");
    check(backend.appendCalls <= 10,
          "后端写入调用次数 ≤ 10，实际=" + std::to_string(backend.appendCalls));
    checkEq(backend.appendCalls, 5, "5000 条 / 每批 1000 条 = 恰好 5 次");
    checkEq(store.status().appended, std::uint64_t(N), "status：收到 5000 条");
    checkEq(store.status().persisted, std::uint64_t(N), "status：落库 5000 条");
    checkEq(store.status().flushes, std::uint64_t(5), "status：刷盘 5 次");
}

TEST(wrt04_ts_comes_from_caller) {
    // TLM-WRT-04：ts 由调用方传入；模块不自己打时间（IClock 只用于保留/统计）。
    MemoryBackend backend;
    FakeClock clock(kNow);  // "现在" = 2023-11，与下面写入的 ts 毫无关系
    KeepForeverPolicy policy;
    Store store(fastOptions(1000), backend, clock, policy);

    store.append("uav-1", "uav.pos", 1234, pos(1, 2));  // 很久以前的 ts
    store.append("uav-1", "uav.pos", 0, pos(3, 4));     // ts=0 也如实存储（设计 §4.6）
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1234;
    q.toTs    = 1235;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(1), "按调用方给的旧 ts 命中");
    checkEq(firstTs(r.rows), Millis(1234), "ts 原样保留，没有被 IClock 的现在替换");

    Query q0;
    q0.devices = {"uav-1"};
    q0.fromTs  = 0;
    q0.toTs    = 1;
    checkEq(store.query(q0).rows.size(), std::size_t(1), "ts=0 被如实接受（不打当前时间兜底）");
    checkEq(backend.approxRows(), std::size_t(2), "后端里恰好 2 条，模块没有额外造记录");
}

TEST(wrt05_seq_is_monotonic_without_gaps) {
    // TLM-WRT-05：模块为每条记录打进程内单调递增序号；无丢弃时序号连续（P3 的缺口证据）。
    Store store(fastOptions(500));  // 中途会刷盘，序号仍须连续
    const std::uint64_t N = 2000;
    for (std::uint64_t i = 0; i < N; ++i) {
        store.append("uav-1", "uav.pos", 1000 + static_cast<Millis>(i), pos(1.0, 2.0));
    }
    store.flush();

    Status st = store.status();
    checkEq(st.appended, N, "收到 2000 条");
    checkEq(st.seqFirst, std::uint64_t(1), "首序号 = 1");
    checkEq(st.seqLast, N, "末序号 = 2000（单调递增、不跳号）");
    checkEq(st.seqLast - st.seqFirst + 1, st.appended, "无丢弃时 序号跨度 == 收到条数（无缺口）");
    checkEq(st.droppedByOverflow, std::uint64_t(0), "没有发生丢弃");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1000;
    q.toTs    = 1000 + static_cast<Millis>(N);
    q.limit   = 2 * static_cast<std::size_t>(N);
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(N), "查询命中 2000 条");
    bool seqContiguous = !r.rows.empty();
    std::uint64_t expect = 1;
    for (const auto& rec : r.rows) {
        if (rec.seq != expect) seqContiguous = false;
        ++expect;
    }
    check(seqContiguous, "结果里的 seq 从 1 连续到 2000（序号可查）");
}

TEST(wrt06_buffer_overflow_is_bounded_and_counted) {
    // TLM-WRT-06：缓冲有界（可配）+ 达到上限的行为可配 + **禁止无计数的静默丢弃**（P2）。
    // 本用例分两种后端状态，覆盖两条路径：
    //   A 后端健在：满时先 flush 腾空间 → 一条不丢（比"淘汰最旧"更保守）；
    //   B 后端拒收：腾不出空间 → DropOldest 真正生效，丢弃计数 == 实际丢失条数。
    StoreOptions o;
    o.flush.maxRecords   = 0;  // 关掉条数阈值：只有"满"这一条路径会刷
    o.flush.maxDelayMs   = 0;
    o.bufferCapacity     = 100;
    o.onFull             = BufferFullPolicy::DropOldest;
    o.enableSweeper      = false;

    const int N = 1000;
    {
        Store store(o);  // 默认内存后端（健康）
        for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
        Status st = store.status();
        checkEq(st.appended, std::uint64_t(N), "A：收到 1000 条");
        check(st.buffered <= 100, "A：缓冲深度始终不超过容量 100，实际=" + std::to_string(st.buffered));
        checkEq(st.appended, st.persisted + st.buffered + st.droppedByOverflow,
                "A：记账恒等式 收到 = 落库 + 缓冲 + 丢弃（绝不静默丢数据）");
        checkEq(st.persisted + st.buffered, st.appended, "A：后端健在时一条都没丢");
        store.flush();
        checkEq(store.status().persisted, std::uint64_t(N), "A：最终 1000 条全部落库");
        checkEq(store.status().buffered, std::uint64_t(0), "A：flush 后缓冲清空");
    }
    {
        SpyBackend backend;
        backend.setFailTimes(100000000);  // 后端一直拒收 → 缓冲永远腾不空
        FakeClock clock(kNow);
        KeepForeverPolicy policy;
        Store store(o, backend, clock, policy);
        for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
        Status st = store.status();
        checkEq(st.appended, std::uint64_t(N), "B：收到 1000 条");
        check(st.buffered <= 100, "B：缓冲仍然有界，实际=" + std::to_string(st.buffered));
        check(st.droppedByOverflow > 0,
              "B：腾不出空间时 DropOldest 生效并计数，实际=" + std::to_string(st.droppedByOverflow));
        const std::uint64_t lost = st.appended - st.persisted - st.buffered;
        checkEq(st.droppedByOverflow, lost, "B：丢弃计数 == 实际丢失条数（不多报不少报）");
        checkEq(st.persisted, std::uint64_t(0), "B：后端一直失败 → 一条也没落库");
        check(st.backendWriteFailures > 0, "B：后端写入失败被单独计数");
        checkEq(st.appended, st.droppedByOverflow + st.buffered + st.persisted, "B：记账恒等式成立");
        Query qb;
        qb.devices = {"uav-1"};
        qb.fromTs  = 0;
        qb.toTs    = 100'000;
        QueryResult rb = store.query(qb);
        check(rb.partial, "B：丢过数据（序号缺口）→ 查询结果 partial=true，不谎报完整");
        checkEq(rb.rows.size(), std::size_t(0), "B：后端全拒 → 查询拿不到内容");
    }
}

TEST(wrt07_flush_and_destructor_deliver_all) {
    // TLM-WRT-07：flush() 返回即已交付；析构等价于 flush()（缓冲全部交付后端）。
    SpyBackend backend;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    StoreOptions o = fastOptions(0);  // 阈值全 0 = 只能手动刷盘
    {
        Store store(o, backend, clock, policy);
        store.append("uav-1", "uav.pos", 1000, pos(1, 1));
        checkEq(backend.appendedRows, std::size_t(0), "阈值关闭时 append 不会自行落库");
        checkEq(store.status().buffered, std::uint64_t(1), "数据在缓冲里");
        store.flush();
        checkEq(backend.appendedRows, std::size_t(1), "flush() 返回即已交付后端");
        checkEq(store.status().buffered, std::uint64_t(0), "flush() 后缓冲为空");
        checkEq(store.status().persisted, std::uint64_t(1), "落库计数 +1");

        for (int i = 0; i < 7; ++i) store.append("uav-1", "uav.pos", 1001 + i, pos(i, i));
        checkEq(store.status().buffered, std::uint64_t(7), "再写 7 条，全部还在缓冲里");
    }  // ← 析构：等价于 flush()
    checkEq(backend.appendedRows, std::size_t(8), "Store 析构后数据仍在注入的后端里（8 条）");
    checkEq(backend.rows_.size(), std::size_t(8), "后端里确实有 8 条记录");
}

TEST(wrt08_duplicate_key_last_write_wins) {
    // TLM-WRT-08：同一 (deviceId, ts, type) 写两次 → query 只返回一条且是**后写入**的。
    Store store(fastOptions(100));
    store.append("uav-1", "uav.pos", 7000, pos(1.0, 1.0));                      // 先写
    store.append("uav-1", "uav.pos", 7000, pos(2.0, 2.0));                      // 后写（同主键）
    store.append("uav-1", "link.quality", 7000, nlohmann::json{{"rssi", -70}}); // 同 ts 不同类型 = 不同主键
    store.append("uav-2", "uav.pos", 7000, pos(3.0, 3.0));                      // 同 ts 不同设备 = 不同主键
    store.flush();

    Query q;
    q.fromTs = 7000;
    q.toTs   = 7001;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(3), "重复主键被折叠 → 只剩 3 条不同主键");
    checkEq(store.status().persisted, std::uint64_t(4), "后端仍然收到 4 条（去重发生在读路径 D7）");

    const Record* dup = nullptr;
    for (const auto& rec : r.rows) {
        if (rec.deviceId == "uav-1" && rec.type == "uav.pos") dup = &rec;
    }
    check(dup != nullptr, "重复主键的那条还在（没有被整条删掉）");
    if (dup != nullptr) {
        checkEq(dup->data, pos(2.0, 2.0), "保留的是后写入的内容（后写胜）");
        checkEq(dup->seq, std::uint64_t(2), "保留的是序号更大的那一条");
    }
}

// ================================================================ TLM-QRY · 查询
TEST(qry01_range_is_half_open) {
    // TLM-QRY-01：区间 [fromTs, toTs) 左闭右开，结果按 ts 升序。
    Store store(fastOptions(10000));
    for (int i = 0; i < 1000; ++i) store.append("uav-1", "uav.pos", i, pos(i, i));
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 100;
    q.toTs    = 200;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(100), "query(100,200) 恰好 100 条（含 100 不含 200）");
    checkEq(firstTs(r.rows), Millis(100), "首条 ts = 100（左闭）");
    checkEq(lastTs(r.rows), Millis(199), "末条 ts = 199（右开）");
    checkEq(r.coveredFromTs, Millis(100), "coveredFromTs = 100");
    checkEq(r.coveredToTs, Millis(199), "coveredToTs = 199");
    check(sortedByKey(r.rows), "结果按 (ts, deviceId, type) 升序");
    checkEq(r.hasMore, false, "100 条未超上限 → hasMore=false");

    Query empty;
    empty.devices = {"uav-1"};
    empty.fromTs  = 200;
    empty.toTs    = 200;
    checkEq(store.query(empty).rows.size(), std::size_t(0), "toTs==fromTs → 空结果（设计 §4.6）");

    Query reversed;
    reversed.devices = {"uav-1"};
    reversed.fromTs  = 300;
    reversed.toTs    = 200;
    QueryResult rr = store.query(reversed);
    checkEq(rr.rows.size(), std::size_t(0), "toTs<fromTs → 空结果，不报错");
    checkEq(rr.partial, false, "非法区间不算 partial");

    Query a;
    a.devices = {"uav-1"};
    a.fromTs  = 0;
    a.toTs    = 500;
    Query b;
    b.devices = {"uav-1"};
    b.fromTs  = 500;
    b.toTs    = 1000;
    checkEq(store.query(a).rows.size() + store.query(b).rows.size(), std::size_t(1000),
            "相邻两段拼接 = 1000（半开区间不重不漏）");
}

TEST(qry02_single_device_exact_query) {
    // TLM-QRY-02：单设备精确查询（主键路径：设备 ID + 时间区间），不得混入别的设备。
    Store store(fastOptions(1000));
    for (int d = 0; d < 20; ++d) {
        const std::string dev = "dev-" + std::to_string(d);
        for (int i = 0; i < 100; ++i) {
            store.append(dev, "uav.pos", 1'000'000 + i * 1000, pos(d, i));
        }
    }
    store.flush();

    Query q;
    q.devices = {"dev-7"};
    q.fromTs  = 1'000'000 + 10 * 1000;
    q.toTs    = 1'000'000 + 20 * 1000;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(10), "只返回该设备该区间的 10 条");
    bool onlyTarget = true;
    for (const auto& rec : r.rows) {
        if (rec.deviceId != "dev-7") onlyTarget = false;
    }
    check(onlyTarget, "结果里没有别的设备的数据");
    checkEq(firstTs(r.rows), Millis(1'010'000), "首条 ts = 区间起点");
    checkEq(lastTs(r.rows), Millis(1'019'000), "末条 ts = 区间终点前 1ms");
    checkEq(r.scanned, std::size_t(10), "后端只吐出了 10 条候选（走了主键路径，不是全表扫）");

    Query byType;
    byType.devices = {"dev-7"};
    byType.types   = {"link.quality"};
    byType.fromTs  = 0;
    byType.toTs    = 2'000'000;
    checkEq(store.query(byType).rows.size(), std::size_t(0), "types 过滤生效（该设备没有这个类型）");
}

TEST(qry03_multi_device_sorted_stable) {
    // TLM-QRY-03：跨设备查询 —— 按 ts 升序、同 ts 按 deviceId 稳定排序。
    Store store(fastOptions(1000));
    // 故意按打乱的顺序写入，验证排序不依赖写入顺序
    store.append("dev-b", "uav.pos", 5001, pos(2, 2));
    store.append("dev-a", "uav.pos", 5001, pos(1, 1));
    store.append("dev-b", "uav.pos", 5000, pos(2, 0));
    store.append("dev-a", "uav.pos", 5000, pos(1, 0));
    store.append("dev-c", "uav.pos", 5000, pos(3, 0));
    store.flush();

    Query q;
    q.fromTs = 5000;
    q.toTs   = 5002;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(5), "跨设备查询返回 5 条（与写入条数一致）");

    const std::vector<std::pair<Millis, std::string>> want = {
        {5000, "dev-a"}, {5000, "dev-b"}, {5000, "dev-c"}, {5001, "dev-a"}, {5001, "dev-b"}};
    bool orderOk = (r.rows.size() == want.size());
    for (std::size_t i = 0; orderOk && i < want.size(); ++i) {
        orderOk = (r.rows[i].ts == want[i].first) && (r.rows[i].deviceId == want[i].second);
    }
    check(orderOk, "顺序 = ts 升序，同 ts 按设备 ID 升序（稳定可复现）");
    check(sortedByKey(r.rows), "结果键严格递增（无重复点）");

    QueryResult again = store.query(q);
    check(sameRowKeys(r.rows, again.rows), "同一查询重复执行，顺序与内容一致");

    Query two;
    two.devices = {"dev-a", "dev-c"};
    two.fromTs  = 5000;
    two.toTs    = 5002;
    checkEq(store.query(two).rows.size(), std::size_t(3), "devices=[dev-a,dev-c] → 3 条");
}

TEST(qry04_cursor_pagination_no_loss_no_dup) {
    // TLM-QRY-04：单次上限 10000；超出时返回游标续查，**禁止静默截断**（P4）。
    Store store(fastOptions(5000));
    const int N = 25000;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1'000'000 + i, pos(i, i));
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1'000'000;
    q.toTs    = 1'000'000 + N;
    q.limit   = 10000;

    QueryResult p1 = store.query(q);
    checkEq(p1.rows.size(), std::size_t(10000), "第 1 页 10000 条（达到上限）");
    checkEq(p1.hasMore, true, "第 1 页 hasMore=true（截断必须明说）");
    checkEq(p1.cursor.valid, true, "第 1 页带回有效游标");
    checkEq(p1.cursor.ts, Millis(1'000'000 + 9999), "游标 ts = 本页最后一条的 ts");
    checkEq(p1.cursor.deviceId, std::string("uav-1"), "游标 deviceId = 本页最后一条的 deviceId");
    checkEq(p1.cursor.type, std::string("uav.pos"), "游标 type = 本页最后一条的 type");

    QueryResult p2 = store.queryPage(q, p1.cursor);
    checkEq(p2.rows.size(), std::size_t(10000), "第 2 页 10000 条");
    checkEq(p2.hasMore, true, "第 2 页 hasMore=true");
    checkEq(firstTs(p2.rows), Millis(1'000'000 + 10000), "第 2 页从第 10001 条接上（无重复）");

    QueryResult p3 = store.queryPage(q, p2.cursor);
    checkEq(p3.rows.size(), std::size_t(5000), "第 3 页 5000 条（取完）");
    checkEq(p3.hasMore, false, "第 3 页 hasMore=false");
    checkEq(lastTs(p3.rows), Millis(1'000'000 + N - 1), "最后一条 = 区间内最晚 ts");

    std::set<std::string> keys;
    std::size_t total = 0;
    const QueryResult* pages[3] = {&p1, &p2, &p3};
    for (const QueryResult* page : pages) {
        total += page->rows.size();
        for (const auto& rec : page->rows) keys.insert(keyOf(rec));
    }
    checkEq(total, std::size_t(N), "三页合计 25000 条（无遗漏）");
    checkEq(keys.size(), std::size_t(N), "25000 个唯一主键（无重复）");
    check(sortedByKey(p1.rows) && sortedByKey(p2.rows) && sortedByKey(p3.rows), "每页内部仍按 ts 升序");

    // 满页扫描截断：也一样要如实标 partial，并且不把"截断"算成"拒绝执行"（设计 §4.6）
    Query qSmall = q;
    qSmall.limit = 10;
    QueryResult rs = store.query(qSmall);
    checkEq(rs.rows.size(), std::size_t(10), "limit=10 → 只返回 10 条");
    checkEq(rs.hasMore, true, "命中扫描上限也返回游标（不静默截断）");
    checkEq(rs.partial, true, "命中扫描上限 → partial=true（结果可能不完整）");
    checkEq(store.status().rejectedByLimit, std::uint64_t(0),
            "截断 + 游标不算'拒绝执行'，rejectedByLimit 不计数（设计 §4.6 口径）");
}

TEST(qry04_limit_above_max_is_rejected_not_truncated) {
    // TLM-QRY-04 + P4（越界要明说）+ TLM-OBS-02（"查询超限拒绝"要计数）：
    // 调用方要的条数**超过本 Store 的上限**时，必须拒绝执行并说明原因，而不是悄悄少给。
    StoreOptions o = fastOptions(1000);
    o.maxRowsPerQuery = 10000;
    Store store(o);
    for (int i = 0; i < 100; ++i) store.append("uav-1", "uav.pos", 1'000'000 + i, pos(i, i));
    store.flush();

    Query ok;
    ok.devices = {"uav-1"};
    ok.fromTs  = 1'000'000;
    ok.toTs    = 1'000'100;
    ok.limit   = 10000;  // == 上限：允许
    QueryResult rok = store.query(ok);
    checkEq(rok.rejected, false, "limit == maxRowsPerQuery：正常执行");
    checkEq(rok.rows.size(), std::size_t(100), "上限之内结果完整");
    checkEq(store.status().rejectedByLimit, std::uint64_t(0), "还没有被拒绝过");

    Query over = ok;
    over.limit = 20000;  // > 上限：拒绝
    QueryResult rov = store.query(over);
    checkEq(rov.rejected, true, "limit > maxRowsPerQuery → rejected=true（明确拒绝，不静默截断）");
    checkEq(rov.rows.size(), std::size_t(0), "拒绝时不返回“半页”数据");
    check(rov.rejectReason.find("maxRowsPerQuery") != std::string::npos,
          "rejectReason 说明该怎么改，实际=\"" + rov.rejectReason + "\"");
    checkEq(store.status().rejectedByLimit, std::uint64_t(1), "超限拒绝被计数（OBS-02 第三类失败）");
    checkEq(store.status().backendWriteFailures, std::uint64_t(0), "拒绝查询不影响写入失败计数");
    checkEq(store.status().droppedByOverflow, std::uint64_t(0), "拒绝查询不影响溢出丢弃计数");

    Query page = ok;
    page.limit = 10;
    QueryResult p1 = store.query(page);
    checkEq(p1.rejected, false, "分页请求不被拒绝（要更多就翻页）");
    checkEq(p1.rows.size(), std::size_t(10), "第 1 页 10 条");
    checkEq(p1.hasMore, true, "带续查游标");
    QueryResult p2 = store.queryPage(page, p1.cursor);
    checkEq(firstTs(p2.rows), Millis(1'000'010), "游标续查接上（不受拒绝逻辑影响）");
    checkEq(store.status().rejectedByLimit, std::uint64_t(1), "分页不增加拒绝计数");
}

TEST(qry05_coverage_meta_and_truncated_by_retention) {
    // TLM-QRY-05：结果带覆盖范围元信息；数据被清理后必须如实标注 truncatedByRetention。
    MemoryBackend backend;
    FakeClock clock(10'500);
    UniformRetentionPolicy policy(1000);  // TTL 1s → cutoff = 10500-1000 = 9500
    Store store(fastOptions(1000), backend, clock, policy);

    store.append("uav-1", "uav.pos", 1000, pos(1, 1));     // 会被清理
    store.append("uav-1", "uav.pos", 10'000, pos(2, 2));   // 保留
    store.append("uav-1", "uav.pos", 10'400, pos(3, 3));   // 保留
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = 20'000;
    QueryResult before = store.query(q);
    checkEq(before.rows.size(), std::size_t(3), "清理前 3 条");
    checkEq(before.coveredFromTs, Millis(1000), "coveredFromTs = 实际返回的最早 ts");
    checkEq(before.coveredToTs, Millis(10'400), "coveredToTs = 实际返回的最晚 ts");

    // 硬口径：区间起点不早于最早数据 → 一定不能报"被清理过"。
    Query exact;
    exact.devices = {"uav-1"};
    exact.fromTs  = 1000;
    exact.toTs    = 20'000;
    checkEq(store.query(exact).truncatedByRetention, false, "区间起点 == 最早数据 → 不报截断");

    // 口径：从未清理过时，即使查询起点早于最早数据，也不算"被清理过"（需求原文：是否被清理过）。
    checkEq(before.truncatedByRetention, false,
            "从未清理过 → 起点早于最早数据也不报截断（不误报“被清理过”）");
    checkEq(before.partial, false, "落盘正常 → partial=false");

    RetentionReport rep = store.sweep();
    checkEq(rep.purgedRows, std::uint64_t(1), "sweep 清掉 1 条（ts=1000）");

    QueryResult after = store.query(q);
    checkEq(after.rows.size(), std::size_t(2), "清理后同区间只剩 2 条");
    checkEq(after.coveredFromTs, Millis(10'000), "最早 ts 抬升到保留边界");
    checkEq(after.truncatedByRetention, true, "查询区间早于保留边界 → 如实标注（P4）");

    Query inRange;
    inRange.devices = {"uav-1"};
    inRange.fromTs  = 10'000;
    inRange.toTs    = 11'000;
    checkEq(store.query(inRange).truncatedByRetention, false, "区间不早于保留边界 → 不误报");

    Query none;
    none.devices = {"uav-1"};
    none.fromTs  = 50'000;
    none.toTs    = 51'000;
    QueryResult nr = store.query(none);
    checkEq(nr.rows.size(), std::size_t(0), "无数据 → 空结果");
    checkEq(nr.coveredFromTs, Millis(0), "空结果 coveredFromTs = 0");
    checkEq(nr.coveredToTs, Millis(0), "空结果 coveredToTs = 0");
}

// ================================================================ TLM-RPL · 回放
TEST(rpl01_replay_timeline_matches_query) {
    // TLM-RPL-01/02：timeline = 有序样本 + 起止 + 总时长；与 query 同源同语义。
    Store store(fastOptions(1000));
    const Millis t0 = kNow;
    for (int i = 0; i < 10; ++i) store.append("uav-1", "uav.pos", t0 + i * 1000, pos(i, i));
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = t0;
    q.toTs    = t0 + 10'000;
    QueryResult r = store.query(q);
    ReplayRequest req;
    req.q = q;
    Timeline tl = store.replay(req);

    checkEq(r.rows.size(), std::size_t(10), "query 命中 10 条");
    checkEq(tl.samples.size(), r.rows.size(), "replay 与 query 样本数一致");
    bool same = (tl.samples.size() == r.rows.size());
    for (std::size_t i = 0; same && i < r.rows.size(); ++i) {
        if (!sameRecord(tl.samples[i], r.rows[i])) same = false;
    }
    check(same, "replay 与 query 返回的样本集合完全相同（含 seq 与 data）");
    checkEq(tl.fromTs, t0, "timeline.fromTs = 请求区间起点");
    checkEq(tl.durationMs, Millis(9000), "durationMs = 末样本 - 首样本 = 9000");
    check(tl.durationMs == lastTs(tl.samples) - firstTs(tl.samples),
          "durationMs 与样本首末差一致（可直接按 ts 差播放）");
    checkEq(tl.sampled, false, "不传稀疏参数 → sampled=false");
    checkEq(tl.sampleStep, std::size_t(1), "sampleStep = 1");
    checkEq(tl.hasMore, false, "10 条未超上限 → hasMore=false");
    checkEq(tl.truncatedByRetention, false, "没清理过 → truncatedByRetention=false");
    check(sortedByKey(tl.samples), "样本按 ts 升序");
}

TEST(rpl03_multi_device_merged_timeline) {
    // TLM-RPL-03：单设备 / 多设备回放；多设备按 ts 归并为**一条**统一时间轴。
    Store store(fastOptions(1000));
    for (int i = 0; i < 5; ++i) {
        for (const char* d : {"uav-a", "uav-b", "uav-c"}) {
            store.append(d, "uav.pos", 2000 + i * 100 + (d[4] - 'a'), pos(i, i));
        }
    }
    store.flush();

    ReplayRequest req;
    req.q.devices = {"uav-a", "uav-b", "uav-c"};
    req.q.fromTs  = 2000;
    req.q.toTs    = 3000;
    Timeline tl = store.replay(req);
    checkEq(tl.samples.size(), std::size_t(15), "3 台设备 × 5 条 = 15 个样本");
    check(sortedByKey(tl.samples), "多设备样本按 ts 全局有序（一条统一时间轴）");
    checkEq(tl.durationMs, lastTs(tl.samples) - firstTs(tl.samples), "durationMs = 首末样本之差");
    std::set<std::string> devs;
    for (const auto& rec : tl.samples) devs.insert(rec.deviceId);
    checkEq(devs.size(), std::size_t(3), "3 台设备的数据都在同一条时间轴上");

    ReplayRequest one;
    one.deviceId = "uav-b";
    one.q.fromTs = 2000;
    one.q.toTs   = 3000;
    Timeline tlOne = store.replay(one);
    checkEq(tlOne.samples.size(), std::size_t(5), "单设备回放：deviceId 便捷字段生效");
    bool onlyB = true;
    for (const auto& rec : tlOne.samples) {
        if (rec.deviceId != "uav-b") onlyB = false;
    }
    check(onlyB, "单设备回放只有该设备的数据");
}

TEST(rpl04_every_nth_sampling) {
    // TLM-RPL-04：稀疏化 everyNth / minIntervalMs，且**必须在结果里标注**。
    Store store(fastOptions(1000));
    for (int i = 0; i < 1000; ++i) store.append("uav-1", "uav.pos", 1'000'000 + i * 100, pos(i, i));
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1'000'000;
    q.toTs    = 1'000'000 + 100'000;
    QueryResult all = store.query(q);
    checkEq(all.rows.size(), std::size_t(1000), "query 全量 1000 条（未超上限）");

    ReplayRequest dense;
    dense.q = q;
    Timeline tlDense = store.replay(dense);
    checkEq(tlDense.samples.size(), all.rows.size(), "不传稀疏参数 → 样本数与 query 一致");
    bool sameAsQuery = (tlDense.samples.size() == all.rows.size());
    for (std::size_t i = 0; sameAsQuery && i < all.rows.size(); ++i) {
        if (!sameRecord(tlDense.samples[i], all.rows[i])) sameAsQuery = false;
    }
    check(sameAsQuery, "不传参数时行为与 query 完全一致（含顺序）");
    checkEq(tlDense.sampled, false, "不传参数 → sampled=false");

    ReplayRequest thin;
    thin.q        = q;
    thin.everyNth = 10;
    Timeline tl = store.replay(thin);
    checkEq(tl.samples.size(), std::size_t(100), "1000 点、步长 10 → 返回 100 点（约 1/10）");
    checkEq(tl.sampled, true, "稀疏化必须标注 sampled=true");
    checkEq(tl.sampleStep, std::size_t(10), "标注实际步长 sampleStep=10");
    checkEq(firstTs(tl.samples), Millis(1'000'000), "首样本 = 第 0 条候选");
    checkEq(nthTs(tl.samples, 1), Millis(1'000'000 + 10 * 100), "第 2 个样本 = 第 10 条候选");
    check(sortedByKey(tl.samples), "稀疏化后仍按 ts 升序");

    ReplayRequest byInterval;
    byInterval.q             = q;
    byInterval.minIntervalMs = 10'000;
    Timeline tlMin = store.replay(byInterval);
    checkEq(tlMin.sampled, true, "minIntervalMs>0 → 标注 sampled=true");
    checkEq(tlMin.samples.size(), std::size_t(10), "最小间隔 10000ms（数据间隔 100ms）→ 10 个样本");
    bool gapOk = true;
    for (std::size_t i = 1; i < tlMin.samples.size(); ++i) {
        if (tlMin.samples[i].ts - tlMin.samples[i - 1].ts < 10'000) gapOk = false;
    }
    check(gapOk, "相邻样本间隔 ≥ minIntervalMs");
}

// ================================================================ TLM-GAP · 补发
TEST(gap01_since_is_strictly_greater) {
    // TLM-GAP-01：since(lastSeenTs) 返回 **ts > lastSeenTs**（严格大于，不重复给客户端已有的点）。
    Store store(fastOptions(1000));
    for (int i = 0; i < 10; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    store.flush();

    SinceRequest req;
    req.devices    = {"uav-1"};
    req.lastSeenTs = 1000;
    Timeline tl = store.since(req);
    checkEq(tl.samples.size(), std::size_t(9), "已有 ts=1000 → since(1000) 返回 1001..1009 共 9 条");
    checkEq(firstTs(tl.samples), Millis(1001), "首条 = 水位 + 1");
    bool noWatermark = true;
    for (const auto& rec : tl.samples) {
        if (rec.ts <= 1000) noWatermark = false;
    }
    check(noWatermark, "结果里没有 ts <= 1000 的点（不含水位那一条）");
    checkEq(tl.fromTs, Millis(1001), "timeline.fromTs = 首样本 ts");
    checkEq(tl.toTs, Millis(1009), "timeline.toTs = 末样本 ts");
    checkEq(tl.durationMs, Millis(8), "durationMs = 1009-1001 = 8");
    checkEq(tl.hasMore, false, "9 条未超上限 → hasMore=false");
    check(sortedByKey(tl.samples), "补发结果按 ts 升序");

    SinceRequest atLatest;
    atLatest.devices    = {"uav-1"};
    atLatest.lastSeenTs = 1009;
    checkEq(store.since(atLatest).samples.size(), std::size_t(0), "水位已在最新点 → 空时间轴");

    SinceRequest fromZero;
    fromZero.devices    = {"uav-1"};
    fromZero.lastSeenTs = 0;
    checkEq(store.since(fromZero).samples.size(), std::size_t(10), "水位 0 → 全部 10 条（ts>0）");
}

TEST(gap02_per_device_watermarks) {
    // TLM-GAP-02：多设备补发 + **每设备各自水位**。
    Store store(fastOptions(1000));
    for (const char* d : {"dev-a", "dev-b", "dev-c"}) {
        for (int i = 0; i <= 10; ++i) store.append(d, "uav.pos", 1000 + i, pos(i, i));
    }
    store.flush();

    SinceRequest req;
    req.perDevice = {{"dev-a", 1005}, {"dev-b", 1000}, {"dev-c", 1010}};
    Timeline tl = store.since(req);
    checkEq(tl.samples.size(), std::size_t(15), "各按自己水位：a 5 条 + b 10 条 + c 0 条 = 15");

    std::map<std::string, std::size_t> cnt;
    std::map<std::string, Millis> minTs;
    for (const auto& rec : tl.samples) {
        ++cnt[rec.deviceId];
        minTs[rec.deviceId] = (minTs.count(rec.deviceId) != 0)
                                  ? std::min(minTs[rec.deviceId], rec.ts)
                                  : rec.ts;
    }
    checkEq(cnt["dev-a"], std::size_t(5), "dev-a 只收到水位 1005 之后的 5 条");
    checkEq(cnt["dev-b"], std::size_t(10), "dev-b 收到 1001..1010 共 10 条");
    checkEq(cnt["dev-c"], std::size_t(0), "dev-c 水位已是最新 → 0 条");
    checkEq(minTs["dev-a"], Millis(1006), "dev-a 最早 = 1006（各自水位生效）");
    checkEq(minTs["dev-b"], Millis(1001), "dev-b 最早 = 1001");
    check(sortedByKey(tl.samples), "多设备补发归并为一条按 ts 有序的时间轴");

    SinceRequest single;
    single.devices    = {"dev-a", "dev-b"};
    single.lastSeenTs = 1008;
    checkEq(store.since(single).samples.size(), std::size_t(4), "单一水位 + devices 列表：各 2 条 = 4");
}

TEST(gap03_since_pagination_with_cursor) {
    // TLM-GAP-03：补发遵守条数上限与游标（复用 QRY-04），缺口大时分批续查。
    Store store(fastOptions(1000));
    const int N = 50;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    store.flush();

    SinceRequest req;
    req.devices    = {"uav-1"};
    req.lastSeenTs = 999;
    req.limit      = 20;
    Timeline p1 = store.since(req);
    checkEq(p1.samples.size(), std::size_t(20), "第 1 批 20 条（达到 limit）");
    checkEq(p1.hasMore, true, "缺口未取完 → hasMore=true");
    checkEq(p1.cursor.valid, true, "带回续查游标");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1000;
    q.toTs    = std::numeric_limits<Millis>::max();
    q.limit   = 20;
    QueryResult p2 = store.queryPage(q, p1.cursor);
    checkEq(p2.rows.size(), std::size_t(20), "第 2 批 20 条");
    checkEq(p2.hasMore, true, "第 2 批仍 hasMore=true");
    checkEq(firstTs(p2.rows), Millis(1020), "第 2 批从第 21 条接上（无重复）");
    QueryResult p3 = store.queryPage(q, p2.cursor);
    checkEq(p3.rows.size(), std::size_t(10), "第 3 批 10 条");
    checkEq(p3.hasMore, false, "取完 → hasMore=false");

    std::set<std::string> keys;
    for (const auto& rec : p1.samples) keys.insert(keyOf(rec));
    for (const auto& rec : p2.rows) keys.insert(keyOf(rec));
    for (const auto& rec : p3.rows) keys.insert(keyOf(rec));
    checkEq(keys.size(), std::size_t(N), "三批共 50 个唯一主键（游标续查无缝衔接）");
    checkEq(firstTs(p1.samples), Millis(1000), "第 1 批从 1000 起");
    checkEq(lastTs(p3.rows), Millis(1049), "最后一批到 1049 止");
}

TEST(gap04_gaps_detects_hole) {
    // TLM-GAP-04：缺口识别 —— 阈值法（默认 3× 名义周期）。人为跳过 5 秒，必须报出这一段。
    Store store(fastOptions(1000));  // nominalPeriodMs 默认 1000 → 阈值 3000
    for (int i = 0; i <= 4; ++i) store.append("uav-1", "uav.pos", i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) store.append("uav-1", "uav.pos", 10'000 + i * 1000, pos(i, i));
    store.flush();

    std::vector<Gap> g = store.gaps(0, 15'000);
    checkEq(g.size(), std::size_t(1), "1Hz 数据中间跳过 5 秒 → 恰好检出 1 段缺口");
    if (!g.empty()) {
        checkEq(g[0].deviceId, std::string("uav-1"), "缺口归属该设备");
        checkEq(g[0].fromTs, Millis(4001), "缺口起点 = 前一条之后（前一条 ts=4000 → 4001，含）");
        checkEq(g[0].toTs, Millis(10'000), "缺口终点 = 后一条之前（下一条 ts=10000，不含）");
        checkEq(g[0].durationMs, Millis(5999), "缺口时长 = 10000-4001 = 5999ms");
        check(g[0].seqBefore > 0 && g[0].seqAfter > g[0].seqBefore, "缺口两侧序号可查（供告警定位）");
    }
    checkEq(store.gaps(0, 5'000).size(), std::size_t(0), "连续段（1Hz，间隔 ≤ 阈值）不误报");
    checkEq(store.gaps(0, 15'000, 7000).size(), std::size_t(0), "阈值放大到 7s > 6s 空档 → 不算缺口");
    checkEq(store.gaps(0, 15'000, 500).size(), std::size_t(9),
            "阈值 500ms → 9 个相邻间隔全超标（阈值法口径：8 个 1s + 1 个 6s）");
    checkEq(store.gaps(15'000, 0).size(), std::size_t(0), "非法区间 → 空结果，不报错");
}

TEST(gap04_multi_device_gaps_are_reported) {
    // TLM-GAP-04（多设备）：需求原文是"返回**每个设备**的未覆盖时间区间列表"。
    // 记录按 (ts, deviceId, type) 排序后同一设备的记录并不相邻，所以必须**按设备分组**比较。
    Store store(fastOptions(1000));
    for (const char* d : {"dev-a", "dev-b"}) {
        for (int i = 0; i <= 4; ++i) store.append(d, "uav.pos", i * 1000, pos(i, i));            // 0..4000
        for (int i = 0; i <= 4; ++i) store.append(d, "uav.pos", 10'000 + i * 1000, pos(i, i));  // 10000..14000
    }
    store.flush();

    std::vector<Gap> g = store.gaps(0, 15'000);
    checkEq(g.size(), std::size_t(2), "两台设备各有一段 5 秒空档 → 报 2 段缺口");
    if (g.size() == 2) {
        checkEq(g[0].deviceId, std::string("dev-a"), "输出按 (缺口起点, 设备名) 稳定排序：第 1 段属 dev-a");
        checkEq(g[1].deviceId, std::string("dev-b"), "第 2 段属 dev-b");
        checkEq(g[0].fromTs, Millis(4001), "dev-a 缺口起点 = 4001");
        checkEq(g[0].toTs, Millis(10'000), "dev-a 缺口终点 = 10000（不含）");
        checkEq(g[0].durationMs, Millis(5999), "dev-a 缺口时长 5999ms");
        checkEq(g[1].fromTs, Millis(4001), "dev-b 缺口起点 = 4001");
        checkEq(g[1].toTs, Millis(10'000), "dev-b 缺口终点 = 10000");
    }

    // 对照：单设备路径同样正确
    Store single(fastOptions(1000));
    for (int i = 0; i <= 4; ++i) single.append("dev-a", "uav.pos", i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) single.append("dev-a", "uav.pos", 10'000 + i * 1000, pos(i, i));
    single.flush();
    checkEq(single.gaps(0, 15'000).size(), std::size_t(1), "对照：单设备报出 1 段缺口");

    // devices 过滤：只要 dev-a
    std::vector<Gap> onlyA = store.gaps(0, 15'000, 0, {"dev-a"});
    checkEq(onlyA.size(), std::size_t(1), "devices 过滤后只报 dev-a 的缺口");
    if (!onlyA.empty()) checkEq(onlyA[0].deviceId, std::string("dev-a"), "过滤结果归属正确");

    // 三台设备、缺口长度不同：每台各报自己那一段
    Store three(fastOptions(1000));
    for (int i = 0; i <= 4; ++i) three.append("dev-a", "uav.pos", i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) three.append("dev-a", "uav.pos", 10'000 + i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) three.append("dev-b", "uav.pos", i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) three.append("dev-b", "uav.pos", 20'000 + i * 1000, pos(i, i));
    for (int i = 0; i <= 4; ++i) three.append("dev-c", "uav.pos", i * 1000, pos(i, i));  // 连续，无缺口
    three.flush();
    std::vector<Gap> tg = three.gaps(0, 25'000);
    checkEq(tg.size(), std::size_t(2), "3 台设备里只有 dev-a / dev-b 有空档 → 2 段缺口");
    if (tg.size() == 2) {
        checkEq(tg[0].deviceId, std::string("dev-a"), "dev-a 的缺口在前（起点更早）");
        checkEq(tg[0].toTs, Millis(10'000), "dev-a 缺口到 10000 止");
        checkEq(tg[1].deviceId, std::string("dev-b"), "dev-b 的缺口在后");
        checkEq(tg[1].toTs, Millis(20'000), "dev-b 缺口到 20000 止（各设备各自计算）");
    }
}

// ================================================================ TLM-RET · 保留
TEST(ret01_ttl_purges_old_keeps_new) {
    // TLM-RET-01：可配置保留期 —— 过期清理、未过期完整保留（假时钟 + 手动巡检，确定性）。
    MemoryBackend backend;
    FakeClock clock(kNow);
    UniformRetentionPolicy policy(3600 * 1000);  // TTL 1 小时
    Store store(fastOptions(1000), backend, clock, policy);

    store.append("uav-1", "uav.pos", kNow - 2 * 3600 * 1000, pos(0, 0));  // 2 小时前 → 过期
    store.append("uav-1", "uav.pos", kNow - 30 * 60 * 1000, pos(1, 1));   // 30 分钟前 → 保留
    store.append("uav-1", "uav.pos", kNow - 60 * 1000, pos(2, 2));        // 1 分钟前 → 保留
    store.flush();

    RetentionReport rep = store.sweep();
    checkEq(rep.supported, true, "后端支持删除 → supported=true");
    checkEq(rep.cutoffTs, kNow - 3600 * 1000, "cutoff = 假时钟 now - TTL(1h)");
    checkEq(rep.purgedRows, std::uint64_t(1), "只清掉 2 小时前那一条");
    checkEq(backend.approxRows(), std::size_t(2), "后端里剩 2 条");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = kNow + 1;
    q.limit   = 100;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(2), "未过期的两条完整保留");
    checkEq(firstTs(r.rows), kNow - 30 * 60 * 1000, "保留的最早一条 = 30 分钟前");
    checkEq(lastTs(r.rows), kNow - 60 * 1000, "保留的最晚一条 = 1 分钟前");

    clock.advance(40 * 60 * 1000);  // 拨钟 40 分钟：30 分钟前那条也过期
    RetentionReport rep2 = store.sweep();
    checkEq(rep2.purgedRows, std::uint64_t(1), "拨钟后 30 分钟前那条过期被清");
    checkEq(backend.approxRows(), std::size_t(1), "只剩 1 条");
    checkEq(store.status().purgedRows, std::uint64_t(2), "累计已清理 2 条");
}

TEST(ret03_purge_counters_visible) {
    // TLM-RET-03：清理可观测 —— 已清理条数 / 最早已清理 ts / 上次清理时刻。
    MemoryBackend backend;
    FakeClock clock(kNow);
    UniformRetentionPolicy policy(60'000);  // TTL 1 分钟
    Store store(fastOptions(1000), backend, clock, policy);

    for (int i = 0; i < 5; ++i) store.append("uav-1", "uav.pos", kNow - 3 * 60'000 + i, pos(i, i));
    for (int i = 0; i < 3; ++i) store.append("uav-1", "uav.pos", kNow - 1000 + i, pos(i, i));
    store.flush();

    const Millis wantCutoff = kNow - 60'000;
    RetentionReport rep = store.sweep();
    Status st = store.status();
    checkEq(rep.purgedRows, std::uint64_t(5), "报告：清掉 5 条过期数据");
    checkEq(st.purgedRows, rep.purgedRows, "status：已清理条数与报告一致");
    checkEq(backend.approxRows(), std::size_t(3), "后端里确实剩 3 条（与实际清理结果一致）");
    checkEq(st.earliestPurgedTs, wantCutoff, "status：最早已清理 ts = 本次 cutoff");
    checkEq(st.lastPurgeTs, kNow, "status：上次清理时刻 = 现在（来自注入的时钟）");
    checkEq(st.purgeFailures, std::uint64_t(0), "没有清理失败");
    checkEq(st.purgeUnsupported, false, "后端支持删除 → 不是降级状态");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = kNow + 1;
    q.limit   = 100;
    checkEq(store.query(q).rows.size(), std::size_t(3), "清理后剩下的 3 条照常可查");
}

TEST(ret04_backend_without_purge_degrades_honestly) {
    // TLM-RET-04：后端 canPurge()==false → 如实返回"不支持"，不报错、不谎报、不反复重试。
    SpyBackend backend;
    backend.canPurgeFlag = false;
    FakeClock clock(kNow);
    UniformRetentionPolicy policy(60'000);
    Store store(fastOptions(1000), backend, clock, policy);
    for (int i = 0; i < 10; ++i) store.append("uav-1", "uav.pos", kNow - 10 * 60'000 + i, pos(i, i));
    store.flush();
    checkEq(backend.appendedRows, std::size_t(10), "数据在注入的后端里");

    RetentionReport rep = store.sweep();
    checkEq(rep.supported, false, "不支持删除 → supported=false（如实告知）");
    checkEq(rep.purgedRows, std::uint64_t(0), "没有清理任何东西（不假装成功）");
    checkEq(store.status().purgeUnsupported, true, "降级状态可观测");
    checkEq(store.status().purgedRows, std::uint64_t(0), "已清理条数保持 0（不谎报）");
    checkEq(backend.purgeCalls, 0, "明知不支持就不去调 purge（不反复重试）");
    checkEq(backend.approxRows(), std::size_t(10), "数据一条没少");

    RetentionReport rep2 = store.sweep();
    checkEq(rep2.supported, false, "再次巡检仍如实返回不支持");
    checkEq(backend.purgeCalls, 0, "重复巡检也不会去重试删除");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = kNow + 1;
    q.limit   = 100;
    checkEq(store.query(q).rows.size(), std::size_t(10), "保留降级后数据照常可查");
}

TEST(ret05_capacity_limit_trims_oldest) {
    // TLM-RET-05：容量上限（条数）作为保留期的补充；超限按最旧优先淘汰。
    MemoryBackend backend;
    FakeClock clock(kNow);
    UniformRetentionPolicy policy(0, 1000);  // ttl=0（不按时间清）+ maxRows=1000
    Store store(fastOptions(1000), backend, clock, policy);
    const int N = 2500;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1'000'000 + i, pos(i, i));
    store.flush();
    checkEq(backend.approxRows(), std::size_t(N), "灌入 2500 条（超过容量上限 1000）");

    RetentionReport rep = store.sweep();
    checkEq(rep.capacityTrimmed, true, "报告：本次因容量上限触发裁剪");
    checkEq(rep.purgedRows, std::uint64_t(0), "容量裁剪不属于时间清理（TTL=0）");
    // TLM-RET-05 要求"超限按最旧优先淘汰**并计数**"：淘汰条数如实记在 capacityEvictedRows。
    checkEq(rep.capacityEvictedRows, std::uint64_t(N - 1000),
            "容量淘汰条数被计数：2500 - 1000 = 1500");
    check(backend.approxRows() <= 1000,
          "条数被压到上限 1000 以内，实际=" + std::to_string(backend.approxRows()));
    checkEq(backend.approxRows(), std::size_t(1000), "正好压到上限 1000");
    checkEq(store.status().capacityUnsupported, false, "内存后端支持容量裁剪");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = 1'000'000 + N;
    q.limit   = 5000;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(1000), "查询也只剩 1000 条");
    checkEq(firstTs(r.rows), Millis(1'000'000 + N - 1000), "留下的最早一条 = 第 1501 条（最旧优先淘汰）");
    checkEq(lastTs(r.rows), Millis(1'000'000 + N - 1), "最新一条保留");
    checkEq(r.truncatedByRetention, true, "容量裁剪后查询更早区间 → 如实标注（P4）");

    // TLM-RET-05 要求"超限按最旧优先淘汰并计数"。这里断言**可观测的等价事实**：
    //   ① 报告明确标注本次因容量触发（rep.capacityTrimmed，已在上面断言）
    //   ② 淘汰条数可由公开口径复算：灌入量 - approxRows() = 2500 - 1000 = 1500
    //   ③ 被淘汰的正好是最旧的那 1500 条（最早幸存者 = 第 1501 条，已在上面断言）
    // 调用方据此就能算出"丢了多少、丢的是哪段"，不必依赖隐藏计数器。
    // （是否再给 Status 加一个"容量淘汰条数"字段属口径问题，见文件末尾的说明。）
    const std::size_t trimmed = static_cast<std::size_t>(N) - backend.approxRows();
    checkEq(trimmed, std::size_t(N) - 1000, "淘汰条数可复算：2500 - 1000 = 1500 条");
    checkEq(static_cast<std::size_t>(N) - r.rows.size(), trimmed,
            "查询条数与后端条数一致：淘汰量从两条公开路径算出来相同");
}

// ================================================================ TLM-ADP · 适配注入
TEST(adp01_swap_backend_only) {
    // TLM-ADP-01：存储后端注入 —— 换后端只改装配代码，模块源码零改动。
    SpyBackend spy;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(100), spy, clock, policy);
    checkEq(store.backendName(), std::string("spy"), "模块用的是注入的后端");

    for (int i = 0; i < 100; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    store.flush();
    checkEq(spy.appendedRows, std::size_t(100), "数据确实落在注入的后端上（appendedRows 对得上）");
    checkEq(spy.rows_.size(), std::size_t(100), "后端内部也收到 100 条");
    checkEq(store.status().persisted, std::uint64_t(100), "模块的落库计数对得上");
    checkEq(store.status().backend, std::string("spy"), "status() 报告注入的后端名");
    checkEq(store.status().backendWriteFailures, std::uint64_t(0), "没有写入失败");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1000;
    q.toTs    = 1100;
    q.limit   = 200;
    checkEq(store.query(q).rows.size(), std::size_t(100), "同一个 Store 通过注入的后端查得到数据");
    check(spy.readCalls > 0, "读路径也走注入的后端");

    Store defaultStore(fastOptions(100));
    checkEq(defaultStore.backendName(), std::string("memory"), "不注入时用模块自带的零依赖内存后端");
}

TEST(adp02_fake_clock_drives_retention) {
    // TLM-ADP-02：时间源注入 —— 模块内部取"现在"只经 IClock，保留清理因此可确定性测试。
    MemoryBackend backend;
    FakeClock clock(kNow);  // 与真实系统时间无关的固定值
    UniformRetentionPolicy policy(60'000);
    Store store(fastOptions(1000), backend, clock, policy);
    store.append("uav-1", "uav.pos", kNow - 120'000, pos(0, 0));  // 2 分钟前
    store.append("uav-1", "uav.pos", kNow - 30'000, pos(1, 1));   // 30 秒前
    store.flush();

    RetentionReport rep = store.sweep();
    checkEq(rep.nowTs, kNow, "sweep 的 nowTs 取自注入的时钟，不是系统时间");
    checkEq(rep.cutoffTs, kNow - 60'000, "cutoff = 假时钟 now - TTL");
    checkEq(rep.purgedRows, std::uint64_t(1), "2 分钟前那条被清理");
    checkEq(store.status().lastPurgeTs, kNow, "status 的上次清理时刻也来自假时钟");

    clock.advance(10 * 60'000);  // 拨快 10 分钟（真实时间没有流逝）
    RetentionReport rep2 = store.sweep();
    checkEq(rep2.cutoffTs, kNow + 10 * 60'000 - 60'000, "拨钟后 cutoff 跟着走");
    checkEq(rep2.purgedRows, std::uint64_t(1), "30 秒前那条现在也过期");
    checkEq(backend.approxRows(), std::size_t(0), "后端里没有数据了");
}

TEST(adp04_fromEnvelope_pure_function) {
    // TLM-ADP-04：`fromEnvelope` 是纯函数，只依赖信封语法；非法输入返回 false 而不抛。
    Record r;
    nlohmann::json env = {
        {"type", "uav.pos"}, {"data", {{"deviceId", "uav-1"}, {"lng", 116.4}}}, {"ts", 1700}};
    checkEq(Store::fromEnvelope(env, r), true, "合法信封 → true");
    checkEq(r.type, std::string("uav.pos"), "type 原样带出");
    checkEq(r.ts, Millis(1700), "ts 原样带出");
    checkEq(r.deviceId, std::string("uav-1"), "deviceId 从 data 里取（信封顶层只有三字段）");
    checkEq(r.data, env["data"], "data 原样透传：不看、不改、不索引");
    checkEq(r.seq, std::uint64_t(0), "seq 留给 append 补号");

    Record out;
    const nlohmann::json obj = nlohmann::json::object();
    checkEq(Store::fromEnvelope(obj, out), false, "空对象 → false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"data", obj}, {"ts", 1}}, out), false, "缺 type → false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", "t"}, {"ts", 1}}, out), false, "缺 data → false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", "t"}, {"data", obj}}, out), false, "缺 ts → false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", "t"}, {"data", nlohmann::json::array()}, {"ts", 1}}, out),
            false, "data 不是对象（数组）→ false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", "t"}, {"data", "x"}, {"ts", 1}}, out),
            false, "data 不是对象（字符串）→ false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", 5}, {"data", obj}, {"ts", 1}}, out),
            false, "type 不是字符串 → false");
    checkEq(Store::fromEnvelope(nlohmann::json{{"type", "t"}, {"data", obj}, {"ts", "1700"}}, out),
            false, "ts 不是数字（字符串）→ false");
    checkEq(Store::fromEnvelope(nlohmann::json::parse("[1,2,3]", nullptr, false), out), false,
            "信封是数组 → false");
    checkEq(Store::fromEnvelope(nlohmann::json::parse("{坏 json", nullptr, false), out), false,
            "非法 JSON（parse 失败后是 discarded）→ false");
    checkEq(Store::fromEnvelope(nlohmann::json::parse("null", nullptr, false), out), false,
            "null → false");

    Record noDev;
    nlohmann::json env2 = {{"type", "link.quality"}, {"data", {{"rssi", -70}}}, {"ts", 9}};
    checkEq(Store::fromEnvelope(env2, noDev), true, "data 里没有 deviceId 仍算合法信封");
    checkEq(noDev.deviceId, std::string(""), "deviceId 留空（模块不猜业务字段名，P1）");

    bool threw = false;
    try {
        Record probe;
        Store::fromEnvelope(nlohmann::json::parse("[]", nullptr, false), probe);
    } catch (...) {
        threw = true;
    }
    checkEq(threw, false, "fromEnvelope 不抛异常");

    // 不接上游也能跑通全流程：宿主手工 append 信封转换出来的记录
    Store store(fastOptions(1000));
    Record rec;
    checkEq(Store::fromEnvelope(env, rec), true, "信封转换成功");
    store.append(rec);
    store.flush();
    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1700;
    q.toTs    = 1701;
    QueryResult qr = store.query(q);
    checkEq(qr.rows.size(), std::size_t(1), "手工驱动的全流程跑通（写入 → 查询）");
    checkEq(firstType(qr.rows), std::string("uav.pos"), "查询到的 type 与信封一致");
}

// ================================================================ TLM-OBS · 可观测
TEST(obs01_status_fields_match_input) {
    // TLM-OBS-01：状态快照字段齐全，数值与实际灌入量对得上。
    SpyBackend backend;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(100), backend, clock, policy);  // 每 100 条刷一次

    const int N = 250;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    Status mid = store.status();
    checkEq(mid.appended, std::uint64_t(N), "appended = 250");
    checkEq(mid.persisted, std::uint64_t(200), "persisted = 200（两次刷盘）");
    checkEq(mid.buffered, std::uint64_t(50), "buffered = 50（尚未刷盘）");
    checkEq(mid.flushes, std::uint64_t(2), "flushes = 2");
    checkEq(mid.droppedByOverflow, std::uint64_t(0), "droppedByOverflow = 0");
    checkEq(mid.backendWriteFailures, std::uint64_t(0), "backendWriteFailures = 0");
    checkEq(mid.rejectedByLimit, std::uint64_t(0), "rejectedByLimit = 0");
    checkEq(mid.purgeFailures, std::uint64_t(0), "purgeFailures = 0");
    checkEq(mid.purgedRows, std::uint64_t(0), "purgedRows = 0");
    checkEq(mid.earliestPurgedTs, Millis(0), "earliestPurgedTs = 0（没清理过）");
    checkEq(mid.lastPurgeTs, Millis(0), "lastPurgeTs = 0（没清理过）");
    checkEq(mid.seqFirst, std::uint64_t(1), "seqFirst = 1");
    checkEq(mid.seqLast, std::uint64_t(N), "seqLast = 250");
    checkEq(mid.thresholdState, std::string("ok"), "thresholdState = ok");
    checkEq(mid.backend, std::string("spy"), "backend = 注入的后端名");
    checkEq(mid.version, std::string(TELEMETRY_STORE_VERSION), "version 可读");
    checkEq(mid.appended, mid.persisted + mid.buffered + mid.droppedByOverflow, "记账恒等式成立");

    // 字段齐全：OBS-01 点名的字段必须都出现在 JSON 快照里
    const nlohmann::json sj = mid.toJson();
    for (const char* key : {"appended", "persisted", "buffered", "droppedByOverflow",
                            "backendWriteFailures", "rejectedByLimit", "purgeFailures",
                            "purgedRows", "earliestPurgedTs", "lastPurgeTs", "lastWriteMs",
                            "avgWriteMs", "seqFirst", "seqLast", "flushes", "thresholdState",
                            "backend", "version"}) {
        check(sj.find(key) != sj.end(), std::string("status().toJson() 含字段 ") + key);
    }
    check(sj["appended"].get<std::uint64_t>() == mid.appended, "JSON 快照与结构体一致（appended）");
    check(sj["buffered"].get<std::uint64_t>() == mid.buffered, "JSON 快照与结构体一致（buffered）");

    store.flush();
    Status done = store.status();
    checkEq(done.persisted, std::uint64_t(N), "flush 后 250 条全部落库");
    checkEq(done.buffered, std::uint64_t(0), "flush 后缓冲为空");
    checkEq(done.flushes, std::uint64_t(3), "flushes = 3");
    checkEq(backend.appendedRows, std::size_t(N), "后端也确实收到 250 条");
}

TEST(obs02_backend_failure_counted_and_recovers) {
    // TLM-OBS-02：逐类构造故障、各自计数；后端失败时数据不丢（留在缓冲重试）并可恢复。
    SpyBackend backend;
    backend.setFailTimes(100);
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(10), backend, clock, policy);  // 每 10 条刷一次 → 多次失败

    const int N = 20;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    Status st = store.status();
    checkEq(st.appended, std::uint64_t(N), "收到 20 条");
    checkEq(st.persisted, std::uint64_t(0), "后端全拒 → 一条都没落库");
    checkEq(st.buffered, std::uint64_t(N), "数据仍在缓冲里等重试（不丢，TLM-WRT-07/§4.6）");
    check(st.backendWriteFailures >= 2,
          "后端写入失败被计数，实际=" + std::to_string(st.backendWriteFailures));
    checkEq(st.droppedByOverflow, std::uint64_t(0), "写入失败与溢出丢弃是两类失败，分开计数");
    checkEq(st.purgeFailures, std::uint64_t(0), "清理失败计数不受影响");
    checkEq(st.rejectedByLimit, std::uint64_t(0), "查询超限计数不受影响");
    checkEq(st.thresholdState, std::string("degraded"), "连续失败 ≥ 3 → thresholdState=degraded");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 1000;
    q.toTs    = 1100;
    q.limit   = 100;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(0), "后端拒收 + 数据还在缓冲 → 查不到");
    check(r.partial,
          "刷盘失败时查询结果 partial=true（设计 §4.6：数据没落盘就不许谎报完整）");

    backend.setFailTimes(0);  // 后端恢复
    store.flush();
    Status st2 = store.status();
    checkEq(st2.persisted, std::uint64_t(N), "恢复后 20 条全部补齐（一条不丢）");
    checkEq(st2.buffered, std::uint64_t(0), "恢复后缓冲清空");
    checkEq(st2.thresholdState, std::string("ok"), "连续失败清零 → 回到 ok");
    checkEq(backend.appendedRows, std::size_t(N), "后端确实收到 20 条");
    checkEq(store.query(q).rows.size(), std::size_t(N), "恢复后查得到数据");
    checkEq(store.query(q).partial, false, "不再有写入失败 → partial=false");
}

TEST(obs02_purge_failure_is_counted_and_retried) {
    // TLM-OBS-02（清理失败类）：后端**声明**支持删除但执行失败 →
    // 必须计 purgeFailures、如实返回、下轮继续重试（既不静默，也不假装清完了）。
    SpyBackend backend;
    backend.failPurge = true;
    FakeClock clock(kNow);
    UniformRetentionPolicy policy(60'000);
    Store store(fastOptions(1000), backend, clock, policy);
    for (int i = 0; i < 5; ++i) store.append("uav-1", "uav.pos", kNow - 10 * 60'000 + i, pos(i, i));
    store.flush();
    checkEq(store.status().purgeUnsupported, false, "后端声明支持删除（不是能力降级那条路）");

    RetentionReport rep = store.sweep();
    checkEq(rep.supported, true, "能力声明为支持 → supported=true");
    checkEq(rep.purgedRows, std::uint64_t(0), "清理失败 → 一条也没清掉（不谎报）");
    checkEq(store.status().purgeFailures, std::uint64_t(1), "清理失败被计数（P2：不静默）");
    checkEq(backend.approxRows(), std::size_t(5), "数据一条没少");
    check(backend.purgeCalls >= 1, "确实调用过后端 purge");

    RetentionReport rep2 = store.sweep();
    checkEq(rep2.purgedRows, std::uint64_t(0), "第二轮仍然失败");
    checkEq(store.status().purgeFailures, std::uint64_t(2), "下轮继续重试并计数（设计 §4.5）");
    checkEq(store.status().purgedRows, std::uint64_t(0), "已清理条数保持 0");

    backend.failPurge = false;  // 后端恢复
    RetentionReport rep3 = store.sweep();
    checkEq(rep3.purgedRows, std::uint64_t(5), "恢复后一次清掉 5 条过期数据");
    checkEq(store.status().purgeFailures, std::uint64_t(2), "失败计数不再增长");
    checkEq(backend.approxRows(), std::size_t(0), "后端已清空");
}

TEST(obs03_log_sink_receives_overflow) {
    // TLM-OBS-03：日志出口可注入；溢出这类"必须不静默"的事件要落到注入的 sink 上。
    CapturingLog log;
    SpyBackend backend;
    backend.setFailTimes(100000000);  // 后端全拒 → 缓冲腾不空 → 必然溢出
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    StoreOptions o;
    o.flush.maxRecords = 0;
    o.flush.maxDelayMs = 0;
    o.bufferCapacity   = 10;
    o.onFull           = BufferFullPolicy::DropOldest;
    o.enableSweeper    = false;
    Store store(o, backend, clock, policy, &log);

    for (int i = 0; i < 50; ++i) store.append("uav-1", "uav.pos", 1000 + i, pos(i, i));
    checkEq(store.status().droppedByOverflow, std::uint64_t(40), "容量 10、灌 50 条、后端全拒 → 丢 40 条");
    check(!log.entries.empty(), "注入的日志出口确实被调用过");
    check(log.hasEvent("bufferOverflow"), "溢出事件写进了注入的日志出口（P2：不静默丢数据）");
}

// ================================================================ TLM-NFR · 非功能
TEST(nfr05_concurrent_append_and_query) {
    // TLM-NFR-05：append / query / status / since 可被多线程并发调用，结果正确、无数据竞争。
    MemoryBackend backend;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    StoreOptions o = fastOptions(1000);
    o.bufferCapacity = 65536;
    Store store(o, backend, clock, policy);

    const int kWriters = 4;
    const int kReaders = 4;
    const int kPerWriter = 2000;
    const Millis base = 1'000'000;
    std::atomic<bool> writersDone{false};

    std::vector<std::thread> writers;
    writers.reserve(static_cast<std::size_t>(kWriters));
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&store, w, base] {
            const std::string dev = "conc-" + std::to_string(w);
            for (int i = 0; i < 2000; ++i) {
                store.append(dev, "uav.pos", base + i, pos(w, i));
            }
        });
    }
    std::vector<std::thread> readers;
    readers.reserve(static_cast<std::size_t>(kReaders));
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&store, &writersDone, base] {
            Query q;
            q.devices = {"conc-0", "conc-1", "conc-2", "conc-3"};
            q.fromTs  = base;
            q.toTs    = base + 2000;
            q.limit   = 50;
            SinceRequest sr;
            sr.devices    = q.devices;
            sr.lastSeenTs = base;
            sr.limit      = 50;
            int iters = 0;
            // 收敛保证：写完就退出，另加硬上限兜底（不无限循环）
            while (!writersDone.load() && iters < 20000) {
                const QueryResult qr = store.query(q);
                if (!sortedByKey(qr.rows)) {
                    g_threadErrors.fetch_add(1);
                    break;
                }
                (void)store.status();
                const Timeline tl = store.since(sr);
                if (!sortedByKey(tl.samples)) {
                    g_threadErrors.fetch_add(1);
                    break;
                }
                ++iters;
            }
        });
    }
    for (auto& t : writers) t.join();
    writersDone.store(true);
    for (auto& t : readers) t.join();
    store.flush();

    const std::uint64_t total = static_cast<std::uint64_t>(kWriters) * kPerWriter;
    Status st = store.status();
    checkEq(st.appended, total, "4 线程 × 2000 条 = 8000 条全部被接管");
    checkEq(st.persisted, total, "8000 条全部落库（并发下无丢失）");
    checkEq(st.droppedByOverflow, std::uint64_t(0), "并发下缓冲未溢出");
    checkEq(st.buffered, std::uint64_t(0), "flush 后缓冲为空");
    checkEq(st.seqLast - st.seqFirst + 1, st.appended, "序号跨度 == 收到条数（并发发号无重复无缺口）");
    checkEq(g_threadErrors.load(), 0, "并发查询/补发的结果始终有序（无数据竞争的症状）");

    Query q;
    q.devices = {"conc-0", "conc-1", "conc-2", "conc-3"};
    q.fromTs  = base;
    q.toTs    = base + kPerWriter;
    q.limit   = 10000;  // ≤ maxRowsPerQuery（超过会被"超限拒绝"，见 qry04 的第二段）
    QueryResult all = store.query(q);
    checkEq(all.rejected, false, "limit 在上限内，不是拒绝执行");
    checkEq(all.rows.size(), std::size_t(total), "全量查询 = 8000 条（无重复点）");
    check(sortedByKey(all.rows), "全量结果按 (ts, deviceId, type) 有序");
    std::map<std::string, std::size_t> per;
    for (const auto& rec : all.rows) ++per[rec.deviceId];
    checkEq(per.size(), std::size_t(kWriters), "4 台设备都有数据");
    bool perOk = true;
    for (const auto& kv : per) {
        if (kv.second != std::size_t(kPerWriter)) perOk = false;
    }
    check(perOk, "每台设备各 2000 条（ts 连续无缺口）");
    checkEq(backend.approxRows(), std::size_t(total), "后端里也是 8000 条");
    checkEq(store.status().rejectedByLimit, std::uint64_t(0), "并发读写没有触发超限拒绝");
}

TEST(nfr06_seq_gap_detectable_after_loss) {
    // TLM-NFR-06：非优雅/异常丢数据时允许丢，但**必须如实反映**（序号缺口可检出，不谎报完整）。
    SpyBackend backend;
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    StoreOptions o;
    o.flush.maxRecords    = 0;  // 关掉自动刷盘，本用例显式控制刷盘时机
    o.flush.maxDelayMs    = 0;
    o.bufferCapacity      = 100;
    o.onFull              = BufferFullPolicy::DropOldest;
    o.enableSweeper       = false;
    o.queryFlushesFirst   = false;
    Store store(o, backend, clock, policy);

    for (int i = 1; i <= 100; ++i) {
        store.append("uav-1", "uav.pos", 1000 + i, nlohmann::json{{"n", i}});
    }
    store.flush();  // 第 1 批：seq 1..100 正常落库
    checkEq(backend.appendedRows, std::size_t(100), "第 1 批 100 条落库");

    backend.setFailTimes(1);  // 制造一次后端拒收
    for (int i = 101; i <= 202; ++i) {
        store.append("uav-1", "uav.pos", 1000 + i, nlohmann::json{{"n", i}});
    }
    // i=201：缓冲满 → flush 失败一次 → DropOldest 淘汰 seq=101 那条
    // i=202：缓冲满 → flush 成功（seq 102..201 落库）→ 缓冲里留 1 条
    store.flush();

    Status st = store.status();
    checkEq(st.appended, std::uint64_t(202), "共 202 次 append");
    checkEq(st.droppedByOverflow, std::uint64_t(1), "溢出丢弃 1 条（缓冲满 + 后端拒收一次）");
    checkEq(st.persisted, std::uint64_t(201), "落库 201 条 = 202 - 1 条丢弃");
    checkEq(st.buffered, std::uint64_t(0), "flush 后缓冲为空");
    checkEq(st.seqLast - st.seqFirst + 1, std::uint64_t(202), "序号跨度 = 202（模块发了 202 个号）");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = 0;
    q.toTs    = 10'000;
    q.limit   = 1000;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(201), "查询回来 201 条（少的那条真的没了）");

    std::set<std::uint64_t> seqs;
    for (const auto& rec : r.rows) seqs.insert(rec.seq);
    checkEq(seqs.size(), std::size_t(201), "201 个不同序号");
    check(*seqs.rbegin() - *seqs.begin() + 1 > seqs.size(),
          "序号跨度 > 条数 → 缺口可检出（P3：不假装完整）");
    std::uint64_t missing = 0;
    for (std::uint64_t s = *seqs.begin(); s <= *seqs.rbegin(); ++s) {
        if (seqs.count(s) == 0) {
            missing = s;
            break;
        }
    }
    checkEq(missing, std::uint64_t(101), "缺口正好是被淘汰的那一条（seq=101）");
    bool lostGone = true;
    for (const auto& rec : r.rows) {
        if (rec.data.value("n", 0) == 101) lostGone = false;
    }
    check(lostGone, "被淘汰的记录查不到，也没有被伪造出来");
    check(r.partial,
          "存在序号缺口（溢出丢弃）时 partial=true（record.h 把'序号缺口'列为 partial 成因）");
    QueryResult again = store.query(q);
    check(again.partial, "丢弃不可逆：之后的查询仍然如实标注 partial=true");
}

// ================================================================ 文件分段后端（L2）
TEST(segfile_restart_keeps_data) {
    // TLM-ADP-01 / TLM-WRT-07 / TLM-NFR-06：换后端只改装配；数据跨"进程重启"留存。
    TempDir dir("restart");
    SegmentFileBackend::Options bo;
    bo.maxRowsPerSegment = 100;

    const int N = 250;
    const Millis t0 = kNow;
    {
        SegmentFileBackend backend(dir.str(), bo);
        FakeClock clock(t0);
        KeepForeverPolicy policy;
        Store store(fastOptions(1000), backend, clock, policy);
        checkEq(store.backendName(), std::string("segfile"), "装配的是段文件后端（模块源码零改动）");
        for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", t0 + i * 1000, pos(i, i));
        store.flush();
        checkEq(backend.approxRows(), std::size_t(N), "落盘 250 条");
        checkEq(backend.segmentCount(), std::size_t(3), "250 条 / 每段上限 100 条 = 3 段");
        checkEq(backend.earliestRetainedTs(), t0, "earliestRetainedTs = 首条 ts");
    }  // 析构：Store 刷缓冲 → 后端关段文件

    {  // "重启"：新后端 + 新 Store 指向同一目录
        SegmentFileBackend reopened(dir.str(), bo);
        checkEq(reopened.approxRows(), std::size_t(N), "重启后从磁盘装回 250 条");
        checkEq(reopened.segmentCount(), std::size_t(3), "重启后认出 3 个段");
        FakeClock clock(t0 + N * 1000);
        KeepForeverPolicy policy;
        Store store(fastOptions(1000), reopened, clock, policy);
        Query q;
        q.devices = {"uav-1"};
        q.fromTs  = t0;
        q.toTs    = t0 + N * 1000;
        q.limit   = 1000;
        QueryResult r = store.query(q);
        checkEq(r.rows.size(), std::size_t(N), "重启后查得到全部 250 条（跨进程留存）");
        checkEq(firstTs(r.rows), t0, "首条 ts 一致");
        checkEq(lastTs(r.rows), t0 + (N - 1) * 1000, "末条 ts 一致");
        checkEq(firstData(r.rows), pos(0, 0), "首条 data 原样留存");
        checkEq(lastData(r.rows), pos(N - 1, N - 1), "末条 data 原样留存");
        checkEq(firstSeq(r.rows), std::uint64_t(1), "seq 一并留存（缺口检测要用它）");
        checkEq(lastSeq(r.rows), std::uint64_t(N), "末条 seq = 250");
        checkEq(store.status().persisted, std::uint64_t(0), "新进程计数从 0 开始（计数随重启归零）");
    }
}

TEST(segfile_purge_deletes_whole_segments) {
    // TLM-RET-01/03/04 + TLM-QRY-05：段文件后端上的清理 —— 整段过期整段删，跨边界只重写那一段。
    TempDir dir("purge");
    SegmentFileBackend::Options bo;
    bo.maxRowsPerSegment = 10;
    SegmentFileBackend backend(dir.str(), bo);
    FakeClock clock(100'000);
    UniformRetentionPolicy policy(85'000);  // TTL 85s → cutoff = 100000-85000 = 15000
    Store store(fastOptions(1000), backend, clock, policy);

    for (int i = 1; i <= 30; ++i) store.append("seg-dev", "uav.pos", i * 1000, pos(i, i));
    store.flush();
    checkEq(backend.approxRows(), std::size_t(30), "30 条落盘");
    checkEq(backend.segmentCount(), std::size_t(3), "每段 10 条 → 3 段");
    checkEq(backend.earliestRetainedTs(), Millis(1000), "清理前 earliestRetainedTs = 1000");

    RetentionReport rep = store.sweep();
    checkEq(rep.supported, true, "segfile 的 canPurge()=true → supported=true");
    checkEq(rep.cutoffTs, Millis(15'000), "cutoff = now - ttl = 15000");
    checkEq(rep.purgedRows, std::uint64_t(14), "清掉 14 条（整段 10 条 + 跨边界段 4 条）");
    checkEq(backend.approxRows(), std::size_t(16), "剩 16 条");
    checkEq(backend.segmentCount(), std::size_t(2), "整段删除生效：3 段 → 2 段");
    checkEq(backend.earliestRetainedTs(), Millis(15'000), "earliestRetainedTs = 保留边界 15000");
    checkEq(store.status().purgedRows, std::uint64_t(14), "status：已清理条数对得上");
    checkEq(store.status().earliestPurgedTs, Millis(15'000), "status：最早已清理 ts = cutoff");
    checkEq(store.status().lastPurgeTs, Millis(100'000), "status：上次清理时刻 = 假时钟的现在");

    Query q;
    q.devices = {"seg-dev"};
    q.fromTs  = 0;
    q.toTs    = 40'000;
    q.limit   = 1000;
    QueryResult r = store.query(q);
    checkEq(r.rows.size(), std::size_t(16), "清理后查得到 16 条");
    checkEq(firstTs(r.rows), Millis(15'000), "最早一条 = 15000（边界那一截保留正确）");
    checkEq(r.truncatedByRetention, true, "查询区间早于保留边界 → 如实标注（P4）");

    // 再拨钟：所有段都过期 → 全删，earliestRetainedTs 归 0
    clock.set(200'000);
    RetentionReport rep2 = store.sweep();
    checkEq(rep2.purgedRows, std::uint64_t(16), "第二轮把剩下的 16 条全部清掉");
    checkEq(backend.approxRows(), std::size_t(0), "段全删后 approxRows = 0");
    checkEq(backend.earliestRetainedTs(), Millis(0), "空库 earliestRetainedTs = 0");
}

TEST(segfile_cursor_pagination_continues_across_segments) {
    // TLM-QRY-04：文件分段后端上的游标续查必须跨段无缝（不重不漏）。
    TempDir dir("cursor");
    SegmentFileBackend::Options bo;
    bo.maxRowsPerSegment = 10;
    SegmentFileBackend backend(dir.str(), bo);
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(1000), backend, clock, policy);

    const int N = 45;
    const Millis t0 = kNow - 100'000;
    for (int i = 0; i < N; ++i) store.append("uav-1", "uav.pos", t0 + i * 1000, pos(i, i));
    store.flush();
    checkEq(backend.approxRows(), std::size_t(N), "45 条落盘");
    checkEq(backend.segmentCount(), std::size_t(5), "每段 10 条 → 5 段");

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = t0;
    q.toTs    = t0 + N * 1000;
    q.limit   = 20;
    QueryResult p1 = store.query(q);
    checkEq(p1.rows.size(), std::size_t(20), "第 1 页 20 条");
    checkEq(p1.hasMore, true, "还有剩余 → hasMore=true");
    checkEq(firstTs(p1.rows), t0, "第 1 页从区间起点开始");

    QueryResult p2 = store.queryPage(q, p1.cursor);
    checkEq(p2.rows.size(), std::size_t(20), "第 2 页 20 条");
    checkEq(firstTs(p2.rows), t0 + 20 * 1000, "第 2 页从第 21 条接上（无重复）");

    QueryResult p3 = store.queryPage(q, p2.cursor);
    checkEq(p3.hasMore, false, "第 3 页取完 → hasMore=false");
    checkEq(p3.rows.size(), std::size_t(5), "第 3 页返回剩余 5 条（跨段游标无缝衔接）");
    checkEq(lastTs(p3.rows), t0 + (N - 1) * 1000, "最后一条 = 区间内最晚 ts");

    std::set<std::string> keys;
    for (const auto& rec : p1.rows) keys.insert(keyOf(rec));
    for (const auto& rec : p2.rows) keys.insert(keyOf(rec));
    for (const auto& rec : p3.rows) keys.insert(keyOf(rec));
    checkEq(keys.size(), p1.rows.size() + p2.rows.size() + p3.rows.size(), "各页之间没有重复记录");
    checkEq(keys.size(), std::size_t(N), "游标续查取完 45 条：无重复无遗漏");
}

TEST(segfile_same_range_query_is_repeatable) {
    // TLM-QRY-02 + TLM-ADP-01：同一区间的重复查询必须每次都返回同样的数据。
    // 这里刻意读同一段 4 次 —— 段读缓存命中路径（第 2 次起）与"缓存被掏空"这类回归
    // 只在第二次查询时才发作，是最容易漏测的一条路径。
    TempDir dir("repeat");
    SegmentFileBackend backend(dir.str());
    FakeClock clock(kNow);
    KeepForeverPolicy policy;
    Store store(fastOptions(1000), backend, clock, policy);
    for (int i = 0; i < 10; ++i) store.append("uav-1", "uav.pos", kNow - 10'000 + i * 1000, pos(i, i));
    store.flush();

    Query q;
    q.devices = {"uav-1"};
    q.fromTs  = kNow - 20'000;
    q.toTs    = kNow + 1000;
    q.limit   = 100;

    std::vector<std::size_t> got;
    for (int round = 0; round < 4; ++round) got.push_back(store.query(q).rows.size());
    checkEq(got[0], std::size_t(10), "第 1 次查询 10 条（读盘，缓存未命中）");
    checkEq(got[1], std::size_t(10), "第 2 次查询 10 条（缓存命中）");
    checkEq(got[2], std::size_t(10), "第 3 次同区间查询仍是 10 条（缓存可重复命中）");
    checkEq(got[3], std::size_t(10), "第 4 次同区间查询仍是 10 条");
    checkEq(backend.approxRows(), std::size_t(10), "后端里数据仍在（读缓存不许吞数据）");

    // 续查路径也读同一段：游标翻页后内容必须仍然完整
    Query one;
    one.devices = {"uav-1"};
    one.fromTs  = kNow - 20'000;
    one.toTs    = kNow + 1000;
    one.limit   = 4;
    QueryResult page1 = store.query(one);
    checkEq(page1.rows.size(), std::size_t(4), "limit=4 → 第 1 页 4 条");
    QueryResult page2 = store.queryPage(one, page1.cursor);
    checkEq(page2.rows.size(), std::size_t(4), "第 2 页 4 条（同一段被再次读出）");
    QueryResult page3 = store.queryPage(one, page2.cursor);
    checkEq(page3.rows.size(), std::size_t(2), "第 3 页 2 条（取完）");
    checkEq(page3.hasMore, false, "取完后 hasMore=false");
}

// ================================================================ 入口
int main(int argc, char** argv) {
    telemetry_store::consoleUtf8();  // Windows 控制台切 UTF-8，中文输出不乱码

    std::string filter;
    if (argc > 1) {
        filter = argv[1];
        if (filter == "--list") {
            for (const auto& t : registry()) std::cout << t.name << "\n";
            return 0;
        }
    }

    int ran = 0;
    for (const auto& t : registry()) {
        if (!filter.empty() && t.name.find(filter) == std::string::npos) continue;
        g_case = t.name;
        ++ran;
        t.fn();
    }

    std::cout << "----------------------------------------\n";
    for (const auto& f : g_failures) std::cout << "FAIL " << f << "\n";
    for (const auto& k : g_knownNotes) std::cout << "XFAIL(待实现修复) " << k << "\n";
    std::cout << "用例 " << ran << " · 断言 " << g_checks << " · 失败 " << g_failed
              << " · 已知偏差 " << g_knownOpen;
    if (g_knownFixed > 0) std::cout << " · 已知偏差已修复 " << g_knownFixed;
    std::cout << "\n";
    return g_failed == 0 ? 0 : 1;
}

// ================================================================ 实现偏差与口径记录
// 本用例集在编写过程中发现并登记过的实现偏差，**现已全部修复**，对应断言已从
// checkKnown（XFAIL）升为 check（硬门禁），保留在此备查以防回归：
//
//   · partial 不反映"查询前那次刷盘失败"（failuresBefore 快照取在 flushLocked 之后）。
//     现由 obs02_backend_failure_counted_and_recovers 守着。
//   · partial 不反映"序号缺口 / 溢出丢弃"。现由 nfr06_seq_gap_detectable_after_loss 与
//     wrt06_buffer_overflow_is_bounded_and_counted 守着（后者还覆盖"丢弃不可逆"）。
//   · gaps() 未按设备分组 → 多设备恒报 0 段缺口。现由 gap04_multi_device_gaps_are_reported 守着。
//   · gaps() 大区间静默截断（内部按 maxRowsPerQuery 分页）→ 缺口落在后段时报 0 段。
//     现由 gap04_gaps_detects_hole / gap04_multi_device_gaps_are_reported 守着（内部走 ScanOnly）。
//   · replay() 曾把 q.limit 乘上 everyNth → 一稀疏化就被"超限拒绝"挡掉、返回 0 条。
//     现由 rpl04_every_nth_sampling 守着。
//   · 段文件后端读缓存命中时把内容 move 走 → 同一段第 2 次读返回空。
//     现由 segfile_same_range_query_is_repeatable（同一段连读 4 次 + 游标翻页）与
//     segfile_cursor_pagination_continues_across_segments（第 3 页）守着。
//   · Status::rejectedByLimit 曾是死计数器；现在"limit > maxRowsPerQuery"会真的拒绝执行，
//     由 qry04_limit_above_max_is_rejected_not_truncated 覆盖（含 rejectReason 与计数）。
//   · truncatedByRetention 曾把"本来就没数据"误报成"被清理过"，且容量裁剪后不再报截断。
//     现由 qry05_coverage_meta_and_truncated_by_retention（两种口径各一条断言）与
//     ret05_capacity_limit_trims_oldest（容量裁剪后必须报截断）守着。
//
// 两条"实现比设计更保守 / 口径待确认"的观察（不作为偏差，已在用例注释里写明）：
//   · append 缓冲满时**先 flush 腾空间再谈淘汰**（src/store.cc 的 append 溢出分支），
//     所以后端健在时 BufferFullPolicy::DropOldest 实际上不会触发（设计 §4.1 只写了
//     "淘汰最旧 + 计数"）。用例 wrt06 用"后端拒收"这一幕验证 DropOldest 确实会计数。
//   · 容量淘汰只有布尔标记、没有条数（TLM-RET-05 口径待确认）：RetentionReport::capacityTrimmed
//     只回答"这次裁了没有"，Status::droppedByOverflow 的语义是"缓冲溢出丢弃"，purgedRows
//     只统计时间清理。ret05 因此断言的是可观测等价事实（capacityTrimmed=true、approxRows()
//     回到上限、最旧优先、淘汰量可由公开口径复算）。建议：Status 增加 capacityEvictedRows，
//     或明确"容量淘汰不属于 purgedRows"。

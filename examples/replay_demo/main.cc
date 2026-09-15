// examples/replay_demo/main.cc · 全流程演示
//
//   写入 → 查询（含分页续查）→ 回放（含 everyNth / minInterval 稀疏化对比）
//        → 补发 since（单一水位 + 每设备水位）→ 缺口 gaps → 保留 sweep → 状态 status
//
// 对应需求：TLM-ADP-01..04（注入形态 / 信封）· TLM-WRT-01..08 · TLM-QRY-01..05
//           TLM-RPL-01..04 · TLM-GAP-01..05 · TLM-RET-01..05 · TLM-OBS-01..03
//
// 为什么这么写：
//   · 用**完整注入形态**（后端 / 时钟 / 策略 / 日志四个依赖全部由宿主给），
//     这正是"装配点在宿主里"的演示：换存储、换时钟、换保留口径都不用改本模块一行代码。
//   · 日志口自己实现成打到 stdout，这样模块内部发生了什么（清理、缓冲溢出…）在演示里看得见；
//     模块本身**不打印任何东西**（log_sink.h / P2）。
//   · 所有断言都是"独立算一遍对照答案"，不是把实际值打印出来当结论——
//     演示同时也是这段接口的可执行说明；它还当过回归哨兵：曾经抓出
//     "replay + everyNth 一稀疏化就返回 0 条" 与 "gaps 不传 devices 就漏报缺口" 两个坑（均已修正）。
//
// 运行： build\bin\Release\example_replay.exe
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "telemetry_store/backends/memory_backend.h"
#include "telemetry_store/console.h"
#include "telemetry_store/store.h"

using namespace telemetry_store;

namespace {

// ---------------------------------------------------------------- 断言（与 tests/selftest.cc 同风格）
int g_checks = 0;
int g_failed = 0;

void check(bool ok, const std::string& what) {
    ++g_checks;
    if (!ok) {
        ++g_failed;
        std::cout << "     FAIL " << what << "\n";
    }
}

template <typename A, typename B>
void checkEq(const A& got, const B& want, const std::string& what) {
    ++g_checks;
    if (!(got == want)) {
        ++g_failed;
        std::cout << "     FAIL " << what << "  期望=" << want << " 实际=" << got << "\n";
    }
}

const char* yn(bool b) { return b ? "是" : "否"; }

// ---------------------------------------------------------------- 演示用注入实现

/// 日志出口（TLM-OBS-03）：模块不越权打印，宿主注入才有输出。
/// 加锁是因为保留维护线程也会调 log()。
class StdoutLogSink : public ILogSink {
public:
    void log(const std::string& category, const nlohmann::json& payload) override {
        std::lock_guard<std::mutex> lk(mtx_);
        std::cout << "     [log][" << category << "] " << payload.dump() << "\n";
    }

private:
    std::mutex mtx_;
};

// ---------------------------------------------------------------- 小工具

/// epoch 毫秒 → "YYYY-MM-DD HH:MM:SS.mmm"（UTC）。
/// 为什么要自己算：纯粹为了打印好读，又不想踩 localtime 的平台差异/告警，
/// 所以用纯算术的 civil-from-days（Howard Hinnant 公有领域算法）。
std::string fmtTs(Millis ms) {
    const std::int64_t secs = ms / 1000;
    const std::int64_t msec = ms - secs * 1000;
    std::int64_t days = secs / 86400;
    std::int64_t rem  = secs - days * 86400;
    if (rem < 0) { rem += 86400; --days; }

    const std::int64_t z   = days + 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const std::int64_t doe = z - era * 146097;
    const std::int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const std::int64_t y   = yoe + era * 400;
    const std::int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const std::int64_t mp  = (5 * doy + 2) / 153;
    const std::int64_t d   = doy - (153 * mp + 2) / 5 + 1;
    const std::int64_t m   = mp < 10 ? mp + 3 : mp - 9;
    const std::int64_t yy  = m <= 2 ? y + 1 : y;

    char buf[40];
    std::snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld %02lld:%02lld:%02lld.%03lld",
                  static_cast<long long>(yy), static_cast<long long>(m),
                  static_cast<long long>(d), static_cast<long long>(rem / 3600),
                  static_cast<long long>((rem % 3600) / 60), static_cast<long long>(rem % 60),
                  static_cast<long long>(msec));
    return std::string(buf);
}

// ---------------------------------------------------------------- 演示数据口径

const char* kDevices[] = {"uav-01", "uav-02", "uav-03"};
const int   kDeviceCount = 3;
const int   kTicks    = 60;    ///< 60 个节拍 = 60 秒
const int   kPeriodMs = 1000;  ///< 1 条/秒（= StoreOptions::nominalPeriodMs 默认值）
const int   kHoleFrom = 20;    ///< uav-02 的 [20, 29] 节拍**人为不写** → 制造一段缺口
const int   kHoleTo   = 29;

/// 像样的位置报文：数值随节拍缓慢变化，人眼能看出轨迹。
nlohmann::json posPayload(int devIndex, int tick) {
    const double t = static_cast<double>(tick);
    return nlohmann::json{
        {"deviceId", kDevices[devIndex]},
        {"lng", 116.400 + 0.010 * devIndex + 0.0004 * t},
        {"lat", 39.900 + 0.010 * devIndex + 0.0002 * t},
        {"alt", 120.0 + 15.0 * devIndex + 6.0 * std::sin(t * 0.2)},
        {"speed", 12.0 + 2.0 * std::sin(t * 0.5)}};
}

/// 按统一规则灌一批合成数据。返回实际写入条数。
/// 用 appendBatch 是因为宿主手上通常已是整批（TLM-WRT-03）；seq 由模块补齐（TLM-WRT-05）。
std::size_t writeSyntheticData(Store& store, Millis base) {
    std::size_t written = 0;
    for (int tick = 0; tick < kTicks; ++tick) {
        std::vector<Record> batch;
        for (int d = 0; d < kDeviceCount; ++d) {
            if (d == 1 && tick >= kHoleFrom && tick <= kHoleTo) continue;  // uav-02 掉线丢数
            Record r;
            r.deviceId = kDevices[d];
            r.type     = "uav.pos";
            r.ts       = base + static_cast<Millis>(tick) * kPeriodMs;
            r.data     = posPayload(d, tick);
            batch.push_back(std::move(r));
        }
        written += batch.size();
        store.appendBatch(std::move(batch));
    }
    return written;
}

/// 独立算一遍"某设备在 ts > watermark 的窗口内应有多少条"——给补发断言一个对照答案。
std::size_t expectAfter(const std::string& dev, Millis watermark, Millis base) {
    std::size_t n = 0;
    for (int tick = 0; tick < kTicks; ++tick) {
        if (dev == "uav-02" && tick >= kHoleFrom && tick <= kHoleTo) continue;  // 空档里本来就没数据
        if (base + static_cast<Millis>(tick) * kPeriodMs > watermark) ++n;
    }
    return n;
}

}  // namespace

int main() {
    consoleUtf8();  // Windows 控制台默认 GBK：先切到 UTF-8，否则下面的中文是乱码
    std::cout << "== telemetry-store · 全流程演示 ==\n";

    // ───────────────────────────────────────────────────────── 0. 装配（注入形态）
    MemoryBackend          backend;
    SystemClock            clock;
    UniformRetentionPolicy policy(7LL * 24 * 3600 * 1000);  // 全体类型留 7 天（TLM-RET-01）
    StdoutLogSink          log;

    StoreOptions opt;
    opt.flush.maxRecords = 50;  // 演示里把阈值调小，好在 flushes 里看清批量刷盘的节拍
    opt.flush.maxDelayMs = 0;   // 只按条数刷（生产默认 1000 条 / 200 ms，TLM-WRT-02）
    // 保留巡检线程保持默认开启（TLM-RET-02）；演示跑不到 60 s，所以它不会插手。
    Store store(opt, backend, clock, policy, &log);

    const Millis nowMs = clock.nowMs();
    const Millis base  = (nowMs / 1000) * 1000 - 90 * 1000;  // 数据窗口从 90 秒前开始
    const Millis windowFrom = base;                                              // [from, to)
    const Millis windowTo   = base + static_cast<Millis>(kTicks) * kPeriodMs;

    std::cout << "[0] 装配：后端=" << store.backendName()
              << " · 时钟=SystemClock · 策略=UniformRetentionPolicy(7天) · 日志=StdoutLogSink\n"
              << "     数据窗口 " << fmtTs(windowFrom) << " → " << fmtTs(windowTo)
              << " UTC（" << kDeviceCount << " 台 × 1 条/秒 × " << kTicks << " 秒）\n";

    // ───────────────────────────────────────────────────────── 1. 写入
    std::cout << "[1] 写入（TLM-WRT-01/03/05/07）\n";
    const std::size_t written = writeSyntheticData(store, base);
    store.flush();  // 返回即已交付后端；失败会留在缓冲里下轮重试，不静默丢
    const Status st1 = store.status();
    checkEq(st1.appended, static_cast<std::uint64_t>(written), "appended = 实际写入条数");
    checkEq(st1.persisted, static_cast<std::uint64_t>(written), "flush 后全部落库");
    checkEq(st1.buffered, static_cast<std::uint64_t>(0), "flush 后缓冲深度为 0");
    checkEq(st1.droppedByOverflow, static_cast<std::uint64_t>(0), "没有任何静默丢弃（P2）");
    std::cout << "     写入 " << written << " 条（uav-02 的节拍 " << kHoleFrom << ".." << kHoleTo
              << " 人为留空）· flushes=" << st1.flushes
              << " · lastWriteMs=" << st1.lastWriteMs << "\n";

    // ───────────────────────────────────────────────────────── 2. 查询
    std::cout << "[2] 查询（TLM-QRY-01..05）\n";
    Query q;
    q.fromTs = windowFrom;
    q.toTs   = windowTo;  // 左闭右开：最后一条 ts = windowTo-1000 也在区间内
    const QueryResult all = store.query(q);
    checkEq(all.rows.size(), written, "无过滤查询命中全部记录");
    check(!all.hasMore && !all.partial, "未超限时 hasMore/partial 都为 false（不误报截断）");
    checkEq(all.coveredFromTs, windowFrom, "coveredFromTs = 实际最早一条");
    checkEq(all.coveredToTs, windowTo - kPeriodMs, "coveredToTs = 实际最晚一条");
    std::cout << "     · 全窗口：" << all.rows.size() << " 条 · covered=[+" << (all.coveredFromTs - base)
              << "ms, +" << (all.coveredToTs - base) << "ms] · scanned=" << all.scanned
              << " · hasMore=" << yn(all.hasMore) << "\n";

    Query one;
    one.devices = {"uav-01"};
    one.types   = {"uav.pos"};
    one.fromTs  = windowFrom;
    one.toTs    = windowTo;
    const QueryResult uav1 = store.query(one);
    checkEq(uav1.rows.size(), static_cast<std::size_t>(kTicks), "uav-01 无空档 → 60 条");
    std::cout << "     · 单设备+类型过滤：uav-01/uav.pos → " << uav1.rows.size() << " 条\n";

    // 分页续查（D8）：超限截断必须给游标，不许静默丢（P4）
    Query pageQ = q;
    pageQ.limit = 50;
    QueryResult page = store.query(pageQ);
    const Cursor firstCursor = page.cursor;  // 第一页末尾的位置：续查就从这里往下取
    std::size_t pages = 1, pagedRows = page.rows.size();
    checkEq(page.rows.size(), static_cast<std::size_t>(50), "第一页刚好一整页 50 条");
    check(page.hasMore && firstCursor.valid, "还有数据时给出有效游标（不静默截断，D8/P4）");
    while (page.hasMore && pages < 100) {  // 100 页是防御性上限，正常流程到不了
        page = store.queryPage(pageQ, page.cursor);
        ++pages;
        pagedRows += page.rows.size();
    }
    checkEq(pagedRows, written, "分页续查合计 = 全量（没有漏条）");
    check(!page.hasMore, "最后一页 hasMore=false（不假装还有，也不静默丢）");
    std::cout << "     · 分页 limit=50：共 " << pages << " 页 / " << pagedRows
              << " 条 · 第 1 页末尾游标=(ts+" << (firstCursor.ts - base) << "ms, "
              << firstCursor.deviceId << ", " << firstCursor.type << ")\n";

    Query old;
    old.fromTs = windowFrom - 60 * 1000;  // 早于窗口：这些数据**从来就不存在**
    old.toTs   = windowFrom;
    const QueryResult oldRes = store.query(old);
    checkEq(oldRes.rows.size(), static_cast<std::size_t>(0), "窗口之前没有数据");
    // ⚠️ 这里必须是 false：此刻**还没清理过任何数据**。
    // "查得更早 → 就报被清理过"是错的 —— 那等于**谎报数据丢失**（窗口之前本来就没数据）。
    // 真正"被清理"的场景在 [6] 段之后：sweep 删过数据，同区间查询才必须为 true。
    check(!oldRes.truncatedByRetention,
          "从未清理过 → 不谎报 truncatedByRetention（TLM-QRY-05 / P4）");
    std::cout << "     · 越界查询 [窗口前 60s, 窗口起点)：" << oldRes.rows.size()
              << " 条 · truncatedByRetention=" << yn(oldRes.truncatedByRetention)
              << "（从未清理过 → 否，不谎报丢失）\n";

    // P4：请求的 limit **本身**超过 maxRowsPerQuery → 拒绝执行 + 给原因，
    // 而不是悄悄只给一部分（设计 §6.3 / TLM-OBS-02 的 rejectedByLimit 计数）
    Query overQ = q;
    overQ.limit = 20000;  // > maxRowsPerQuery（默认 10000）
    const QueryResult overRes = store.query(overQ);
    check(overRes.rejected && overRes.rows.empty(), "超上限的 limit 被拒绝执行（rejected=true + 空结果）");
    std::cout << "     · limit=20000（> maxRowsPerQuery=10000）：rejected=" << yn(overRes.rejected)
              << " · rows=" << overRes.rows.size() << " · reason=" << overRes.rejectReason << "\n";

    // ───────────────────────────────────────────────────────── 3. 回放
    std::cout << "[3] 回放（TLM-RPL-01..04）\n";
    ReplayRequest rq;
    rq.deviceId = "uav-01";
    rq.q.fromTs = windowFrom;
    rq.q.toTs   = windowTo;
    // limit 口径：这是**采样后**的样本上限（0 = 用 StoreOptions::maxRowsPerQuery），
    // 候选数量由内部扫描上限自然约束 —— 所以 everyNth 给多大都不用跟着调 limit。
    // （历史坑：早期实现把 q.limit 乘上 everyNth 去取候选，结果 2000×5 越过 maxRowsPerQuery
    //   被"超限拒绝"挡掉，"一稀疏化就 0 条"；本示例跑出来过，已修正。）
    rq.q.limit = 0;

    rq.everyNth = 1;
    const Timeline full = store.replay(rq);
    checkEq(full.samples.size(), static_cast<std::size_t>(kTicks), "everyNth=1 → uav-01 全部 60 条");
    check(!full.sampled, "未做稀疏化时 sampled=false");
    checkEq(full.durationMs, static_cast<Millis>((kTicks - 1) * kPeriodMs),
            "时间轴 durationMs = 首末样本之差（TLM-RPL-01）");
    bool uniform = true;  // 1 条/秒的数据，相邻样本间隔应恒为 kPeriodMs
    for (std::size_t i = 1; i < full.samples.size(); ++i) {
        if (full.samples[i].ts - full.samples[i - 1].ts != kPeriodMs) uniform = false;
    }
    check(uniform, "时间轴样本相邻间隔恒为 1 秒 → 按 ts 差播放即可");
    std::cout << "     · 全量时间轴：" << full.samples.size() << " 条 · durationMs=" << full.durationMs
              << " · 前 2 条：+" << (full.samples[0].ts - base) << "ms / +"
              << (full.samples[1].ts - base) << "ms（相邻差恒为 " << kPeriodMs << "ms）\n";

    std::cout << "     · 稀疏化对比（同一区间，候选 60 条）：\n";
    struct Variant {
        const char* what;
        std::size_t everyNth;
        Millis      minIntervalMs;
    };
    const Variant variants[] = {
        {"everyNth=1, minInterval=0   ", 1, 0},
        {"everyNth=5, minInterval=0   ", 5, 0},
        {"everyNth=1, minInterval=5000", 1, 5000},
        {"everyNth=2, minInterval=5000", 2, 5000},
    };
    std::size_t counts[4] = {0, 0, 0, 0};
    for (int i = 0; i < 4; ++i) {
        ReplayRequest rr = rq;
        rr.everyNth      = variants[i].everyNth;
        rr.minIntervalMs = variants[i].minIntervalMs;
        const Timeline tl = store.replay(rr);
        counts[i] = tl.samples.size();
        std::cout << "         " << variants[i].what << " → " << tl.samples.size()
                  << " 条 · sampled=" << yn(tl.sampled) << " step=" << tl.sampleStep
                  << " · 首末=+" << (tl.samples.empty() ? Millis(0) : tl.samples.front().ts - base)
                  << "/+" << (tl.samples.empty() ? Millis(0) : tl.samples.back().ts - base) << "ms\n";
    }
    check(counts[1] < counts[0], "everyNth=5 比全量稀疏");
    check(counts[1] > 0, "稀疏化不等于清空：everyNth=5 仍必须有样本");
    check(counts[2] < counts[0], "minInterval=5000 比全量稀疏");
    check(counts[3] <= counts[1] && counts[3] <= counts[2],
          "everyNth 与 minInterval 同时给出 → 取更稀疏者（TLM-RPL-03）");

    // 采样上限口径的回归检查：默认 limit（0 → maxRowsPerQuery）+ everyNth=5 也必须拿到样本。
    // （曾经这里返回 0 条：内部把 limit 放大 5 倍被拒答，而时间轴上没有 rejected 字段，
    //   调用方分不清"被拒"和"没数据" —— 本示例抓出过，现在当回归哨兵留着。）
    {
        ReplayRequest d = rq;
        d.q.limit  = 0;  // 0 = 用 StoreOptions::maxRowsPerQuery
        d.everyNth = 5;
        const Timeline t = store.replay(d);
        check(!t.samples.empty(), "默认 limit 下 everyNth=5 仍必须有样本（稀疏化 ≠ 清空）");
        checkEq(t.samples.size(), counts[1], "默认 limit 与显式 limit 下稀疏结果一致");
        std::cout << "     · 默认口径（limit=0）+ everyNth=5 → " << t.samples.size() << " 条\n";
    }

    ReplayRequest mr;  // 不指定 deviceId：多设备归并成一条时间轴（TLM-RPL-02）
    mr.q.fromTs = windowFrom;
    mr.q.toTs   = windowTo;
    const Timeline merged = store.replay(mr);
    checkEq(merged.samples.size(), written, "多设备回放 = 归并后的全量时间轴");
    bool sorted = true;
    for (std::size_t i = 1; i < merged.samples.size(); ++i) {
        if (Record::lessByKey(merged.samples[i], merged.samples[i - 1])) sorted = false;
    }
    check(sorted, "归并时间轴全程按 (ts, deviceId, type) 升序（D5）");
    std::cout << "     · 多设备归并：" << merged.samples.size() << " 条 · 升序=" << yn(sorted)
              << " · partial=" << yn(merged.partial)
              << " · truncatedByRetention=" << yn(merged.truncatedByRetention) << "\n";

    // ───────────────────────────────────────────────────────── 4. 补发
    std::cout << "[4] 补发 since（TLM-GAP-01/02/03）\n";
    const Millis watermark = base + 30 * kPeriodMs;  // 客户端说"我最后见到的是 +30s 那条"
    SinceRequest sr;
    sr.devices    = {"uav-01", "uav-02", "uav-03"};
    sr.lastSeenTs = watermark;
    const Timeline tl = store.since(sr);
    std::size_t expectSingle = 0;
    for (int d = 0; d < kDeviceCount; ++d) expectSingle += expectAfter(kDevices[d], watermark, base);
    checkEq(tl.samples.size(), expectSingle, "单一水位补发条数符合预期");
    check(!tl.samples.empty() && tl.samples.front().ts == watermark + kPeriodMs,
          "补发首条 = 水位的下一拍（严格大于，不重复下发）");
    bool allAfter = true;
    for (const Record& r : tl.samples) {
        if (r.ts <= watermark) allAfter = false;
    }
    check(allAfter, "补发结果全部 ts > 水位");
    std::cout << "     · 单一水位 +30s → " << tl.samples.size() << " 条 · 首条 +"
              << (tl.samples.front().ts - base) << "ms · 全部 ts>水位=" << yn(allAfter) << "\n";

    SinceRequest pr;  // 每设备各自水位：断线时间不同的客户端（TLM-GAP-02）
    pr.perDevice = {{"uav-01", base + 50 * kPeriodMs},
                    {"uav-02", base + 10 * kPeriodMs},
                    {"uav-03", base + 55 * kPeriodMs}};
    const Timeline ptl = store.since(pr);
    std::size_t expectPer = 0;
    for (const auto& kv : pr.perDevice) expectPer += expectAfter(kv.first, kv.second, base);
    checkEq(ptl.samples.size(), expectPer, "每设备水位补发条数符合预期");
    std::cout << "     · 每设备水位 {uav-01:+50s, uav-02:+10s, uav-03:+55s} → " << ptl.samples.size()
              << " 条（uav-02 含它空档之后补上的数据）\n";

    // ───────────────────────────────────────────────────────── 5. 缺口识别
    std::cout << "[5] 缺口识别 gaps（TLM-GAP-04/05）\n";
    for (int d = 0; d < kDeviceCount; ++d) {
        const std::vector<Gap> gs = store.gaps(windowFrom, windowTo, 0, {kDevices[d]});
        std::cout << "     · " << kDevices[d] << "：" << gs.size() << " 段缺口";
        for (const Gap& g : gs) {
            std::cout << " [+" << (g.fromTs - base) << "ms, +" << (g.toTs - base)
                      << "ms) durationMs=" << g.durationMs << " seqBefore=" << g.seqBefore
                      << " seqAfter=" << g.seqAfter;
        }
        std::cout << "\n";
        if (d == 1) {
            checkEq(gs.size(), static_cast<std::size_t>(1), "uav-02 应识别出 1 段缺口");
            checkEq(gs[0].durationMs, static_cast<Millis>((kHoleTo - kHoleFrom + 2) * kPeriodMs - 1),
                    "缺口时长 = 前一条之后(含) 到 后一条之前(不含)");
        } else {
            checkEq(gs.size(), static_cast<std::size_t>(0), "无空档的设备不应报缺口");
        }
    }
    // 阈值是参数（默认 = nominalPeriodMs × 3 = 3000 ms）：调大到超过缺口宽度就不再算缺口
    const std::vector<Gap> wide = store.gaps(windowFrom, windowTo, 20000, {"uav-02"});
    checkEq(wide.size(), static_cast<std::size_t>(0), "阈值 20000ms > 缺口宽度 → 不算缺口（D9 阈值法）");
    std::cout << "     · 阈值 20000ms 下 uav-02：" << wide.size() << " 段（阈值法，不做失联语义）\n";
    std::cout << "       （seqBefore/seqAfter 是**进程内全局**序号：两者之间还夹着其它设备的记录，"
                 "不能直接当成「这台设备少了几条」来看）\n";
    // 多设备一次调用：必须同样找到这 1 段缺口。
    // （历史坑：实现在排序结果上比较"相邻两行"，而多设备交错时相邻两行几乎总是不同设备，
    //   于是不传 devices 就永远报 0 段缺口 —— 本示例抓出过，这里留成回归哨兵。）
    const std::vector<Gap> allDev = store.gaps(windowFrom, windowTo, 0);
    checkEq(allDev.size(), static_cast<std::size_t>(1), "不传 devices（全部设备）也能识别出这 1 段缺口");
    if (!allDev.empty()) {
        checkEq(allDev[0].deviceId, std::string("uav-02"), "缺口归属设备正确");
        checkEq(allDev[0].durationMs, static_cast<Millis>((kHoleTo - kHoleFrom + 2) * kPeriodMs - 1),
                "逐设备查与全设备查给出的缺口区间一致");
    }
    std::cout << "     · 不传 devices（全部设备一次调用）：" << allDev.size() << " 段 · 归属="
              << (allDev.empty() ? std::string("-") : allDev[0].deviceId) << "\n";

    // ───────────────────────────────────────────────────────── 6. 保留巡检
    std::cout << "[6] 保留巡检 sweep（TLM-RET-01..05）\n";
    const RetentionReport rep = store.sweep();
    check(rep.supported, "内存后端 canPurge()=true → 保留不是降级形态");
    checkEq(rep.purgedRows, static_cast<std::uint64_t>(0), "TTL 7 天 > 数据年龄 90 秒 → 一条都不该清");
    check(rep.cutoffTs > 0, "本轮清理截止点已算出（now - 7 天）");
    std::cout << "     · 后端=" << rep.backend << " · cutoff=" << fmtTs(rep.cutoffTs)
              << " · 清理 " << rep.purgedRows << " 条 · 容量裁剪=" << yn(rep.capacityTrimmed) << "\n";

    // 6b. 反证：把时钟拨到 8 天后，同一份数据就会被清掉——
    //     IClock 只回答"现在几点"，不产生记录时间（clock.h 的分界），
    //     所以假时钟能让"保留"变成可确定性验证的行为（TLM-ADP-02）。
    MemoryBackend          backend2;
    UniformRetentionPolicy policy2(7LL * 24 * 3600 * 1000);
    FakeClock              fakeClock(nowMs + 8LL * 24 * 3600 * 1000);
    StoreOptions opt2;
    opt2.enableSweeper = false;  // 关掉维护线程，让这一段完全由本线程驱动、可确定
    Store store2(opt2, backend2, fakeClock, policy2, &log);
    const std::size_t written2 = writeSyntheticData(store2, base);
    store2.flush();
    const RetentionReport rep2 = store2.sweep();
    checkEq(written2, written, "两份演示数据的口径一致");
    checkEq(rep2.purgedRows, static_cast<std::uint64_t>(written), "时钟拨到 8 天后，7 天 TTL 把数据全清了");
    checkEq(store2.status().persisted - store2.status().purgedRows, static_cast<std::uint64_t>(0),
            "清理后剩下的条数为 0");
    std::cout << "     · 假时钟拨到 8 天后重跑：清理 " << rep2.purgedRows << " 条 · cutoff="
              << fmtTs(rep2.cutoffTs) << " ← 保留策略真的在起作用（假时钟可确定验证）\n";

    // ───────────────────────────────────────────────────────── 7. 状态快照
    std::cout << "[7] 状态快照 status（TLM-OBS-01/02）\n";
    const Status s = store.status();
    checkEq(s.appended, static_cast<std::uint64_t>(written), "appended 与写入量一致");
    checkEq(s.persisted, static_cast<std::uint64_t>(written), "persisted 与写入量一致");
    checkEq(s.seqLast - s.seqFirst + 1, static_cast<std::uint64_t>(written),
            "序号区间宽度 = 条数 → 没有缺口（P3）");
    check(s.thresholdState == "ok", "后端无连续失败 → 阈值状态 ok");
    std::cout << "     · appended=" << s.appended << " persisted=" << s.persisted
              << " buffered=" << s.buffered << " flushes=" << s.flushes
              << " dropped=" << s.droppedByOverflow << " writeFailures=" << s.backendWriteFailures
              << " rejectedByLimit=" << s.rejectedByLimit
              << "\n     · seq=[" << s.seqFirst << ", " << s.seqLast << "] avgWriteMs=" << s.avgWriteMs
              << " thresholdState=" << s.thresholdState << " backend=" << s.backend
              << " version=" << s.version << "\n";
    std::cout << "     · toJson() 快照（可直接喂监控）：" << s.toJson().dump() << "\n";

    // ───────────────────────────────────────────────────────── 8. 信封接入
    std::cout << "[8] 信封接入 fromEnvelope（TLM-ADP-04）\n";
    const nlohmann::json envelope = {{"type", "link.quality"},
                                     {"ts", base - 1000},
                                     {"data", {{"deviceId", "gw-01"}, {"rssi", -71}, {"loss", 0.02}}}};
    Record rec;
    const bool ok = Store::fromEnvelope(envelope, rec);
    check(ok, "标准信封应能解析成 Record");
    checkEq(rec.deviceId, std::string("gw-01"), "deviceId 从 data.deviceId 取（模块不猜业务字段）");
    checkEq(rec.type, std::string("link.quality"), "type 原样透传");
    checkEq(rec.ts, base - 1000, "ts 原样透传");
    checkEq(rec.seq, static_cast<std::uint64_t>(0), "seq 留 0，由 append 补齐");
    const nlohmann::json bad = {{"type", "link.quality"}, {"ts", base}};  // 缺 data
    check(!Store::fromEnvelope(bad, rec), "缺字段 → 返回 false，不抛异常");
    if (ok) store.append(rec);  // 解析结果照常走 append 主路径
    store.flush();
    Query envQ;
    envQ.fromTs = base - 1000;
    envQ.toTs   = base;  // 这条在数据窗口之外，所以不影响上面的统计
    const QueryResult envRows = store.query(envQ);
    checkEq(envRows.rows.size(), static_cast<std::size_t>(1), "信封来的记录同样能查到");
    std::cout << "     · 信封 → " << rec.deviceId << " " << rec.type << " ts+" << (rec.ts - base)
              << "ms data=" << rec.data.dump() << " · 缺 data 的信封返回 false="
              << yn(!Store::fromEnvelope(bad, rec)) << "\n";

    std::cout << "== 演示结束：断言 " << g_checks << " · 失败 " << g_failed << " ==\n";
    return g_failed == 0 ? 0 : 1;
}

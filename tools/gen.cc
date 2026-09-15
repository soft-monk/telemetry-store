// tools/gen.cc · 造数 / 压测工具：ts_gen
//
// 用途（TLM-NFR-01 的验收工具）：手边没有真设备、也没有真实数据流时，
// 先造出 1000 条/s 的周期遥测，把写入路径压起来；需要时还能导出成文件喂给别的工具。
//
//   ts_gen --devices 1000 --hz 1 --seconds 10 [--type uav.pos] [--out <目录>] [--csv]
//
// 两种模式：
//   --mode realtime   （默认）按 hz 真的走节拍，等于"持续写入"验收：能看出跟不跟得上；
//   --mode throughput  不计节拍、尽快灌满，用来测写入路径的吞吐上限（条/s 与延迟分位数）。
//
// 为什么逐条调 append 而不是 appendBatch：本工具要给出**每条 append 的延迟分位数**
// （TLM-NFR-01 的 P95 ≤ 1 ms 就是这条路径的指标）。appendBatch 语义与逐条完全一致
// （store.cc 里就是 for 循环调 append），宿主手上已是整批时用哪个都一样。
//
// 落盘有两种用法，互不冲突：
//   --segfile DIR  生成阶段直接写段文件后端（SegmentFileBackend）：真实落盘、跨重启留存。
//   --out DIR      把 store 里的数据**分页查回来**再导出成交换格式（每行一条）：
//                    {"deviceId":...,"type":...,"ts":...,"seq":...,"data":{...}}
//                  （--csv 改成 CSV）。两者可组合：写入落段文件，同时导出一份给人看/给别的工具吃。
//
// 运行： build\bin\Release\ts_gen.exe --devices 1000 --hz 1 --seconds 5
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "telemetry_store/backends/memory_backend.h"
#include "telemetry_store/backends/segment_file.h"
#include "telemetry_store/console.h"
#include "telemetry_store/store.h"

using namespace telemetry_store;
namespace fs = std::filesystem;

namespace {

// ================================================================ 参数
struct Options {
    long long   devices    = 10;          ///< 设备台数
    double      hz         = 1.0;         ///< 每台每秒上报条数
    double      seconds    = 10.0;        ///< 造数时长（秒）
    std::string type       = "uav.pos";   ///< 事件类型
    std::string mode       = "realtime";  ///< realtime | throughput
    std::string out;                      ///< 非空 = 导出到该目录（JSONL / CSV）
    std::string segfile;                  ///< 非空 = 生成阶段直接写段文件后端
    bool        csv        = false;       ///< 导出成 CSV 而不是 JSONL
    double      ttlSec     = 0.0;         ///< 保留 TTL（秒），0 = 永久保留
    unsigned long long seed = 20240501ULL;///< 随机种子（同样的种子给同样的数据）
    long long   maxRecords = 2000000;     ///< 安全上限：超过直接报错，避免把内存灌爆
    bool        quiet      = false;       ///< 只打印最终统计
};

void printUsage() {
    std::cout <<
        "ts_gen · 造数 / 压测工具（telemetry-store）\n"
        "\n"
        "用法:\n"
        "  ts_gen [选项]\n"
        "\n"
        "选项:\n"
        "  --devices N       设备台数（默认 10）\n"
        "  --hz H            每台每秒上报条数，可小数（默认 1）\n"
        "  --seconds S       造数时长秒数（默认 10）\n"
        "  --type T          事件类型（默认 uav.pos；其它类型走通用负载）\n"
        "  --mode M          realtime（默认，按节拍真跑，验收 TLM-NFR-01）| throughput（尽快灌满）\n"
        "  --out DIR         额外导出到目录 DIR（JSONL，每行一条；可与其他后端组合）\n"
        "  --csv             导出成 CSV（需与 --out 一起用）\n"
        "  --segfile DIR     生成阶段直接写**段文件后端**（跨重启留存；同一目录再跑一次会累加）\n"
        "  --ttl-sec N       注入 UniformRetentionPolicy 的 TTL 秒数（0 = 永久保留，默认 0）\n"
        "  --seed N          随机种子（默认 20240501，同种子同数据）\n"
        "  --max-records N   条数安全上限（默认 2000000）\n"
        "  --quiet           只打印最终统计\n"
        "  -h, --help        显示本帮助\n"
        "\n"
        "示例:\n"
        "  ts_gen --devices 1000 --hz 1 --seconds 5\n"
        "  ts_gen --devices 1000 --hz 1 --seconds 5 --out .\\build\\out --csv\n"
        "  ts_gen --devices 200 --hz 50 --seconds 20 --mode throughput\n";
}

bool parseLL(const char* s, long long& out) {
    if (s == nullptr || *s == '\0') return false;
    char* end = nullptr;
    errno = 0;
    const long long v = std::strtoll(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') return false;
    out = v;
    return true;
}

bool parseULL(const char* s, unsigned long long& out) {
    if (s == nullptr || *s == '\0') return false;
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0') return false;
    out = v;
    return true;
}

bool parseDouble(const char* s, double& out) {
    if (s == nullptr || *s == '\0') return false;
    char* end = nullptr;
    errno = 0;
    const double v = std::strtod(s, &end);
    if (errno != 0 || end == s || *end != '\0' || !std::isfinite(v)) return false;
    out = v;
    return true;
}

/// 返回值：0 = 正常继续；1 = 已打印帮助/错误，直接退出（exitCode 给调用方）。
bool parseArgs(int argc, char** argv, Options& o, bool& exitNow, int& exitCode, std::string& err) {
    exitNow  = false;
    exitCode = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        // 取下一个参数；缺了就报错
        auto value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                err = std::string("选项 ") + name + " 缺少参数";
                return nullptr;
            }
            return argv[++i];
        };

        if (a == "-h" || a == "--help") {
            printUsage();
            exitNow  = true;
            exitCode = 0;
            return false;
        } else if (a == "--devices") {
            const char* v = value("--devices");
            if (v == nullptr || !parseLL(v, o.devices)) { err = "--devices 需要一个正整数"; return false; }
        } else if (a == "--hz") {
            const char* v = value("--hz");
            if (v == nullptr || !parseDouble(v, o.hz)) { err = "--hz 需要一个正数"; return false; }
        } else if (a == "--seconds") {
            const char* v = value("--seconds");
            if (v == nullptr || !parseDouble(v, o.seconds)) { err = "--seconds 需要一个正数"; return false; }
        } else if (a == "--type") {
            const char* v = value("--type");
            if (v == nullptr || *v == '\0') { err = "--type 需要一个非空字符串"; return false; }
            o.type = v;
        } else if (a == "--mode") {
            const char* v = value("--mode");
            if (v == nullptr) return false;
            o.mode = v;
            if (o.mode != "realtime" && o.mode != "throughput") {
                err = "--mode 只支持 realtime | throughput";
                return false;
            }
        } else if (a == "--out") {
            const char* v = value("--out");
            if (v == nullptr || *v == '\0') { err = "--out 需要一个目录"; return false; }
            o.out = v;
        } else if (a == "--csv") {
            o.csv = true;
        } else if (a == "--segfile") {
            const char* v = value("--segfile");
            if (v == nullptr || *v == '\0') { err = "--segfile 需要一个目录"; return false; }
            o.segfile = v;
        } else if (a == "--ttl-sec") {
            const char* v = value("--ttl-sec");
            if (v == nullptr || !parseDouble(v, o.ttlSec)) { err = "--ttl-sec 需要一个非负数"; return false; }
        } else if (a == "--seed") {
            const char* v = value("--seed");
            if (v == nullptr || !parseULL(v, o.seed)) { err = "--seed 需要一个非负整数"; return false; }
        } else if (a == "--max-records") {
            const char* v = value("--max-records");
            if (v == nullptr || !parseLL(v, o.maxRecords)) { err = "--max-records 需要一个正整数"; return false; }
        } else if (a == "--quiet") {
            o.quiet = true;
        } else {
            err = "未知选项：" + a + "（用 --help 看用法）";
            return false;
        }
    }
    return true;
}

// ================================================================ 小工具
const char* yn(bool b) { return b ? "是" : "否"; }

/// epoch 毫秒 → "YYYY-MM-DD HH:MM:SS"（UTC）。
/// 纯算术实现（civil-from-days，Howard Hinnant 公有领域算法）：
/// 只为打印好读，不想踩 localtime 的平台差异与告警。
std::string fmtTs(Millis ms) {
    const std::int64_t secs = ms / 1000;
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
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld %02lld:%02lld:%02lld",
                  static_cast<long long>(yy), static_cast<long long>(m), static_cast<long long>(d),
                  static_cast<long long>(rem / 3600), static_cast<long long>((rem % 3600) / 60),
                  static_cast<long long>(rem % 60));
    return std::string(buf);
}

/// 文件名用时间戳："YYYYMMDD-HHMMSS"。
std::string fileStamp(Millis ms) {
    std::string s = fmtTs(ms);  // "YYYY-MM-DD HH:MM:SS"
    std::string out;
    for (char c : s) {
        if (c == '-') continue;
        if (c == ' ') { out += '-'; continue; }
        if (c == ':') continue;
        out += c;
    }
    return out;
}

/// 零依赖的伪随机：写死 LCG 而不是 <random>，是为了让"同种子同数据"成立，
/// 也避免每条记录都去构造分布对象而污染吞吐测量。
double noise01(unsigned long long seed, long long index, int salt) {
    std::uint64_t x = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    x += static_cast<std::uint64_t>(index) * 2654435761ULL;
    x += static_cast<std::uint64_t>(salt) * 40503ULL;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return static_cast<double>(x % 1000003ULL) / 1000003.0;  // [0, 1)
}

std::string deviceIdOf(long long index, int width) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "uav-%0*lld", width, index);
    return std::string(buf);
}

int digitsOf(long long n) {
    int w = 1;
    while (n >= 10) { n /= 10; ++w; }
    return w;
}

/// 造一条像样的负载。模块不看 data（原样透传、不索引），这里只是让数据有真实感。
nlohmann::json makePayload(const std::string& type, const std::string& dev, long long deviceIndex,
                           long long tick, unsigned long long seed) {
    const double n1 = noise01(seed, deviceIndex, 1);
    const double n2 = noise01(seed, deviceIndex, 2);
    const double n3 = noise01(seed, deviceIndex, 3);
    if (type == "uav.pos") {
        return nlohmann::json{
            {"deviceId", dev},
            {"lng", 116.3000 + 0.00002 * static_cast<double>(deviceIndex) + 0.00005 * n1},
            {"lat", 39.8000 + 0.00002 * static_cast<double>(deviceIndex) + 0.00005 * n2},
            {"alt", 100.0 + 200.0 * n3},
            {"speed", 5.0 + 25.0 * n1},
            {"heading", 360.0 * n2}};
    }
    // 其它类型：通用负载，字段名与 uav.pos 无关，正好说明"模块不解释业务字段"
    return nlohmann::json{
        {"deviceId", dev},
        {"value", 100.0 * n1},
        {"battery", 0.3 + 0.7 * n2},
        {"rssi", -50.0 - 40.0 * n3},
        {"tick", tick}};
}

// ================================================================ 延迟采样
/// append 延迟采样器：条数很大时按固定步长抽样，免得为了统计把内存吃满。
class LatencySampler {
public:
    explicit LatencySampler(long long total) : stride_(total > kMaxSamples ? total / kMaxSamples : 1) {}

    void add(long long index, double us) {
        if (index % stride_ == 0) {
            samples_.push_back(us);
            if (us > 1000.0) ++slow_;  // > 1 ms：TLM-NFR-01 的 P95 目标线
        }
    }

    std::size_t size() const { return samples_.size(); }
    std::uint64_t slowCount() const { return slow_; }

    /// 分位数（微秒）。p 传 0.5 / 0.95 / 0.99。
    double percentile(double p) const {
        if (samples_.empty()) return 0.0;
        std::vector<double> v = samples_;
        std::sort(v.begin(), v.end());
        const double idx = p * (static_cast<double>(v.size()) - 1.0);
        std::size_t i = static_cast<std::size_t>(idx + 0.5);
        if (i >= v.size()) i = v.size() - 1;
        return v[i];
    }

    double maxUs() const {
        double m = 0.0;
        for (double v : samples_) m = std::max(m, v);
        return m;
    }

private:
    static const long long kMaxSamples = 200000;
    long long           stride_ = 1;
    std::uint64_t       slow_   = 0;
    std::vector<double> samples_;
};

// ================================================================ 日志出口（注入用）
/// 把模块日志打到 stderr：stdout 留给"结果"，出了问题才在 stderr 上看见原因。
class StderrLogSink : public ILogSink {
public:
    void log(const std::string& category, const nlohmann::json& payload) override {
        std::lock_guard<std::mutex> lk(mtx_);
        std::cerr << "[ts_gen][log][" << category << "] " << payload.dump() << "\n";
    }

private:
    std::mutex mtx_;
};

// ================================================================ 节拍
/// 睡到目标时刻（节拍对齐用）。
///
/// 为什么要"留余量 + 自旋收尾"：Windows 上 sleep_for 的实际唤醒粒度默认约 15.6 ms，
/// "睡到还差 1 ms"会被系统多睡十几毫秒——节拍抖动甚至超过写入耗时本身，
/// 那样打出来的"迟到"就全是在测操作系统的定时器，而不是在测模块。
/// 做法是留出余量 margin = min(16ms, 0.75×周期) 后再自旋，把粒度误差吸收掉，
/// 抖动压到 1 ms 量级；代价是每拍最多自旋 16 ms（1 Hz 时约 1.6% 的 CPU）。
void sleepUntil(std::chrono::steady_clock::time_point target,
                std::chrono::microseconds          margin) {
    for (;;) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= target) return;
        const auto remain = target - now;
        if (remain > margin) {
            std::this_thread::sleep_for(remain - margin);
        } else {
            std::this_thread::yield();  // 最后一段自旋：yield 会让出时间片，不独占 CPU
        }
    }
}

/// 两位小数的数字字符串（只为输出好读）。
std::string num2(double v) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.2f", v);
    return std::string(buf);
}

// ================================================================ 导出
std::string csvEscape(const std::string& s) {
    if (s.find_first_of(",\"\n\r") == std::string::npos) return s;
    std::string out = "\"";
    for (char c : s) {
        if (c == '"') out += "\"\"";
        else out += c;
    }
    out += "\"";
    return out;
}

/// JSONL：每行一条 `{"deviceId":...,"type":...,"ts":...,"seq":...,"data":{...}}`
std::string jsonlLine(const Record& r) {
    nlohmann::json j;
    j["deviceId"] = r.deviceId;
    j["type"]     = r.type;
    j["ts"]       = r.ts;
    j["seq"]      = r.seq;
    j["data"]     = r.data.is_null() ? nlohmann::json::object() : r.data;
    return j.dump();
}

std::string csvLine(const Record& r) {
    return csvEscape(r.deviceId) + "," + csvEscape(r.type) + "," + std::to_string(r.ts) + "," +
           std::to_string(r.seq) + "," + csvEscape(r.data.dump());
}

/// 分页查回 + 落文件。分页是关键：一次性取全量会把内存顶起来（TLM-QRY-03 的游标就是为此）。
bool exportRows(Store& store, const Options& o, Millis fromTs, Millis toTs, long long expected,
                std::string& outPath, long long& rowsWritten, std::uintmax_t& bytesOut,
                std::string& err) {
    std::error_code ec;
    fs::create_directories(fs::path(o.out), ec);
    if (ec) {
        err = "创建目录失败：" + o.out + "（" + ec.message() + "）";
        return false;
    }
    const Millis nowMs = SystemClock().nowMs();
    const std::string name =
        std::string("telemetry-") + fileStamp(nowMs) + (o.csv ? ".csv" : ".jsonl");
    const fs::path path = fs::path(o.out) / name;
    outPath = path.string();

    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) {
        err = "无法写入文件：" + outPath;
        return false;
    }
    if (o.csv) f << "deviceId,type,ts,seq,data\n";

    Query q;
    q.fromTs = fromTs;
    q.toTs   = toTs;
    q.limit  = 5000;  // 每页 5000 条
    QueryResult page = store.query(q);
    long long pages = 0;
    for (;;) {
        for (const Record& r : page.rows) {
            f << (o.csv ? csvLine(r) : jsonlLine(r)) << "\n";
            ++rowsWritten;
        }
        ++pages;
        if (!o.quiet && expected > 0 && rowsWritten > 0 &&
            rowsWritten % 100000 == 0) {
            std::cout << "  … 已导出 " << rowsWritten << " / " << expected << " 条\n";
        }
        if (!page.hasMore) break;
        page = store.queryPage(q, page.cursor);
        if (page.rows.empty() && !page.hasMore) break;  // 防御：不该发生
        if (pages > 1000000) {                          // 防御：绝不允许死循环
            err = "分页导出超过 1000000 页，疑似游标不前进";
            return false;
        }
    }
    f.flush();
    if (!f) {
        err = "写文件过程中出错：" + outPath;
        return false;
    }
    f.close();
    bytesOut = fs::file_size(path, ec);
    if (ec) bytesOut = 0;
    return true;
}

}  // namespace

// ================================================================ 入口
int main(int argc, char** argv) {
    consoleUtf8();  // Windows 控制台默认 GBK：先切到 UTF-8，否则下面的中文是乱码
    Options opt;
    std::string err;
    bool exitNow = false;
    int  exitCode = 0;
    if (!parseArgs(argc, argv, opt, exitNow, exitCode, err)) {
        if (exitNow) return exitCode;
        std::cerr << "[ts_gen] 参数错误：" << err << "\n";
        return 1;
    }

    // ───────────────────────────────────────────────────────── 参数校验与规模换算
    if (opt.devices < 1) { std::cerr << "[ts_gen] --devices 必须 ≥ 1\n"; return 1; }
    if (opt.hz <= 0.0) { std::cerr << "[ts_gen] --hz 必须 > 0\n"; return 1; }
    if (opt.seconds <= 0.0) { std::cerr << "[ts_gen] --seconds 必须 > 0\n"; return 1; }
    if (opt.maxRecords < 1) { std::cerr << "[ts_gen] --max-records 必须 ≥ 1\n"; return 1; }
    if (opt.csv && opt.out.empty()) {
        std::cerr << "[ts_gen] --csv 需要与 --out 一起用\n";
        return 1;
    }

    const double  periodMs = 1000.0 / opt.hz;                          // 节拍周期
    const long long ticks  = static_cast<long long>(std::llround(opt.seconds * opt.hz));
    const long long total  = ticks * opt.devices;
    if (ticks < 1) {
        std::cerr << "[ts_gen] --seconds × --hz 不足一个节拍，至少给一拍\n";
        return 1;
    }
    if (total > opt.maxRecords) {
        std::cerr << "[ts_gen] 计划生成 " << total << " 条，超过 --max-records " << opt.maxRecords
                  << "；请调小参数或显式放宽上限（内存后端会把数据全放内存里）\n";
        return 1;
    }
    const int devWidth = std::max(3, digitsOf(opt.devices - 1));
    const bool realtime = (opt.mode == "realtime");

    const double requestedRate = static_cast<double>(opt.devices) * opt.hz;  // 计划条/s

    std::cout << "== ts_gen · telemetry-store 造数/压测 ==\n";
    std::cout << "[参数] devices=" << opt.devices << " hz=" << opt.hz << " seconds=" << opt.seconds
              << " type=" << opt.type << " mode=" << opt.mode
              << " 节拍周期=" << periodMs << "ms 节拍数=" << ticks << " 计划条数=" << total << "\n";
    if (std::fabs(opt.seconds * opt.hz - static_cast<double>(ticks)) > 1e-9) {
        std::cout << "[提示] seconds × hz = " << (opt.seconds * opt.hz)
                  << " 不是整数，按 " << ticks << " 个节拍生成\n";
    }
    if (total > 500000 && !opt.quiet) {
        std::cout << "[提示] 条数较大（" << total << "），内存后端会一直持有这些数据，注意内存占用\n";
    }
    const double periodUsD = periodMs * 1000.0;
    // 自旋余量：见 sleepUntil 的说明。周期很短时按周期的 3/4 自旋（此时定时器粒度已是硬限制）。
    const auto margin = std::chrono::microseconds(
        static_cast<long long>(std::min(16000.0, std::max(500.0, periodUsD * 0.75))));
    if (realtime && periodMs < 16.0 && !opt.quiet) {
        std::cout << "[提示] 节拍周期 " << num2(periodMs)
                  << "ms < 16ms：Windows 默认定时器粒度约 15.6ms，节拍抖动会明显——这是系统限制、"
                     "不是模块问题；要高负载看写入能力请用 --mode throughput\n";
    }

    // ───────────────────────────────────────────────────────── 装配（注入形态）
    // 默认内存后端；--segfile DIR 时换成段文件后端 —— 换的只是这一个注入对象，
    // 写入/查询/导出/保留的代码路径完全不变（这正是"装配点在宿主里"的意思，TLM-ADP-01）。
    MemoryBackend memBackend;
    std::unique_ptr<SegmentFileBackend> segBackend;
    if (!opt.segfile.empty()) segBackend.reset(new SegmentFileBackend(opt.segfile));
    IStorageBackend* be = opt.segfile.empty() ? static_cast<IStorageBackend*>(&memBackend)
                                              : static_cast<IStorageBackend*>(segBackend.get());

    SystemClock            clock;
    UniformRetentionPolicy policy(static_cast<std::int64_t>(opt.ttlSec * 1000.0));  // 0 = 永久保留
    StderrLogSink          log;

    StoreOptions so;
    so.flush.maxRecords    = 1000;   // 生产默认口径：1000 条 / 200 ms（TLM-WRT-02）
    so.flush.maxDelayMs    = 200;
    so.nominalPeriodMs     = static_cast<Millis>(std::llround(periodMs));  // 名义周期：gaps() 默认阈值 = 3×
    so.enableSweeper       = true;   // 保留巡检线程按默认开着（TLM-RET-02）
    Store store(so, *be, clock, policy, &log);  // segBackend 声明在 Store 之前 → 比 Store 活得久

    const Millis wallBase = clock.nowMs();  // 事件时间基准（D3：时间由调用方给）

    LatencySampler lat(total);
    long long      generated    = 0;
    long long      lateTicks    = 0;
    double         maxLateMs    = 0.0;
    double         burstMsSum   = 0.0;  // 真正用于"生成"的时间（不含节拍等待）
    double         worstBurstMs = 0.0;  // 单拍最坏生成耗时：超过节拍周期才是真的"跟不上"

    const auto t0 = std::chrono::steady_clock::now();
    for (long long tick = 0; tick < ticks; ++tick) {
        const auto target =
            t0 + std::chrono::microseconds(static_cast<long long>(static_cast<double>(tick) * periodMs * 1000.0));
        if (realtime && tick > 0) {
            sleepUntil(target, margin);
            const double lateMs = std::chrono::duration<double, std::milli>(
                                      std::chrono::steady_clock::now() - target).count();
            if (lateMs > 1.0) {
                ++lateTicks;
                maxLateMs = std::max(maxLateMs, lateMs);
            }
        }
        const Millis ts = wallBase + static_cast<Millis>(std::llround(static_cast<double>(tick) * periodMs));

        const auto burstStart = std::chrono::steady_clock::now();
        for (long long d = 0; d < opt.devices; ++d) {
            const std::string dev = deviceIdOf(d, devWidth);
            const auto a0 = std::chrono::steady_clock::now();
            store.append(dev, opt.type, ts, makePayload(opt.type, dev, d, tick, opt.seed));
            const double us =
                std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - a0).count();
            lat.add(generated, us);
            ++generated;
        }
        const double burstMs =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - burstStart)
                .count();
        burstMsSum += burstMs;
        worstBurstMs = std::max(worstBurstMs, burstMs);

        if (!opt.quiet && realtime && (tick + 1) % 10 == 0) {
            std::cout << "  … 节拍 " << (tick + 1) << "/" << ticks << " · 已写入 " << generated << " 条\n";
        }
    }
    if (realtime && ticks > 0) {
        // 走满整个窗口：最后一拍之后还要等到 seconds 的窗口末尾，
        // 这样"实测用时"与 --seconds 口径一致，吞吐数才可比。
        sleepUntil(t0 + std::chrono::microseconds(
                            static_cast<long long>(static_cast<double>(ticks) * periodMs * 1000.0)),
                   margin);
    }
    store.flush();  // 收尾刷盘：不刷的话最后一批还在缓冲里（析构也会刷，这里显式一点）

    const double elapsedSec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const double achievedRate = elapsedSec > 0.0 ? static_cast<double>(generated) / elapsedSec : 0.0;

    // ───────────────────────────────────────────────────────── 结果
    const Status st = store.status();
    std::cout << "[写入] appended=" << st.appended << " persisted=" << st.persisted
              << " buffered=" << st.buffered << " flushes=" << st.flushes
              << " droppedByOverflow=" << st.droppedByOverflow
              << " backendWriteFailures=" << st.backendWriteFailures
              << " thresholdState=" << st.thresholdState << "\n";
    if (segBackend) segBackend->flushToDisk();  // 段文件后端：把当前段刷到磁盘（析构也会刷，这里显式）
    std::cout << "[后端] " << store.backendName() << " · approxRows=" << be->approxRows()
              << " · approxBytes=" << be->approxBytes() << " · lastWriteMs="
              << num2(st.lastWriteMs) << " · avgWriteMs=" << num2(st.avgWriteMs) << "\n";
    if (segBackend) {
        std::cout << "[落盘] 目录=" << segBackend->dir() << " · 段数=" << segBackend->segmentCount()
                  << " · 目录内累计条数=" << be->approxRows() << "（本次生成 " << generated
                  << " + 该目录里已有的历史数据）\n"
                  << "       对同一个目录再跑一次，条数会在原基础上累加 —— 这就是跨重启留存"
                     "（TLM-NFR-06 / R3）\n";
    }

    const double genCapacity =
        burstMsSum > 0.0 ? static_cast<double>(generated) / (burstMsSum / 1000.0) : 0.0;
    std::cout << "[吞吐] " << generated << " 条 / " << num2(elapsedSec) << " s = "
              << num2(achievedRate) << " 条/s";
    if (realtime) {
        // 实测吞吐里混着"等节拍"的时间，所以同时给出纯生成能力（写入路径真实上限）
        std::cout << "（窗口口径含节拍等待；纯生成耗时 " << num2(burstMsSum) << " ms → 生成能力 "
                  << num2(genCapacity) << " 条/s）";
    } else {
        std::cout << "（1000 条/s 目标的 " << num2(achievedRate / 1000.0) << " 倍）";
    }
    std::cout << "\n";

    std::cout << "[延迟] append 采样 " << lat.size() << " 条 · P50=" << num2(lat.percentile(0.50))
              << "µs P95=" << num2(lat.percentile(0.95)) << "µs P99="
              << num2(lat.percentile(0.99)) << "µs max=" << num2(lat.maxUs())
              << "µs · 超 1ms 的调用=" << lat.slowCount() << " 次（含触发刷盘的那几次）\n";

    // 节拍口径：只有"单拍生成耗时超过节拍周期"才叫写入跟不上；
    // sleep 唤醒的迟到量属于操作系统定时器粒度，单独报告、不与写入能力混为一谈。
    std::string tickNote;
    if (!realtime) {
        tickNote = "throughput 模式不计节拍（尽快灌满）";
    } else {
        tickNote = "计划 " + num2(requestedRate) + " 条/s · 单拍最坏生成 " + num2(worstBurstMs) +
                   " ms / 周期 " + num2(periodMs) + " ms（超周期才是跟不上）";
        if (lateTicks == 0) {
            tickNote += " · 迟到节拍 0 个";
        } else {
            tickNote += " · 迟到节拍 " + std::to_string(lateTicks) + " 个（最大 " +
                        num2(maxLateMs) + " ms，属系统定时器粒度）";
        }
    }
    std::cout << "[节拍] " << tickNote << "\n";

    // 判定（TLM-NFR-01：1000 条/s 持续写入 + P95 ≤ 1 ms + 丢弃为 0）
    // 注意"跟得上"的口径：只有**单拍生成耗时超过节拍周期**才算写入跟不上；
    // sleep 唤醒的迟到量属于操作系统定时器粒度，不拿来给模块定罪（但也要打印出来）。
    const bool keepUp = realtime ? (worstBurstMs <= periodMs && maxLateMs <= periodMs * 0.5) : true;
    const bool applicable = realtime ? (requestedRate >= 1000.0) : true;
    const bool rateOk     = realtime ? keepUp : (achievedRate >= 1000.0);
    const bool latOk      = lat.percentile(0.95) <= 1000.0;
    const bool lossOk     = (st.droppedByOverflow == 0 && st.backendWriteFailures == 0);
    std::string verdict;
    if (!applicable) {
        verdict = "不适用（本次计划速率 " + num2(requestedRate) +
                  " 条/s < 1000 条/s 的目标线，没在压 NFR-01）";
    } else {
        verdict = (rateOk && latOk && lossOk) ? "PASS" : "未达标";
    }
    std::cout << "[判定] TLM-NFR-01（≥1000 条/s · P95 ≤ 1ms · 零丢弃）：" << verdict
              << "（速率=" << (applicable ? std::string(yn(rateOk)) : std::string("不适用"))
              << " 延迟=" << yn(latOk) << " 不丢=" << yn(lossOk) << "）\n";

    if (opt.ttlSec > 0.0) {  // 注入了 TTL 就顺手巡检一次，把保留路径也走一遍
        const RetentionReport rep = store.sweep();
        std::cout << "[保留] TTL=" << opt.ttlSec << "s · cutoff=" << fmtTs(rep.cutoffTs)
                  << " · 清理 " << rep.purgedRows << " 条 · 后端支持删除=" << yn(rep.supported)
                  << " · 容量裁剪=" << yn(rep.capacityTrimmed) << "\n";
    }

    // ───────────────────────────────────────────────────────── 导出（可选）
    if (!opt.out.empty()) {
        const Millis fromTs = wallBase;
        const Millis toTs   = wallBase + static_cast<Millis>(std::llround(static_cast<double>(ticks) * periodMs)) + 1;
        std::string outPath;
        long long   rowsWritten = 0;
        std::uintmax_t bytesOut = 0;
        std::string  e;
        std::cout << "[导出] 分页查回并写入 " << opt.out << " …\n";
        if (!exportRows(store, opt, fromTs, toTs, total, outPath, rowsWritten, bytesOut, e)) {
            std::cerr << "[ts_gen] 导出失败：" << e << "\n";
            return 1;
        }
        std::cout << "[导出] " << outPath << " · " << rowsWritten << " 行 · " << bytesOut
                  << " 字节 · 格式=" << (opt.csv ? "CSV" : "JSONL（每行一条）") << "\n";
        if (rowsWritten != total) {
            std::cerr << "[ts_gen] 警告：导出 " << rowsWritten << " 条，少于生成的 " << total
                      << " 条（检查是否有去重/截断）\n";
            return 1;
        }
        if (!opt.quiet) {
            // 打一行真实首行回来，方便一眼确认导出格式（结构与 {"deviceId","type","ts","seq","data"} 一致）
            std::ifstream head(fs::path(outPath), std::ios::binary);
            std::string firstLine;
            std::getline(head, firstLine);
            if (firstLine.size() > 260) firstLine = firstLine.substr(0, 260) + " …";
            std::cout << "[导出] 首行：" << firstLine << "\n";
        }
    }

    std::cout << "== 结束：生成 " << generated << " 条 · 用时 " << elapsedSec << " s ==\n";
    return 0;
}

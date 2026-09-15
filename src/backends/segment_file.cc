// src/backends/segment_file.cc · 文件分段后端实现
//
// 设计依据：docs/设计/遥测留存概要设计.md §2.2 · §7 R3 · D15
//
// 三件事值得说明：
//  ① **写**：一串 JSON Lines，够 maxRowsPerSegment 行就滚新段。写缓冲交给 ofstream，
//     不每行 fsync —— 崩溃丢的只是"还没落盘的尾巴"，而模块的序号缺口检测会如实反映它（P3）。
//  ② **读**：段按起始 ts 有序 → 先按区间筛段；段内用稀疏索引二分跳到区间起点，
//     再顺序读到 toTs。候选读进内存后由模块统一去重排序（D7：去重不在后端做）。
//  ③ **清理**：`ts < cutoff` 的**整段直接删文件**；只有跨越边界的那一段需要重写。
#include "telemetry_store/backends/segment_file.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>
#include <tuple>

#include "query.h"

namespace fs = std::filesystem;

namespace telemetry_store {
namespace {

constexpr const char* kSegSuffix = ".seg";
constexpr const char* kIdxPrefix = "#idx ";

/// 文件名：`<起始ts 补零19位>-<序号6位>.seg`。
/// 补零的目的：**字典序 = 时间序**，装载已有段时直接按文件名排序就是时间顺序。
std::string segmentFileName(Millis startTs, std::uint64_t ordinal) {
    std::ostringstream oss;
    oss << std::setw(19) << std::setfill('0') << (startTs < 0 ? 0 : startTs) << '-'
        << std::setw(6) << std::setfill('0') << ordinal << kSegSuffix;
    return oss.str();
}

std::string joinPath(const std::string& dir, const std::string& name) {
    return (fs::path(dir) / name).string();
}

}  // namespace

SegmentFileBackend::SegmentFileBackend(std::string dir, Options opt)
    : dir_(std::move(dir)), opt_(opt) {
    if (opt_.maxRowsPerSegment == 0) opt_.maxRowsPerSegment = 65536;
    if (opt_.indexEvery == 0) opt_.indexEvery = 1024;

    std::error_code ec;
    fs::create_directories(dir_, ec);  // 目录不存在就建（ec 形式：失败也不抛）

    std::lock_guard<std::mutex> lk(mtx_);
    loadExistingLocked();
}

SegmentFileBackend::~SegmentFileBackend() {
    std::lock_guard<std::mutex> lk(mtx_);
    closeWriterLocked();
}

void SegmentFileBackend::closeWriterLocked() {
    if (writer_.is_open()) {
        writer_.flush();
        writer_.close();
    }
}

void SegmentFileBackend::flushToDisk() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (writer_.is_open()) writer_.flush();
}

void SegmentFileBackend::loadExistingLocked() {
    std::error_code ec;
    std::vector<std::string> names;
    for (const auto& entry : fs::directory_iterator(dir_, ec)) {
        if (ec) break;
        if (!entry.is_regular_file(ec)) continue;
        const std::string name = entry.path().filename().string();
        if (name.size() > 4 && name.compare(name.size() - 4, 4, kSegSuffix) == 0) {
            names.push_back(name);
        }
    }
    std::sort(names.begin(), names.end());  // 字典序 = 时间序（文件名补零）

    for (const auto& name : names) {
        SegmentInfo s;
        s.path = joinPath(dir_, name);
        // 起始 ts 从文件名解析；解析不出就退回扫描文件内容求最早 ts。
        const std::size_t dash = name.find('-');
        Millis startTs = -1;
        if (dash != std::string::npos && dash > 0) {
            try {
                startTs = std::stoll(name.substr(0, dash));
            } catch (...) {
                startTs = -1;
            }
        }

        std::uint64_t rows = 0;
        Millis first = -1, last = 0;
        std::ifstream in(s.path, std::ios::binary);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty() || line.compare(0, 5, kIdxPrefix) == 0) continue;
            try {
                Record r = Record::fromJson(nlohmann::json::parse(line));
                if (first < 0 || r.ts < first) first = r.ts;
                if (r.ts > last) last = r.ts;
                ++rows;
            } catch (...) {
                // 坏行（例如上次崩溃写了一半）：跳过并继续 —— 单行坏数据不许拖垮整段。
            }
        }
        s.rows    = rows;
        s.startTs = (startTs >= 0) ? startTs : first;
        s.endTs   = (last != 0) ? last : s.startTs;
        std::error_code ec2;
        s.bytes = static_cast<std::uint64_t>(fs::file_size(s.path, ec2));
        if (ec2) s.bytes = 0;
        if (s.rows > 0) {
            segs_.push_back(std::move(s));
        } else {
            // 空段（比如上次刚建就崩了）：直接清掉，免得留在列表里干扰区间判定。
            std::error_code ec3;
            fs::remove(s.path, ec3);
        }
    }
}

std::uint64_t SegmentFileBackend::countRowsIn(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::uint64_t rows = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line.compare(0, 5, kIdxPrefix) == 0) continue;
        ++rows;
    }
    return rows;
}

bool SegmentFileBackend::openNewSegmentLocked(Millis firstTs) {
    closeWriterLocked();

    // 序号 = 已有段数（保证同一毫秒内也能区分）。
    const std::string name = segmentFileName(firstTs, segs_.size());
    curPath_  = joinPath(dir_, name);
    curStartTs_ = firstTs;
    curEndTs_   = firstTs;
    curRows_    = 0;
    curBytes_   = 0;

    writer_.open(curPath_, std::ios::binary | std::ios::trunc);
    if (!writer_.is_open()) return false;

    SegmentInfo s;
    s.path    = curPath_;
    s.startTs = firstTs;
    s.endTs   = firstTs;
    s.rows    = 0;
    s.bytes   = 0;
    segs_.push_back(std::move(s));
    return true;
}

bool SegmentFileBackend::append(RecordBatch batch) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (batch.empty()) return true;

    for (const auto& r : batch) {
        if (!writer_.is_open()) {
            if (!openNewSegmentLocked(r.ts)) return false;
        } else if (curRows_ >= opt_.maxRowsPerSegment) {
            if (!openNewSegmentLocked(r.ts)) return false;
        }

        const std::string line = r.toJson().dump();
        const std::streamoff off = writer_.tellp();
        writer_ << line << '\n';
        if (!writer_) {
            // 写失败（磁盘满 / 权限）：本批算失败，调用方会把数据留在缓冲里重试。
            writer_.clear();
            return false;
        }

        // 稀疏索引：每 indexEvery 行插一条，供后续二分跳转。
        if (opt_.indexEvery > 0 && (curRows_ % opt_.indexEvery) == 0) {
            nlohmann::json idx{{"ts", r.ts}, {"off", static_cast<std::int64_t>(off)},
                               {"row", curRows_}};
            writer_ << kIdxPrefix << idx.dump() << '\n';
        }

        ++curRows_;
        curBytes_ += line.size() + 1;
        if (curStartTs_ < 0) curStartTs_ = r.ts;
        if (r.ts > curEndTs_) curEndTs_ = r.ts;
        if (r.ts < curStartTs_) curStartTs_ = r.ts;

        SegmentInfo& s = segs_.back();
        s.startTs = curStartTs_;
        s.endTs   = curEndTs_;
        s.rows    = curRows_;
        s.bytes   = curBytes_;
    }
    return true;
}

SegmentFileBackend::ReadHit SegmentFileBackend::locateLocked(const std::string& path,
                                                             uint64_t wantTs) {
    // 做法：把读位置推到"第一个 ts >= wantTs"附近。先收集索引，二分，再从头回放。
    std::vector<IndexEntry> idx;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return ReadHit{};
    std::string line;
    while (std::getline(in, line)) {
        if (line.compare(0, 5, kIdxPrefix) != 0) continue;
        try {
            nlohmann::json j = nlohmann::json::parse(line.substr(5));
            IndexEntry e;
            e.ts  = static_cast<Millis>(j.value("ts", 0LL));
            e.off = static_cast<std::streamoff>(j.value("off", 0LL));
            e.row = static_cast<std::uint64_t>(j.value("row", 0ULL));
            idx.push_back(e);
        } catch (...) {
            // 坏索引行：忽略（索引只是加速，不是正确性依赖）
        }
    }
    if (idx.empty()) return ReadHit{};  // 没有索引 → 从头读

    auto it = std::upper_bound(idx.begin(), idx.end(), static_cast<Millis>(wantTs),
                               [](Millis ts, const IndexEntry& e) { return ts < e.ts; });
    const IndexEntry* pick = nullptr;
    if (it == idx.begin()) {
        pick = &idx.front();
    } else {
        pick = &(*std::prev(it));
    }
    ReadHit hit;
    hit.found = true;
    hit.ts    = pick->ts;
    hit.off   = pick->off;
    hit.row   = pick->row;
    return hit;
}

bool SegmentFileBackend::readSegmentLocked(const std::string& path, std::vector<Record>& out) {
    // 缓存命中。⚠️ 这里必须**拷贝**而不是 move：
    // 曾经用 move 把缓存项掏空，结果"第一次查询正常、第二次同一段返回空"——
    // 一个只在第二次查询时才发作的 bug（已由 tests/manual_segfile_check.cc 抓出）。
    for (auto it = cache_.begin(); it != cache_.end(); ++it) {
        if (it->path == path) {
            out = it->rows;
            if (it != std::prev(cache_.end())) {  // 命中即提到队尾（LRU）
                CacheEntry hit = std::move(*it);
                cache_.erase(it);
                cache_.push_back(std::move(hit));
            }
            return true;
        }
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;

    std::vector<Record> rows;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line.compare(0, 5, kIdxPrefix) == 0) continue;
        try {
            rows.push_back(Record::fromJson(nlohmann::json::parse(line)));
        } catch (...) {
            // 坏行跳过（单行坏数据不许拖垮整段）
        }
    }
    out = rows;  // 返回给调用方

    CacheEntry e;
    e.path = path;
    e.rows = std::move(rows);
    e.bytes = 0;
    for (const auto& r : e.rows) e.bytes += r.data.dump().size() + 64;
    cacheBytes_ += e.bytes;
    cache_.push_back(std::move(e));
    trimCacheLocked();
    return true;
}

void SegmentFileBackend::trimCacheLocked() {
    const std::size_t limit = opt_.readCacheRows > 0 ? opt_.readCacheRows * 128 : 0;
    while (cache_.size() > 1 && limit > 0 && cacheBytes_ > limit) {
        cacheBytes_ -= cache_.front().bytes;
        cache_.pop_front();
    }
}

std::size_t SegmentFileBackend::read(const Query& q, const Cursor* beginAfter,
                                     const RecordVisitor& visit) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (writer_.is_open()) writer_.flush();  // 让"刚 append 的"能被读到

    std::size_t matched = 0;

    // 1) 按区间筛段：段 [startTs, endTs] 与查询 [fromTs, toTs) 无交集就整段跳过。
    for (const auto& seg : segs_) {
        if (seg.rows == 0) continue;
        if (seg.endTs < q.fromTs) continue;   // 整段太旧
        if (seg.startTs >= q.toTs) continue;  // 整段太新（段已按时间有序，但保持通用写法）

        // 2) 段内定位到区间起点附近（稀疏索引二分；没有索引就从头读）。
        ReadHit hit = locateLocked(seg.path, static_cast<uint64_t>(q.fromTs));

        std::vector<Record> rows;
        if (!readSegmentLocked(seg.path, rows)) continue;

        std::size_t startRow = 0;
        if (hit.found) {
            // 索引位置是"第 row 行的 ts = hit.ts"，它可能早于 fromTs（索引粒度粗），
            // 所以从它开始顺序找第一个 ts >= fromTs。
            startRow = static_cast<std::size_t>(hit.row);
            if (startRow >= rows.size()) continue;
            while (startRow < rows.size() && rows[startRow].ts < q.fromTs) ++startRow;
        } else {
            while (startRow < rows.size() && rows[startRow].ts < q.fromTs) ++startRow;
        }

        for (std::size_t i = startRow; i < rows.size(); ++i) {
            const Record& r = rows[i];
            if (r.ts >= q.toTs) break;  // 段内已按写入序近似有序；跨过 toTs 即可停
            if (!q.acceptsDevice(r.deviceId) || !q.acceptsType(r.type)) continue;
            if (beginAfter != nullptr && !detail::cursorPasses(r, *beginAfter)) continue;
            ++matched;
            if (!visit(r)) return matched;  // 调用方收够了
        }
    }
    return matched;
}

bool SegmentFileBackend::rewriteSegmentLocked(SegmentInfo& seg, Millis cutoffTs) {
    // 只保留 ts >= cutoff 的记录，写到临时文件后原子替换。
    std::vector<Record> keep;
    std::vector<Record> rows;
    if (!readSegmentLocked(seg.path, rows)) return false;
    for (const auto& r : rows) {
        if (r.ts >= cutoffTs) keep.push_back(r);
    }

    const std::string tmp = seg.path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) return false;
        std::uint64_t n = 0;
        for (const auto& r : keep) {
            const std::string line = r.toJson().dump();
            const std::streamoff off = out.tellp();
            out << line << '\n';
            if (opt_.indexEvery > 0 && (n % opt_.indexEvery) == 0) {
                nlohmann::json idx{{"ts", r.ts}, {"off", static_cast<std::int64_t>(off)},
                                   {"row", n}};
                out << kIdxPrefix << idx.dump() << '\n';
            }
            ++n;
        }
        out.flush();
        if (!out) return false;
    }

    std::error_code ec;
    fs::rename(tmp, seg.path, ec);
    if (ec) {
        fs::remove(tmp, ec);
        return false;
    }

    // 缓存里的旧内容作废。
    for (auto it = cache_.begin(); it != cache_.end(); ++it) {
        if (it->path == seg.path) {
            cacheBytes_ -= it->bytes;
            cache_.erase(it);
            break;
        }
    }

    seg.rows = keep.size();
    if (!keep.empty()) {
        seg.startTs = keep.front().ts;
        seg.endTs   = keep.back().ts;
        for (const auto& r : keep) {
            if (r.ts < seg.startTs) seg.startTs = r.ts;
            if (r.ts > seg.endTs) seg.endTs = r.ts;
        }
    }
    std::error_code ec2;
    seg.bytes = static_cast<std::uint64_t>(fs::file_size(seg.path, ec2));
    if (ec2) seg.bytes = 0;
    return true;
}

bool SegmentFileBackend::purge(Millis cutoffTs) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (cutoffTs <= 0) return true;

    // 当前写入段如果整个都在 cutoff 之前，先把它关掉——不能边写边删。
    if (writer_.is_open() && curEndTs_ < cutoffTs) {
        closeWriterLocked();
        curPath_.clear();
    }
    if (writer_.is_open()) writer_.flush();

    std::vector<SegmentInfo> kept;
    kept.reserve(segs_.size());

    for (auto& seg : segs_) {
        if (seg.path == curPath_) {  // 正在写的段不参与清理
            kept.push_back(seg);
            continue;
        }
        if (seg.endTs < cutoffTs) {
            // 整段过期 → 整段删除（这正是"分段"最划算的地方）。
            std::error_code ec;
            fs::remove(seg.path, ec);
            for (auto it = cache_.begin(); it != cache_.end(); ++it) {
                if (it->path == seg.path) {
                    cacheBytes_ -= it->bytes;
                    cache_.erase(it);
                    break;
                }
            }
            continue;  // 不放进 kept = 从元数据里消失
        }
        if (seg.startTs < cutoffTs) {
            // 跨越边界 → 只重写这一段。
            if (!rewriteSegmentLocked(seg, cutoffTs)) return false;
            if (seg.rows == 0) continue;
        }
        kept.push_back(seg);
    }
    segs_.swap(kept);
    return true;
}

Millis SegmentFileBackend::earliestRetainedTs() const {
    std::lock_guard<std::mutex> lk(mtx_);
    Millis best = 0;
    for (const auto& s : segs_) {
        if (s.rows == 0) continue;
        if (best == 0 || s.startTs < best) best = s.startTs;
    }
    return best;
}

std::size_t SegmentFileBackend::approxRows() const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t n = 0;
    for (const auto& s : segs_) n += static_cast<std::size_t>(s.rows);
    return n;
}

std::size_t SegmentFileBackend::approxBytes() const {
    std::lock_guard<std::mutex> lk(mtx_);
    std::size_t n = 0;
    for (const auto& s : segs_) n += static_cast<std::size_t>(s.bytes);
    return n;
}

std::size_t SegmentFileBackend::segmentCount() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return segs_.size();
}

}  // namespace telemetry_store

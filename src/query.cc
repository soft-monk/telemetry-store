// src/query.cc · 排序 / 去重 / 过滤 / 游标比较
#include "query.h"

#include <algorithm>
#include <tuple>

namespace telemetry_store {
namespace detail {

bool cursorPasses(const Record& r, const Cursor& c) {
    if (!c.valid) return true;  // 无游标 = 从头开始
    return std::tie(r.ts, r.deviceId, r.type) > std::tie(c.ts, c.deviceId, c.type);
}

bool inRange(const Record& r, Millis fromTs, Millis toTs) {
    return r.ts >= fromTs && r.ts < toTs;  // 左闭右开（D5）
}

bool matches(const Record& r, const Query& q) {
    if (!inRange(r, q.fromTs, q.toTs)) return false;
    if (!q.acceptsDevice(r.deviceId)) return false;
    if (!q.acceptsType(r.type)) return false;
    return true;
}

void sortByKey(std::vector<Record>& rows) {
    std::sort(rows.begin(), rows.end(), [](const Record& a, const Record& b) {
        return std::tie(a.ts, a.deviceId, a.type) < std::tie(b.ts, b.deviceId, b.type);
    });
}

void dedupLastWins(std::vector<Record>& rows) {
    if (rows.size() < 2) return;
    // 前置条件：已按主键排序（重复项相邻）。保留同键中 seq 最大者。
    std::size_t out = 0;
    std::size_t i = 0;
    while (i < rows.size()) {
        std::size_t j = i + 1;
        std::size_t best = i;
        while (j < rows.size() && rows[j].sameKeyAs(rows[i])) {
            if (rows[j].seq > rows[best].seq) best = j;
            ++j;
        }
        if (out != best) rows[out] = std::move(rows[best]);
        ++out;
        i = j;
    }
    rows.resize(out);
}

}  // namespace detail
}  // namespace telemetry_store

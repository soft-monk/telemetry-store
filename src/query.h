// src/query.h · 查询路径的公共零件（模块内部）
//
// 设计依据：docs/设计/遥测留存概要设计.md §4.2（去重）· §4.3（游标）· §4.6（边界条件）
// 需求：TLM-WRT-08 · TLM-QRY-01..05
//
// query / since / replay / gaps 共用同一条取数路径（§1 事实：一套语义收成一条路径）。
#pragma once

#include <cstddef>
#include <vector>

#include "telemetry_store/record.h"

namespace telemetry_store {
namespace detail {

/// 游标全序比较：`(ts, deviceId, type) > (c.ts, c.deviceId, c.type)`。
/// 非法的游标视为"从头开始"（返回 true）。
bool cursorPasses(const Record& r, const Cursor& c);

/// 记录是否落在 `[fromTs, toTs)` 内。
bool inRange(const Record& r, Millis fromTs, Millis toTs);

/// 是否命中 Query 的设备/类型/区间三重过滤。
bool matches(const Record& r, const Query& q);

/// 按逻辑主键排序（ts → deviceId → type），保证查询结果全序稳定。
void sortByKey(std::vector<Record>& rows);

/// 去重：同一 `(deviceId, ts, type)` 只留 **seq 最大**（后写胜，TLM-WRT-08）。
/// 排序后重复项相邻，一趟线性完成。
void dedupLastWins(std::vector<Record>& rows);

}  // namespace detail
}  // namespace telemetry_store

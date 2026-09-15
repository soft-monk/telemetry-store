// src/util.h · 模块内部小工具（不属公开面）
//
// 放在 src/ 根部、以全路径 `#include "util.h"` 引用（沿用 device-ingest 的经验：
// src 内部目录不进 include 路径，避免与 CRT 头文件同名打架）。
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "telemetry_store/record.h"

namespace telemetry_store {
namespace detail {

/// 求 `now - ttl`，饱和到 0（不会因 ttl 巨大或 now 很小而环绕）。
inline Millis cutoffFor(Millis now, std::int64_t ttlMs) {
    if (ttlMs <= 0) return 0;
    if (now <= ttlMs) return 0;
    return now - ttlMs;
}

/// 字符串是否在过滤集里；集合为空 = 不过滤（与 Query 的口径一致）。
inline bool inFilter(const std::vector<std::string>& set, const std::string& v) {
    if (set.empty()) return true;
    for (const auto& s : set) {
        if (s == v) return true;
    }
    return false;
}

/// 取 JSON 里的字符串字段；不是字符串就返回空。
inline std::string strField(const nlohmann::json& j, const char* key) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return std::string();
    return it->get<std::string>();
}

/// 取 JSON 里的整数字段；不存在或类型不符就返回 fallback。
inline Millis intField(const nlohmann::json& j, const char* key, Millis fallback = 0) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_number()) return fallback;
    return static_cast<Millis>(it->get<double>());
}

}  // namespace detail
}  // namespace telemetry_store

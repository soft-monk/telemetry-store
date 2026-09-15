// src/record.cc · 数据结构：比较、序列化
//
// 设计依据：docs/设计/遥测留存概要设计.md §2.1 / §3.3
// （`Store::fromEnvelope` 的实现在 store.cc —— 它属门面，不属于数据结构）
#include "telemetry_store/record.h"

#include <algorithm>
#include <tuple>

#include "util.h"

namespace telemetry_store {

// ---------------------------------------------------------------- Record
bool Record::lessByKey(const Record& a, const Record& b) {
    // 全序：先 ts，再 deviceId，再 type —— 与 D8 的游标口径严格一致。
    return std::tie(a.ts, a.deviceId, a.type) < std::tie(b.ts, b.deviceId, b.type);
}

bool Record::sameKeyAs(const Record& other) const {
    return ts == other.ts && deviceId == other.deviceId && type == other.type;
}

nlohmann::json Record::toJson() const {
    return nlohmann::json{
        {"deviceId", deviceId},
        {"type", type},
        {"ts", ts},
        {"seq", seq},
        {"data", data.is_null() ? nlohmann::json::object() : data},
    };
}

Record Record::fromJson(const nlohmann::json& j) {
    Record r;
    r.deviceId = detail::strField(j, "deviceId");
    r.type     = detail::strField(j, "type");
    r.ts       = detail::intField(j, "ts");
    r.seq      = static_cast<std::uint64_t>(detail::intField(j, "seq"));
    auto it = j.find("data");
    if (it != j.end() && it->is_object()) {
        r.data = *it;
    } else {
        r.data = nlohmann::json::object();
    }
    return r;
}

// ---------------------------------------------------------------- Cursor
Cursor Cursor::after(const Record& r) {
    Cursor c;
    c.ts       = r.ts;
    c.deviceId = r.deviceId;
    c.type     = r.type;
    c.valid    = true;
    return c;
}

nlohmann::json Cursor::toJson() const {
    return nlohmann::json{
        {"ts", ts}, {"deviceId", deviceId}, {"type", type}, {"valid", valid}};
}

Cursor Cursor::fromJson(const nlohmann::json& j) {
    Cursor c;
    c.ts       = detail::intField(j, "ts");
    c.deviceId = detail::strField(j, "deviceId");
    c.type     = detail::strField(j, "type");
    auto it = j.find("valid");
    c.valid = (it != j.end() && it->is_boolean()) ? it->get<bool>() : false;
    return c;
}

// ---------------------------------------------------------------- Query
bool Query::acceptsDevice(const std::string& id) const {
    return detail::inFilter(devices, id);
}

bool Query::acceptsType(const std::string& t) const {
    return detail::inFilter(types, t);
}

// ---------------------------------------------------------------- QueryResult
nlohmann::json QueryResult::toJson() const {
    nlohmann::json rowsJson = nlohmann::json::array();
    for (const auto& r : rows) rowsJson.push_back(r.toJson());
    return nlohmann::json{
        {"rows", rowsJson},
        {"count", rows.size()},
        {"hasMore", hasMore},
        {"cursor", cursor.toJson()},
        {"coveredFromTs", coveredFromTs},
        {"coveredToTs", coveredToTs},
        {"truncatedByRetention", truncatedByRetention},
        {"partial", partial},
        {"rejected", rejected},
        {"rejectReason", rejectReason},
        {"scanned", scanned},
    };
}

// ---------------------------------------------------------------- Timeline
nlohmann::json Timeline::toJson() const {
    nlohmann::json samplesJson = nlohmann::json::array();
    for (const auto& r : samples) samplesJson.push_back(r.toJson());
    return nlohmann::json{
        {"fromTs", fromTs},
        {"toTs", toTs},
        {"durationMs", durationMs},
        {"samples", samplesJson},
        {"count", samples.size()},
        {"sampled", sampled},
        {"sampleStep", sampleStep},
        {"hasMore", hasMore},
        {"cursor", cursor.toJson()},
        {"truncatedByRetention", truncatedByRetention},
        {"partial", partial},
    };
}

// ---------------------------------------------------------------- RetentionReport
nlohmann::json RetentionReport::toJson() const {
    return nlohmann::json{
        {"supported", supported},
        {"purgedRows", purgedRows},
        {"capacityEvictedRows", capacityEvictedRows},
        {"cutoffTs", cutoffTs},
        {"nowTs", nowTs},
        {"capacityTrimmed", capacityTrimmed},
        {"backend", backend},
    };
}

// ---------------------------------------------------------------- Status
nlohmann::json Status::toJson() const {
    return nlohmann::json{
        {"appended", appended},
        {"persisted", persisted},
        {"buffered", buffered},
        {"droppedByOverflow", droppedByOverflow},
        {"backendWriteFailures", backendWriteFailures},
        {"rejectedByLimit", rejectedByLimit},
        {"purgeFailures", purgeFailures},
        {"purgedRows", purgedRows},
        {"earliestPurgedTs", earliestPurgedTs},
        {"lastPurgeTs", lastPurgeTs},
        {"lastWriteMs", lastWriteMs},
        {"avgWriteMs", avgWriteMs},
        {"seqFirst", seqFirst},
        {"seqLast", seqLast},
        {"flushes", flushes},
        {"thresholdState", thresholdState},
        {"purgeUnsupported", purgeUnsupported},
        {"capacityUnsupported", capacityUnsupported},
        {"backend", backend},
        {"version", version},
    };
}

}  // namespace telemetry_store

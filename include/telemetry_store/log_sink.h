// telemetry-store · log_sink.h  日志出口（反向接口，可空）
//
// 设计依据：docs/设计/遥测留存概要设计.md §3.5 · 需求 TLM-OBS-03
//
// **模块自身不写文件、不落库、不打控制台**（P2：不静默，但也不越权）。
// 要日志就注入一个实现；不注入就什么都不产生。
#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace telemetry_store {

class ILogSink {
public:
    virtual ~ILogSink() = default;

    /// 结构化日志。category 取以下之一：
    ///   wrt  写入（刷盘、缓冲溢出、后端拒收）
    ///   qry  查询（超限、非法区间）
    ///   rpl  回放（稀疏化）
    ///   gap  缺口识别
    ///   ret  保留清理（清理结果、能力不支持、失败）
    ///   obs  状态与内部错误
    virtual void log(const std::string& category, const nlohmann::json& payload) = 0;
};

/// 空实现：默认。模块内所有日志点都走指针调用，不注入时零开销。
class NullLogSink : public ILogSink {
public:
    void log(const std::string& /*category*/, const nlohmann::json& /*payload*/) override {}
};

}  // namespace telemetry_store

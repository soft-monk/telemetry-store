// telemetry-store · clock.h  时间来源（反向接口）
//
// 设计依据：docs/设计/遥测留存概要设计.md §3.5 · 需求 TLM-ADP-02 · TLM-WRT-04
//
// ⚠️ 一条容易搞错的分界：IClock **不是事件时间的来源**。
//     记录里的 ts 由调用方传入（事件产生时刻，见 record.h），模块**不自己打时间**。
//     IClock 只回答"现在是几点"，用于保留清理、状态统计——这样假时钟就能让
//     "清理"这类与当下有关的行为变成可确定性测试的。
#pragma once

#include <chrono>
#include <cstdint>

namespace telemetry_store {

class IClock {
public:
    virtual ~IClock() = default;

    /// 墙上时钟毫秒（epoch），用于保留期计算与状态统计。
    virtual std::int64_t nowMs() const = 0;
};

/// 系统时钟（默认实现）。
class SystemClock : public IClock {
public:
    std::int64_t nowMs() const override {
        return static_cast<std::int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }
};

/// 可手动拨动的假时钟（测试 / 回放用）。
class FakeClock : public IClock {
public:
    explicit FakeClock(std::int64_t startMs = 0) : now_(startMs) {}

    std::int64_t nowMs() const override { return now_; }
    void set(std::int64_t ms) { now_ = ms; }
    void advance(std::int64_t deltaMs) { now_ += deltaMs; }

private:
    std::int64_t now_;
};

/// 单调时钟毫秒：只用于**间隔与超时**（刷盘节拍、巡检周期），不受系统时间回拨影响。
inline std::int64_t steadyMs() {
    return static_cast<std::int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace telemetry_store

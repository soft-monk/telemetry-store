// telemetry-store · policy.h  保留策略（反向接口：留多久由宿主说了算）
//
// 设计依据：docs/设计/遥测留存概要设计.md §3.5 · 需求 TLM-RET-01/05 · TLM-ADP-03
//
// 模块**不写死任何保留口径**：留多久、留多少，全部由注入的实现回答。
// 模块只负责按它的回答去裁决"该清到哪个时刻"，执行清理的是后端。
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace telemetry_store {

/// 保留策略。实现应无副作用、可被维护线程与任意线程并发调用。
class IRetentionPolicy {
public:
    virtual ~IRetentionPolicy() = default;

    /// 某个事件类型保留多久（毫秒）。返回 <= 0 表示**该类型不清理**（永久保留）。
    /// 模块会取所有类型 ttl 的**最小值**作为全局清理截止点，
    /// 并在后端支持按类型清理时逐类型裁剪（TLM-RET-01）。
    virtual std::int64_t ttlMs(const std::string& type) const = 0;

    /// 记录条数上限，0 = 不限（TLM-RET-05）。超出时按**最旧优先**淘汰。
    virtual std::size_t maxRows() const = 0;

    /// 字节数上限，0 = 不限。后端不支持字节统计时可按 approxBytes() 估算。
    virtual std::size_t maxBytes() const = 0;
};

/// 默认策略：全体类型同一个 TTL，不设容量上限。
/// 用途：示例与测试；生产建议实现自己的策略（按类型分别设期）。
class UniformRetentionPolicy : public IRetentionPolicy {
public:
    /// ttlMs <= 0 表示不清理。
    explicit UniformRetentionPolicy(std::int64_t ttlMs = 7LL * 24 * 3600 * 1000,
                                    std::size_t maxRows = 0,
                                    std::size_t maxBytes = 0)
        : ttlMs_(ttlMs), maxRows_(maxRows), maxBytes_(maxBytes) {}

    std::int64_t ttlMs(const std::string& /*type*/) const override { return ttlMs_; }
    std::size_t  maxRows() const override { return maxRows_; }
    std::size_t  maxBytes() const override { return maxBytes_; }

    void setTtlMs(std::int64_t v) { ttlMs_ = v; }
    void setMaxRows(std::size_t v) { maxRows_ = v; }

private:
    std::int64_t ttlMs_;
    std::size_t  maxRows_;
    std::size_t  maxBytes_;
};

/// 永久保留策略：什么都不清。用于"先把数据留住，策略以后再说"的场合。
class KeepForeverPolicy : public IRetentionPolicy {
public:
    std::int64_t ttlMs(const std::string&) const override { return 0; }
    std::size_t  maxRows() const override { return 0; }
    std::size_t  maxBytes() const override { return 0; }
};

}  // namespace telemetry_store

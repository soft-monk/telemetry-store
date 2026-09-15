# telemetry-store · 实时数据留存层

> **实时数据存下来，让"刚才发生了什么"可以回答。**
>
> 它只回答一个问题：**"过去那段时间，这条流里到底发生了什么？"**
> —— 至于这条流是什么业务、谁需要看、画成什么样子，都不是它的事。

[![status](https://img.shields.io/badge/status-implemented%20%C2%B7%20R1--R6%20done-brightgreen)](#-当前状态)
[![layer](https://img.shields.io/badge/layer-storage%20%C2%B7%20query%20%C2%B7%20replay-teal)](#-它解决什么问题)
[![runtime](https://img.shields.io/badge/runtime-server--side-blue)](#-当前状态)
[![cpp](https://img.shields.io/badge/C%2B%2B-17-00599C)](#-接入方式)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](./LICENSE)

---

## 📌 当前状态（请先读这一段）

**本仓已实现可交付**：`cmake` 一条命令构建，产出静态库 + 两个示例 + 造数/压测工具 + 零依赖自测。
**不需要任何数据库、中间件或网络**（内置 `nlohmann/json` 兜底，装了系统包则优先用系统的）。

| 阶段 | 内容 | 状态 |
|---|---|---|
| 模块边界 | 单独一个仓；只认"带时间戳的流式数据"，不认识任何业务表 | ✅ |
| 职责划分 | 写入 / 查询 / 回放 / 补发 / 保留清理；**补发是它的活**（`realtime-hub` 明确不管） | ✅ |
| 接口契约 | 对外接口 + 反向接口（`IStorageBackend` / `IRetentionPolicy` / `IClock` / `ILogSink`） | ✅ 已冻结 |
| 实现选型 | 存储后端：内存（L1）+ 段文件（L2）；写入：有界缓冲 + 批量刷盘 | ✅ |
| **R1** 接口冻结 | 公开面 9 个头文件（模块本体 6 + 两个内置后端 + 一个示例用控制台小工具），各自可单独包含 | ✅ |
| **R2** 写入 | 非阻塞 `append`、批量刷盘、有界缓冲、失败重试 | ✅ |
| **R3** 查询 / 回放 | `[from,to)` 区间、游标分页、读路径去重、时间轴输出 | ✅ |
| **R4** 补发 | `since`（严格大于 + 每设备水位 + 分页）、`gaps` 缺口识别 | ✅ |
| **R5** 保留 | TTL 清理 + 容量上限 + 后端能力诚实降级 | ✅ |
| **R6** 独立交付 | 示例 + 工具 + 自测（**40 用例 / 449 断言，全绿**）+ 跨平台构建脚本 | ✅ |

> **实测口径**（本机 Windows / MSVC 19.34 / Release，`build.ps1` 全流程约 29 s）：
> `selftest` → `用例 40 · 断言 449 · 失败 0`；`ctest` → `1/1 Passed`；
> `ts_gen --devices 1000 --hz 1 --seconds 5` → `999.36 条/s`、`append P95 = 5.0 µs`、
> `droppedByOverflow = 0`、判定 **TLM-NFR-01 PASS**。

**需求与设计的权威文档**（本仓 `docs/`）：

| 想知道 | 读 |
|---|---|
| 它必须满足什么（41 条需求，`TLM-*` 编号 + 验收标准） | [`docs/需求/遥测留存需求专篇.md`](docs/需求/遥测留存需求专篇.md) |
| 它长什么样（接口冻结稿 / 关键机制 / 决策记录 D1–D15） | [`docs/设计/遥测留存概要设计.md`](docs/设计/遥测留存概要设计.md) |

---

## 🎯 它解决什么问题

> 实时数据存下来，让"刚才发生了什么"可以回答。

**没有它，实时系统就只是"看着动"：刷新一下全没了，出问题查不了，报告只能编。**

实时展示和留存是**两条路**，不是一条：

```
   实时事件流
        │
        ├──▶ 实时展示（不需要存储）
        │
        └──▶ ┌──────────────────┐
             │ telemetry-store  │
             │  写入 → 存储      │
             │  查询 → 区间取数  │
             │  回放 → 时间轴    │
             │  保留 → 过期清理  │
             └────────┬─────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
     轨迹回放      指标曲线      报告/追溯
```

**注意箭头是单向的**：留存层不回头依赖实时展示，也不认识那三个下游是谁。

### 它填的是链路里的第 ③ 节

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   ①  入口     │   │   ②  管道     │   │   ③  留存     │   │   ④  出口     │
│  把设备数据   │──▶│  把事件推给   │──▶│  把数据存下来 │──▶│  把数据画出来 │
│  变标准       │   │  所有客户端   │   │  可查可回放   │   │              │
└──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
  device-ingest      realtime-hub      telemetry-store       map-2d
                     + ws-client        ← 本仓
```

### 为什么"补发"这件事落在它头上

| 问题 | 谁来回答 |
|---|---|
| "现在有数据了，怎么同时送到每个人手上？" | `realtime-hub` |
| "**刚才我不在，那段时间丢了什么？**" | **`telemetry-store`** |
| "我能按人筛选推送吗？" | 都不管——在 hub 外面包一层 |

`realtime-hub` 是**哑的**：断开的人就错过了，**它不补发**。客户端重连后要拿回断线期间的数据，
只能**自己去查留存层**（`since(ts)`）。

> **这是两个模块，别混在一起。** hub 管"在场的人"，留存层管"发生过的事"。

---

## 🚀 接入方式

### 构建（一条命令）

```powershell
# Windows：自动激活 MSVC 环境，然后配置 + 编译 + 跑自测
.\build.ps1
```

> 若提示 `running scripts is disabled on this system`：那是本机 PowerShell 的**执行策略**
> （默认 `Restricted`），与本模块无关。两种解法任选：
> ```powershell
> powershell -ExecutionPolicy Bypass -File .\build.ps1   # 只放宽这一次
> Set-ExecutionPolicy -Scope Process RemoteSigned        # 只放宽当前窗口
> ```

```bash
# Linux / macOS / MSYS2 / WSL
./build.sh
```

手动方式（任何平台）：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

**依赖**：C++17 编译器 + CMake ≥ 3.20 + 线程库。`nlohmann/json` 优先用系统包，
没有则回落内置副本 `third_party/nlohmann/json.hpp`（3.11.3）——**因此"空环境 clone 即构建"成立**。

### 三种引入方式

| 方式 | 做法 |
|---|---|
| FetchContent | `FetchContent_Declare(telemetry_store GIT_REPOSITORY ...)` + `add_subdirectory` |
| git submodule | 同上（子模块目录作为 `SOURCE_DIR`） |
| 本地目录 | `add_subdirectory(path/to/telemetry-store)` |

作为子目录引入时目标名：`telemetry_store::telemetry_store`。

### 最小用法（20 行）

```cpp
#include "telemetry_store/store.h"
using namespace telemetry_store;

Store store;                                  // 默认 = 内存后端 + 系统时钟 + 永不清除
store.append("uav-1", "uav.pos", 1700000000000LL,
             {{"lng", 116.4}, {"lat", 39.9}});   // ts 由调用方给（事件产生时刻）
store.flush();                                     // 返回即已交付后端

Query q;
q.devices = {"uav-1"};
q.fromTs  = 1700000000000LL;
q.toTs    = 1700000001000LL;                       // 区间 [from, to) 左闭右开
for (const auto& r : store.query(q).rows) { /* ... */ }
```

跑一下现成的：

```powershell
.\build\bin\Release\example_minimal.exe     # 最小链路：写入 → 查询 → 状态
.\build\bin\Release\example_replay.exe      # 全流程：查询 → 回放 → 补发 → 缺口 → 状态
.\build\bin\Release\ts_gen.exe --devices 1000 --hz 1 --seconds 5   # 造数 / 压测
.\build\bin\Release\selftest.exe            # 自测（--list 看用例名）
```

### 换成持久化后端（跨重启留存）

```cpp
#include "telemetry_store/backends/segment_file.h"

SegmentFileBackend backend("./telemetry");   // 段文件：滚段 + 段内稀疏索引，零外部依赖
SystemClock        clock;
UniformRetentionPolicy policy(7LL * 24 * 3600 * 1000);   // 留 7 天

Store store(StoreOptions{}, backend, clock, policy, &myLogSink);
```

**换后端只改这一处装配代码，模块源码零改动**——`Store` 只认识 `IStorageBackend` 接口。

---

## ✅ 核心能力

| 能力 | 接口 | 说明 |
|---|---|---|
| 写入 | `append` / `appendBatch` | 非阻塞（P95 ≤ 1 ms）、不抛异常、失败留在缓冲重试 |
| 批量刷盘 | `StoreOptions::flush` | 默认 **1000 条 / 200 ms** 双阈值：1000 条/s 折成约 1 次/s 后端调用 |
| 查询 | `query` / `queryPage` | 区间 `[from, to)`、结果 `ts` 升序、超限**截断 + 游标**（不静默丢） |
| 回放 | `replay` | 输出时间轴（`Timeline`），支持 `everyNth` / `minIntervalMs` 稀疏化并如实标注 |
| 补发 | `since` | 严格 `ts > 水位`，支持每设备各自水位，分页续查 |
| 缺口识别 | `gaps` | 阈值法（默认 3× 名义周期）报出未覆盖区间，**不假装连续** |
| 保留 | `sweep` | TTL + 容量上限（`purgedRows` / `capacityEvictedRows` 分别计数）；后端不支持删除时**降级并如实上报**，不谎报成功 |
| 观测 | `status` | 条数 / 缓冲深度 / 丢弃计数 / 后端失败 / 缺口序号 / 阈值状态（可 JSON 化） |

**四条设计底线**（详见设计文档 §1）：

```
① 数据只从 append 进：模块不订阅任何上游，落库由宿主装配点调用（零上游依赖）
② 模块内零 SQL / 零连接串 / 零表名：存哪里由注入的后端决定
③ ts 由调用方传入：模块不自己打时间（IClock 只用于保留与统计）
④ 不静默丢数据：任何丢弃（缓冲溢出、后端拒收、清理过期）都有计数与可读日志
```

---

## 🔌 两个内置后端

| 后端 | 头文件 | 适用 | 持久 |
|---|---|---|---|
| `MemoryBackend` | `backends/memory_backend.h` | 示例、测试、可丢数据 | ❌ 进程退出即失 |
| `SegmentFileBackend` | `backends/segment_file.h` | 生产起步形态 | ✅ 跨重启 |

段文件后端物理形态（设计文档 D15）：

```
<dir>/
  ├─ 0000000001700000000000-000000.seg   每条记录一行 JSON；'#idx ' 开头是稀疏索引行
  └─ ...                                 文件名 = 起始 ts（补零）+ 序号 ⇒ 字典序 = 时间序
```

- **查询**：按区间先筛段 → 段内用稀疏索引二分跳到起点 → 顺序读到 `toTs`
- **清理**：`ts < cutoff` 的**整段删文件**；只有跨越边界那一段需要重写（临时文件 + 原子 rename）
- **崩溃**：最多坏最后一行；坏行跳过、不拖垮整段，序号缺口如实可见

---

## 🚫 它明确不做

| 做 | 不做 |
|---|---|
| 追加写入高频流式数据 | **推送**——那是 `realtime-hub` 的事 |
| 按设备 ID + 时间区间查询 | **业务查询**——"哪个任务完成了"去问业务数据库 |
| 输出时间轴，供回放 / 曲线使用 | **画图**——那是出口模块（`map-2d` 等）的事 |
| 可配置的过期清理 | **业务规则判断**——它不认识告警、任务、编组 |
| 为断线客户端提供补发查询 | **连接管理**——谁连着、谁断了，它不知道也不关心 |
| 集群 / 鉴权 / 加密 | 部署层与后续版本 |

**判断捷径**：源码里搜不到"任务 / 方案 / 目标 / 场景 / 阶段 / 编组"这类词——
搜到就是业务漏进来了。

---

## 🗂 目录结构

```
telemetry-store/
├─ include/telemetry_store/        ★ 公开面（9 个头文件）
│   ├─ store.h                     门面：宿主只认识这一个
│   ├─ record.h                    Record / Query / QueryResult / Timeline / Gap / Status
│   ├─ backend.h                   IStorageBackend + RecordBatch（反向接口）
│   ├─ policy.h  clock.h  log_sink.h
│   ├─ console.h                   示例程序的 UTF-8 控制台小工具（不属模块本体）
│   └─ backends/{memory_backend,segment_file}.h
├─ src/                            模块本体（不含业务）
│   ├─ store.cc query.cc record.cc util.h
│   └─ backends/{memory_backend,segment_file}.cc
├─ examples/{minimal,replay_demo}/ 最小示例 · 全流程演示
├─ tools/gen.cc                    造流式数据 / 压写入频率（含吞吐与延迟分位）
├─ tests/selftest.cc               零依赖自测（一个可执行 + 手写断言）
├─ docs/{需求,设计}/                需求专篇 · 概要设计（接口与决策的权威来源）
├─ build.ps1  build.sh             一条命令构建 + 自测
└─ LICENSE
```

---

## 🔗 与其它模块的关系

```
   device-ingest ──ISink──▶ 宿主 ──▶ realtime-hub ──WebSocket──▶ ws-client ──▶ 宿主适配器 ──props──▶ map-2d
                             │                                                                    ▲
                             │ 落库                                                         回放数据 │
                             ▼                                                                    │
                      telemetry-store ────────────────────────────────────────────────────────────┘
```

**所有箭头都是单向的**，没有一个模块回头依赖另一个。这就是它们能被独立拿走的原因。

| 关系 | 说明 |
|---|---|
| 与 `realtime-hub` | **互不认识**。hub 推给在场的人，留存层记下发生的事；两者之间由**宿主**装配 |
| 与 `device-ingest` | **不认识**。设备接入只管把报文变标准数据，落不落库由宿主决定 |
| 与 `map-2d` | **不认识**。留存层输出时间轴，画成什么样是出口的事 |
| 与宿主的连接点 | 宿主里那几个**适配器文件**：`onEvent → append`、`重连 → since`、`replay → 喂给出口` |

> `Store::fromEnvelope()` 是唯一的"顺手帮忙"：把标准信封 `{type, data, ts}` 转成 `Record`。
> 它只依赖信封**语法**，不 import `realtime-hub` 的任何代码。

---

## ✅ 交付验收清单

| 项 | 证据 |
|---|---|
| **独立构建** | `.\build.ps1` / `./build.sh`：空环境一条命令通过，**不需要任何外部服务** |
| **独立运行** | `examples/minimal`（最小链路）· `examples/replay_demo`（全流程） |
| **独立测试** | `tests/selftest.cc`：**40 用例 / 449 断言**，不依赖宿主的数据库与配置 |
| **接口稳定** | 公开面 9 个头文件，各自可单独包含；接口冻结稿见设计文档 §3 |
| **反向接口** | `IStorageBackend` / `IRetentionPolicy` / `IClock` / `ILogSink` 全部注入 |
| **零业务依赖** | 源码中只有：设备 / 数据 / 事件 / 时间 / 区间 / 保留 |
| **零跨仓 import** | 不 import `device-ingest` / `realtime-hub` / `map-2d` 的任何内部 |
| **有配套工具** | `tools/gen.cc`：造数 + 吞吐/延迟测量 |
| **能一句话说清** | **"把实时流存下来，能查、能回放、能补发。"** |

---

## 📄 许可

[Apache License 2.0](./LICENSE)

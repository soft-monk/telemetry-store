// examples/minimal/main.cc · 最小示例：不接任何宿主，一条链路跑通
//
// 对应需求：TLM-NFR-07（空环境 clone 即跑，零外部依赖）
//           TLM-WRT-01（append 写入）· TLM-WRT-07（flush 交付）
//           TLM-QRY-01（区间查询）· TLM-OBS-01（状态快照）
//
// 为什么这么写：
//   · `Store store;` 用**默认构造** —— 零依赖内存后端 + 系统时钟 + 永久保留，
//     让人一眼看到"这个模块自己就能跑"，不需要数据库、不需要配置文件（TLM-ADP-01）。
//   · `ts` 由**调用方**给（D3：模块不自己打时间），所以下面自己算时间戳；
//     它不是"写入时刻"，而是"事件产生时刻"。
//   · 查询区间一律 `[fromTs, toTs)` **左闭右开**（D5）：toTs 要比最后一条的 ts 大。
//
// 运行： build\bin\Release\example_minimal.exe
#include <iostream>

#include "telemetry_store/console.h"
#include "telemetry_store/store.h"

using namespace telemetry_store;

int main() {
    consoleUtf8();  // Windows 控制台默认 GBK：先切到 UTF-8，否则下面的中文是乱码

    // ① 装配：默认形态 = 内存后端（不持久）+ 系统时钟 + 永不清除策略
    Store store;

    // ② 写入：append 非阻塞、不抛异常；seq 留 0 由模块补号（TLM-WRT-05）
    const Millis base = 1700000000000LL;  // 事件时刻（epoch 毫秒），由调用方决定
    store.append("uav-01", "uav.pos", base + 0,
                 {{"lng", 116.400}, {"lat", 39.900}, {"alt", 120.5}});
    store.append("uav-01", "uav.pos", base + 1000,
                 {{"lng", 116.401}, {"lat", 39.901}, {"alt", 121.0}});
    store.append("uav-02", "uav.pos", base + 1000,
                 {{"lng", 116.410}, {"lat", 39.910}, {"alt", 133.2}});

    // ③ flush：返回即**已交付后端**；后端失败会留在缓冲里下轮重试，绝不静默丢（P2 / TLM-WRT-07）
    store.flush();

    // ④ 查询：[base, base+2000) 左闭右开；结果按 ts 升序，同 ts 再按 deviceId/type 排（D5）
    Query q;
    q.fromTs = base;
    q.toTs   = base + 2000;
    const QueryResult result = store.query(q);

    std::cout << "后端=" << store.backendName() << " · 命中 " << result.rows.size() << " 条\n";
    for (const Record& r : result.rows) {
        std::cout << "  ts=" << r.ts << " (+" << (r.ts - base) << "ms)"
                  << "  " << r.deviceId << " " << r.type
                  << "  seq=" << r.seq << "  data=" << r.data.dump() << "\n";
    }

    // ⑤ 观测快照（TLM-OBS-01）：persisted 应等于命中条数，buffered 应为 0
    const Status s = store.status();
    std::cout << "状态：appended=" << s.appended << " persisted=" << s.persisted
              << " buffered=" << s.buffered << " flushes=" << s.flushes
              << " 后端=" << s.backend << " 版本=" << s.version << "\n";
    return 0;
}

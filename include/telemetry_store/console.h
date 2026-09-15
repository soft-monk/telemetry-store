// telemetry-store · console.h · 控制台 UTF-8 小工具（给示例 / 工具 / 自测用）
//
// 为什么需要它：Windows 控制台默认代码页是 GBK（936），而本项目源码与输出都是 UTF-8，
// 于是中文打印出来是乱码。这个头文件提供一个"切到 UTF-8 代码页"的小函数，
// 各程序的 main 开头调一次即可（CMake 里已定义 TELEMETRY_STORE_CONSOLE_UTF8=1 启用）。
//
// ⚠️ 它**不属于模块本体**：`Store` 与各后端从不打印任何东西（TLM-OBS-03：模块自身不写文件、
//    不落库、不打控制台）。这里只是让示例程序的输出在 Windows 上可读。
#pragma once

#if defined(_WIN32) && defined(TELEMETRY_STORE_CONSOLE_UTF8)
#include <windows.h>
#endif

namespace telemetry_store {

/// 把当前进程的控制台输入/输出代码页切到 UTF-8。非 Windows 或未启用宏时是空操作。
inline void consoleUtf8() {
#if defined(_WIN32) && defined(TELEMETRY_STORE_CONSOLE_UTF8)
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

}  // namespace telemetry_store

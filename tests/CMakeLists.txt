# tests/ · 零依赖自测（一个可执行 + 手写断言，对齐 device-ingest / map-2d 的做法）
#
# 覆盖 TLM-* 需求条目，**不需要任何外部服务、不需要网络**：
# 全部通过内存后端与段文件后端在进程内驱动。
#
# 运行： selftest            → 跑全部
#        selftest --list     → 只列用例名
#        selftest <名字片段>  → 只跑名字里含该片段的用例

add_executable(selftest selftest.cc)
target_link_libraries(selftest PRIVATE telemetry_store)

set_target_properties(selftest PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(MSVC)
  target_compile_options(selftest PRIVATE /utf-8 /W4)
  target_compile_definitions(selftest PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
else()
  target_compile_options(selftest PRIVATE -Wall -Wextra)
endif()

# Windows 控制台默认代码页是 GBK，直接输出 UTF-8 的中文会是乱码。
# 统一定义这个宏，让各程序在 main 开头把控制台切到 UTF-8
# （见 include/telemetry_store/console.h 的 telemetry_store::consoleUtf8()）。
target_compile_definitions(selftest PRIVATE TELEMETRY_STORE_CONSOLE_UTF8=1)

add_test(NAME telemetry_store_selftest COMMAND selftest)

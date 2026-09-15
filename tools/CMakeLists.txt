# tools/ · 配套工具（TLM-* 验收清单"有配套工具"一条）
#
# 造流式数据 / 压写入频率 —— 别人接这个模块时第一个就想要它：
# 手上没有真设备、也没有真实数据流，先得有个能造出 1000 条/s 的东西。
add_executable(ts_gen gen.cc)
target_link_libraries(ts_gen PRIVATE telemetry_store)
set_target_properties(ts_gen PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(MSVC)
  target_compile_options(ts_gen PRIVATE /utf-8 /W4)
  target_compile_definitions(ts_gen PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
else()
  target_compile_options(ts_gen PRIVATE -Wall -Wextra)
endif()

# Windows 控制台默认 GBK：输出 UTF-8 中文会乱码，统一切到 UTF-8 代码页。
target_compile_definitions(ts_gen PRIVATE TELEMETRY_STORE_CONSOLE_UTF8=1)

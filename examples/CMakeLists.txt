# examples/ · 最小示例：不接任何宿主也能跑出完整链路
#
#   minimal/      20 行跑通 append → query
#   replay_demo/  写入 → 查询 → 回放 → 补发 → 缺口识别，全流程
add_executable(example_minimal minimal/main.cc)
target_link_libraries(example_minimal PRIVATE telemetry_store)
set_target_properties(example_minimal PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

add_executable(example_replay replay_demo/main.cc)
target_link_libraries(example_replay PRIVATE telemetry_store)
set_target_properties(example_replay PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

foreach(tgt example_minimal example_replay)
  if(MSVC)
    target_compile_options(${tgt} PRIVATE /utf-8 /W4)
    target_compile_definitions(${tgt} PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
  else()
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
  endif()
  # Windows 控制台默认 GBK：输出 UTF-8 中文会乱码，统一切到 UTF-8 代码页。
  target_compile_definitions(${tgt} PRIVATE TELEMETRY_STORE_CONSOLE_UTF8=1)
endforeach()

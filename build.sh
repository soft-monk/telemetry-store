#!/usr/bin/env bash
# build.sh · 一条命令完成配置 / 编译 / 自测（Linux / macOS / MSYS2 / WSL）
#
# Windows 上用 build.ps1（它会自动激活 MSVC 环境）；本脚本给其它平台用。
#
# 用法：
#   ./build.sh              配置 + 编译 + 跑自测
#   ./build.sh clean        删掉 build/ 重新来
#   ./build.sh debug        Debug 构建
#   ./build.sh werror       告警当错误
#   ./build.sh notest       只编译不测试
set -euo pipefail

cd "$(dirname "$0")"

BUILD_DIR=build
BUILD_TYPE=Release
CMAKE_EXTRA=()
RUN_TESTS=1

for arg in "$@"; do
  case "$arg" in
    clean)  rm -rf "$BUILD_DIR" ;;
    debug)  BUILD_TYPE=Debug ;;
    werror) CMAKE_EXTRA+=("-DTELEMETRY_STORE_WARNINGS_AS_ERRORS=ON") ;;
    notest) RUN_TESTS=0 ;;
    *) echo "未知参数：$arg"; exit 2 ;;
  esac
done

# 并行度：nproc（Linux）/ sysctl（macOS）
JOBS=$( (nproc 2>/dev/null) || (sysctl -n hw.ncpu 2>/dev/null) || echo 4 )

echo "[build] 配置（$BUILD_TYPE）"
cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" "${CMAKE_EXTRA[@]+"${CMAKE_EXTRA[@]}"}"

echo "[build] 编译（$JOBS 并行）"
cmake --build "$BUILD_DIR" --parallel "$JOBS"

if [ "$RUN_TESTS" -eq 1 ]; then
  echo "[build] 自测"
  ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

echo "[build] 完成 · 产物在 $BUILD_DIR/bin"

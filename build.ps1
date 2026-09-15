# build.ps1 · 一条命令完成配置 / 编译 / 自测（Windows）
#
# 为什么要这个脚本：本模块的构建依赖 MSVC 环境变量（INCLUDE / LIB / PATH）。
# 直接开一个普通终端跑 cmake 会因为找不到编译器或运行库而失败——
# 这不是模块的问题，是 Windows 上任何 C++ 项目的共同麻烦。
# 脚本自动激活 Visual Studio 环境后调用 cmake，让 TLM-NFR-07
# "空环境 clone 下来一条命令能构建通过"在 Windows 上真的成立。
#
# 用法：
#   .\build.ps1              配置 + 编译 + 跑自测
#   .\build.ps1 -Clean       删掉 build/ 重新来
#   .\build.ps1 -Config Debug
#   .\build.ps1 -NoTest      只编译不测试
#   .\build.ps1 -Werror      告警当错误
#
# ⚠️ 如果提示 "running scripts is disabled on this system"：那是本机 PowerShell 的
#    执行策略（默认 Restricted），与本模块无关。两种解法任选：
#      powershell -ExecutionPolicy Bypass -File .\build.ps1      ← 只放宽这一次
#      Set-ExecutionPolicy -Scope Process RemoteSigned           ← 只放宽当前窗口
#    本文件带 UTF-8 BOM：Windows PowerShell 5.1 会按 ANSI 解码无 BOM 的脚本，
#    中文注释会被读坏并报 ParserError。**不要去掉这个 BOM。**
param(
    [string]$Config = "Release",
    [switch]$Clean,
    [switch]$NoTest,
    [switch]$Werror
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$buildDir = Join-Path $root "build"

# ---------------------------------------------------------------- 1. 激活 MSVC
function Find-VsDevCmd {
    $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $install = & $vswhere -latest -products * `
            -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
            -property installationPath 2>$null
        if ($install) {
            $candidate = Join-Path $install "Common7\Tools\VsDevCmd.bat"
            if (Test-Path $candidate) { return $candidate }
        }
    }
    foreach ($edition in "Enterprise", "Professional", "Community", "BuildTools") {
        $candidate = "C:\Program Files\Microsoft Visual Studio\2022\$edition\Common7\Tools\VsDevCmd.bat"
        if (Test-Path $candidate) { return $candidate }
    }
    return $null
}

$needVs = -not (Get-Command cl.exe -ErrorAction SilentlyContinue)
if ($needVs) {
    $vsDevCmd = Find-VsDevCmd
    if (-not $vsDevCmd) {
        Write-Error "找不到 Visual Studio 2022 的 VsDevCmd.bat。请先装 VS 2022（含 C++ 工具集），或在一个已激活的开发者命令行里运行 cmake。"
    }
    Write-Host "[build] 激活 MSVC 环境：$vsDevCmd"
    # 用 cmd 把环境变量导进当前 PowerShell 会话。
    # 注意：这里用 cmd 的 `&&`（而不是 PowerShell 的），并整串用单引号包住 —— 
    # PowerShell 5.1 不支持 `&&` 运算符，写在外面会直接语法错误。
    $envLines = cmd /c ('call "{0}" -arch=x64 -host_arch=x64 -no_logo >nul 2>&1 && set' -f $vsDevCmd)
    foreach ($line in $envLines) {
        $idx = $line.IndexOf('=')
        if ($idx -gt 0) {
            $name = $line.Substring(0, $idx)
            $value = $line.Substring($idx + 1)
            Set-Item -Path "env:$name" -Value $value -ErrorAction SilentlyContinue
        }
    }
}

# ---------------------------------------------------------------- 2. 配置
if ($Clean -and (Test-Path $buildDir)) {
    Write-Host "[build] 清理 $buildDir"
    Remove-Item -Recurse -Force $buildDir
}

$cmakeArgs = @("-S", $root, "-B", $buildDir, "-G", "Visual Studio 17 2022", "-A", "x64")
if ($Werror) { $cmakeArgs += "-DTELEMETRY_STORE_WARNINGS_AS_ERRORS=ON" }

Write-Host "[build] cmake $($cmakeArgs -join ' ')"
& cmake @cmakeArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# ---------------------------------------------------------------- 3. 编译
Write-Host "[build] 编译（$Config）"
& cmake --build $buildDir --config $Config --parallel
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# ---------------------------------------------------------------- 4. 自测
if (-not $NoTest) {
    Write-Host "[build] 自测"
    & ctest --test-dir $buildDir -C $Config --output-on-failure
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "[build] 完成 · 产物在 $buildDir\bin"

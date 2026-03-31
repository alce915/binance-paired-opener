# 服务双击启动方案参考

## 适用场景

适用于这类本地项目：
- Python Web 服务
- 使用 `FastAPI` / `uvicorn`
- 项目目录可能包含中文路径
- 需要给自己或别人一个“可双击启动”的入口文件

目标是：
- 双击即可启动服务
- 启动失败时能快速定位原因
- 同一套方法可以复用到其他项目

## 本次问题的根因

`启动自动开单服务.bat` 一开始不可用，主要有 3 类原因。

### 1. `.bat` 文件编码导致 `cmd` 解析异常

如果批处理文件里用了不合适的编码，尤其是混入中文提示文本时，`cmd` 可能会把一整行拆坏。

常见现象：
- `ExecutionPolicy` 被拆成乱码片段
- `http:` 被当成命令
- 中文提示文本被当成命令执行

### 2. 虚拟环境启动器在中文路径下不稳定

原方案使用的是：

```bat
.venv\Scripts\python.exe -m uvicorn ...
```

但如果项目目录是中文路径，某些机器上的 venv 启动器会不稳定，尤其是在：
- `Start-Process`
- 后台拉起
- PowerShell 再套一层 PowerShell

这时可能出现：

```text
Unable to create process using '...python.exe -m uvicorn ...'
```

### 3. 只拉进程，不做健康检查

如果只是 `Start-Process` 一下，不代表服务真的起来了。

真实情况可能是：
- 进程已退出
- 端口没监听
- 导入依赖失败
- 配置问题导致服务没完成启动

## 最终稳定方案

最终采用的是这条思路：

1. `.bat` 只做最薄的一层入口
2. 真正的启动逻辑写进 `.ps1`
3. 不直接依赖 `.venv\Scripts\python.exe`
4. 改用系统 Python
5. 显式设置 `PYTHONPATH`
6. 启动后轮询健康检查
7. 启动失败时输出日志文件路径

## 推荐结构

### 1. 双击入口：`.bat`

建议保持纯 ASCII，尽量不要在 `.bat` 里写复杂逻辑。

职责只保留：
- 切到项目目录
- 调用 `.ps1`
- 显示成功或失败结果
- 保持窗口不要秒退

示例：

```bat
@echo off
setlocal
cd /d "%~dp0"

echo Starting service...
echo.

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\restart_service.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if "%EXIT_CODE%"=="0" (
    echo Service started.
) else (
    echo Service failed.
)
echo.
pause
```

### 2. 核心逻辑：`restart_service.ps1`

职责建议包括：
- 停掉旧端口监听进程
- 删除旧日志
- 用稳定方式后台启动服务
- 轮询健康检查
- 启动失败时回显日志内容或日志路径

## 为什么不用 `.venv\Scripts\python.exe`

如果项目目录可能带中文，建议优先改成：
- 使用系统 Python
- 手动补 `PYTHONPATH`

例如：

- 项目根目录
- `.venv\Lib\site-packages`

这样既能：
- 正常导入项目自己的模块
- 继续使用虚拟环境里的依赖
- 避开 venv 启动器在中文路径下的兼容问题

## 推荐启动命令思路

在 PowerShell 中，核心思路类似：

```powershell
$env:PYTHONPATH = "$projectRoot;$projectRoot\.venv\Lib\site-packages"
& "$systemPython" -m uvicorn your_package.api:app --host 127.0.0.1 --port 8000
```

如果需要后台运行，可以再包一层后台启动，但建议先确认“这条命令前台能稳定跑起来”。

## 健康检查一定要做

启动后不要只看进程是否创建成功，要确认服务是否真的可访问。

例如轮询：

```powershell
http://127.0.0.1:8000/
```

判断标准：
- 返回 `200`
- 才算启动成功

如果超时未成功：
- 读取日志文件
- 抛出明确错误

## 日志文件建议

每个服务都建议固定一个运行日志，例如：

- `api.runtime.log`
- `monitor.runtime.log`

启动脚本里建议：
- 启动前先删旧日志
- 启动后把 stdout/stderr 都重定向进去

这样启动失败时不用猜，直接看日志。

## 可复用模板思路

以后别的项目可以直接照这个模板改：

需要替换的只有这些变量：
- `projectRoot`
- `systemPython`
- `port`
- `runtimeLog`
- `uvicorn` 的应用入口，例如：
  - `your_project.api:app`

## 实操建议

如果你要给别的项目也做双击启动，建议优先遵守这几条：

1. `.bat` 保持最薄
2. 复杂逻辑放 `.ps1`
3. 中文路径项目不要迷信 `.venv\Scripts\python.exe`
4. 优先用“系统 Python + 显式 PYTHONPATH”
5. 启动后必须做健康检查
6. 失败时必须有日志文件可看
7. 如果还要做双击关闭，就再配一个 `stop_service.ps1 + 关闭服务.bat`

## 本项目最终做法

本项目现在采用的是：
- 双击启动：`启动自动开单服务.bat`
- 双击关闭：`关闭自动开单服务.bat`
- 启动脚本：`scripts\restart_service.ps1`
- 停止脚本：`scripts\stop_service.ps1`
- 健康地址：`http://127.0.0.1:8000/`
- 运行日志：`api.runtime.log`

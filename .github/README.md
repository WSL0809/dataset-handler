# GitHub Actions 构建工作流说明

本项目包含了自动化构建工作流，支持多平台编译和自动发布。

## 🔧 工作流概述

### 1. 主构建工作流 (`build.yml`)

**触发条件：**
- 推送代码到 `main`、`master` 或 `dev` 分支
- 创建 Pull Request

**构建平台：**
- Windows 64位 (`x86_64-pc-windows-msvc`)
- Linux 64位 (`x86_64-unknown-linux-gnu`)
- macOS ARM64 (`aarch64-apple-darwin`) - 适用于 M1/M2 Mac

**功能：**
- 自动编译 Rust 项目
- 运行单元测试
- 上传编译产物到 GitHub Actions Artifacts
- 当推送 Git 标签时自动创建 Release

### 2. Nightly 构建工作流 (`nightly.yml`)

**触发条件：**
- 每天 UTC 2:00（北京时间 10:00）自动运行
- 可手动触发

**功能：**
- 检查最近24小时是否有新提交
- 如有新提交则进行构建
- 创建 "nightly" 预发布版本
- 提供最新开发版本的下载

## 📦 如何下载编译包

### 方式一：从 Actions Artifacts 下载

1. 进入项目的 GitHub 页面
2. 点击顶部的 "Actions" 标签
3. 选择一个成功的构建任务
4. 在页面底部的 "Artifacts" 部分下载对应平台的文件

**优点：** 每次推送都有新的构建
**缺点：** 需要登录 GitHub 账号，文件保留30天

### 方式二：从 Releases 下载

1. 进入项目的 GitHub 页面
2. 点击右侧的 "Releases" 或访问 `/releases` 页面
3. 下载对应版本的文件

**文件说明：**
- `dataset-handler-windows-x64.exe` - Windows 64位版本
- `dataset-handler-linux-x64` - Linux 64位版本
- `dataset-handler-macos-arm64` - macOS ARM64 版本

### 方式三：Nightly 版本

访问 Releases 页面，查找标记为 "Pre-release" 的 "Nightly Build" 版本，这是最新的开发版本。

## 🏷️ 如何创建正式发布版本

要创建一个正式发布版本：

1. 确保代码已合并到主分支
2. 创建并推送一个版本标签：

```bash
git tag v1.0.0
git push origin v1.0.0
```

3. GitHub Actions 会自动构建所有平台的版本并创建 Release

## 🔍 工作流状态

可以通过以下徽章查看工作流状态：

- 主构建: ![Build Status](https://github.com/YOUR_USERNAME/YOUR_REPO/workflows/多平台构建/badge.svg)
- Nightly 构建: ![Nightly Build](https://github.com/YOUR_USERNAME/YOUR_REPO/workflows/Nightly%20构建/badge.svg)

## 🛠️ 本地测试

如果需要本地测试跨平台编译：

```bash
# 安装目标平台
rustup target add x86_64-pc-windows-msvc
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-apple-darwin

# 编译指定平台
cargo build --release --target x86_64-pc-windows-msvc
cargo build --release --target x86_64-unknown-linux-gnu
cargo build --release --target aarch64-apple-darwin
```

## 📝 注意事项

1. **macOS ARM64 版本**：只能在 Apple M1/M2 芯片的 Mac 上运行
2. **Linux 版本**：使用 glibc，在大多数现代 Linux 发行版上可以运行
3. **Windows 版本**：需要 Windows 7 或更高版本
4. **Nightly 版本**：仅供测试使用，可能包含未经充分测试的功能

## 🔧 故障排除

如果构建失败，请检查：
1. Rust 代码是否有编译错误
2. 依赖项是否正确配置
3. 测试是否全部通过
4. 是否需要特定的系统依赖

可以在 Actions 页面查看详细的构建日志来诊断问题。 
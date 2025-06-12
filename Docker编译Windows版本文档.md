# Docker 交叉编译 Windows 版本文档

### 1. 创建 Dockerfile

创建 `Dockerfile.windows` 文件：

```dockerfile
FROM ubuntu:22.04

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:$PATH

# 安装基础依赖
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    mingw-w64 \
    && rm -rf /var/lib/apt/lists/*

# 安装Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
RUN chmod -R a+w $RUSTUP_HOME $CARGO_HOME

# 添加Windows目标
RUN rustup target add x86_64-pc-windows-gnu

# 配置交叉编译
RUN mkdir -p ~/.cargo
RUN echo '[target.x86_64-pc-windows-gnu]' >> ~/.cargo/config.toml && \
    echo 'linker = "x86_64-w64-mingw32-gcc"' >> ~/.cargo/config.toml && \
    echo 'ar = "x86_64-w64-mingw32-ar"' >> ~/.cargo/config.toml

WORKDIR /app

# 复制项目文件
COPY . .

# 编译Windows版本
RUN cargo build --target x86_64-pc-windows-gnu --release
```

### 2. 修改 Cargo.toml

为了支持 Snappy 等压缩格式，修改 `Cargo.toml`：

```toml
[package]
name = "dataset_handler"
version = "0.1.0"
edition = "2021"

[dependencies]
parquet = { version = "55.0.0", features = ["arrow", "snap", "lz4", "brotli"] }
arrow = { version = "55.0.0" }
arrow-array = { version = "55.0.0" }
arrow-schema = { version = "55.0.0" }
serde_json = "1.0"
rayon = "1.10"
tokio = { version = "1", features = ["full"] }
chrono = { version = "0.4", default-features = false, features = ["std"] }
clap = { version = "4.0", features = ["derive"] }

# 可选依赖，用于SIMD优化
simd-json = { version = "0.15.1", optional = true }

[features]
default = []
simd-json = ["dep:simd-json"]

# 为发布版本优化
[profile.release]
lto = true
codegen-units = 1
opt-level = 3
panic = "abort"
```

### 3. 执行编译命令

```bash
# 构建 Docker 镜像并编译
docker build -f Dockerfile.windows -t dataset-handler-windows .

# 创建临时容器
docker create --name temp-container dataset-handler-windows

# 提取编译好的可执行文件
docker cp temp-container:/app/target/x86_64-pc-windows-gnu/release/dataset_handler.exe ./dataset_handler_windows.exe

# 清理临时容器
docker rm temp-container
```
